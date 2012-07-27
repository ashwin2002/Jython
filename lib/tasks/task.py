import time
import logger
import random
import string
from threading import Thread
from memcacheConstants import ERR_NOT_FOUND
from membase.api.rest_client import RestConnection
from membase.api.exception import BucketCreationException
from membase.helper.bucket_helper import BucketOperationHelper
from memcached.helper.data_helper import VBucketAwareMemcached, MemcachedClientHelper
from couchbase.document import DesignDocument, View
from mc_bin_client import MemcachedError
from tasks.future import Future
import json
from membase.api.exception import DesignDocCreationException, QueryViewException, ReadDocumentException, RebalanceFailedException, \
                                        GetBucketInfoFailed, CompactViewFailed, SetViewInfoNotFound, FailoverFailedException

#TODO: Setup stacktracer
#TODO: Needs "easy_install pygments"
#import stacktracer
#stacktracer.trace_start("trace.html",interval=30,auto=True) # Set auto flag to always update file!


PENDING = 'PENDING'
EXECUTING = 'EXECUTING'
CHECKING = 'CHECKING'
FINISHED = 'FINISHED'

class Task(Future):
    def __init__(self, name):
        Future.__init__(self)
        self.log = logger.Logger.get_logger()
        self.state = PENDING
        self.name = name
        self.cancelled = False
        self.retries = 0
        self.res = None

    def step(self, task_manager):
        if not self.done():
            if self.state == PENDING:
                self.state = EXECUTING
                task_manager.schedule(self)
            elif self.state == EXECUTING:
                self.execute(task_manager)
            elif self.state == CHECKING:
                self.check(task_manager)
            elif self.state != FINISHED:
                raise Exception("Bad State in {0}: {1}".format(self.name, self.state))

    def execute(self, task_manager):
        raise NotImplementedError

    def check(self, task_manager):
        raise NotImplementedError

class NodeInitializeTask(Task):
    def __init__(self, server):
        Task.__init__(self, "node_init_task")
        self.server = server
        self.quota = 0

    def execute(self, task_manager):
        rest = RestConnection(self.server)
        username = self.server.rest_username
        password = self.server.rest_password
        rest.init_cluster(username, password)
        info = rest.get_nodes_self()
        self.quota = int(info.mcdMemoryReserved * 2 / 3)
        rest.init_cluster_memoryQuota(username, password, self.quota)
        self.state = CHECKING
        task_manager.schedule(self)

    def check(self, task_manager):
        self.state = FINISHED
        self.set_result(self.quota)


class BucketCreateTask(Task):
    def __init__(self, server, bucket='default', replicas=1, size=0, port=11211,
                 password=None):
        Task.__init__(self, "bucket_create_task")
        self.server = server
        self.bucket = bucket
        self.replicas = replicas
        self.port = port
        self.size = size
        self.password = password

    def execute(self, task_manager):
        rest = RestConnection(self.server)
        if self.size <= 0:
            info = rest.get_nodes_self()
            self.size = info.memoryQuota * 2 / 3

        authType = 'none' if self.password is None else 'sasl'

        try:
            rest.create_bucket(bucket=self.bucket,
                               ramQuotaMB=self.size,
                               replicaNumber=self.replicas,
                               proxyPort=self.port,
                               authType=authType,
                               saslPassword=self.password)
            self.state = CHECKING
            task_manager.schedule(self)
        except BucketCreationException as e:
            self.state = FINISHED
            self.set_exception(e)
        #catch and set all unexpected exceptions
        except Exception as e:
            self.state = FINISHED
            self.log.info("Unexpected Exception Caught")
            self.set_exception(e)

    def check(self, task_manager):
        try:
            if BucketOperationHelper.wait_for_memcached(self.server, self.bucket):
                self.log.info("bucket '{0}' was created with per node RAM quota: {1}".format(self.bucket, self.size))
                self.set_result(True)
                self.state = FINISHED
                return
            else:
                self.log.info("vbucket map not ready after try {0}".format(self.retries))
        except Exception:
            self.log.info("vbucket map not ready after try {0}".format(self.retries))
        self.retries = self.retries + 1
        task_manager.schedule(self)


class BucketDeleteTask(Task):
    def __init__(self, server, bucket="default"):
        Task.__init__(self, "bucket_delete_task")
        self.server = server
        self.bucket = bucket

    def execute(self, task_manager):
        rest = RestConnection(self.server)
        try:
            if rest.delete_bucket(self.bucket):
                self.state = CHECKING
                task_manager.schedule(self)
            else:
                self.state = FINISHED
                self.set_result(False)
        #catch and set all unexpected exceptions
        except Exception as e:
            self.state = FINISHED
            self.log.info("Unexpected Exception Caught")
            self.set_exception(e)


    def check(self, task_manager):
        rest = RestConnection(self.server)
        try:
            if BucketOperationHelper.wait_for_bucket_deletion(self.bucket, rest, 200):
                self.set_result(True)
            else:
                self.set_result(False)
            self.state = FINISHED
        #catch and set all unexpected exceptions
        except Exception as e:
            self.state = FINISHED
            self.log.info("Unexpected Exception Caught")
            self.set_exception(e)

class RebalanceTask(Task):
    def __init__(self, servers, to_add=[], to_remove=[], do_stop=False, progress=30):
        Task.__init__(self, "rebalance_task")
        self.servers = servers
        self.to_add = to_add
        self.to_remove = to_remove

    def execute(self, task_manager):
        try:
            self.add_nodes(task_manager)
            self.start_rebalance(task_manager)
            self.state = CHECKING
            task_manager.schedule(self)
        except Exception as e:
            self.state = FINISHED
            self.set_exception(e)

    def add_nodes(self, task_manager):
        master = self.servers[0]
        rest = RestConnection(master)
        for node in self.to_add:
            self.log.info("adding node {0}:{1} to cluster".format(node.ip, node.port))
            rest.add_node(master.rest_username, master.rest_password,
                          node.ip, node.port)

    def start_rebalance(self, task_manager):
        rest = RestConnection(self.servers[0])
        nodes = rest.node_statuses()
        ejectedNodes = []
        for server in self.to_remove:
            for node in nodes:
                if server.ip == node.ip and int(server.port) == int(node.port):
                    ejectedNodes.append(node.id)
        rest.rebalance(otpNodes=[node.id for node in nodes], ejectedNodes=ejectedNodes)

    def check(self, task_manager):
        rest = RestConnection(self.servers[0])
        progress = -100
        try:
            progress = rest._rebalance_progress()
        except RebalanceFailedException as ex:
            self.state = FINISHED
            self.set_exception(ex)
        #catch and set all unexpected exceptions
        except Exception as e:
            self.state = FINISHED
            self.log.info("Unexpected Exception Caught")
            self.set_exception(e)
        if progress != -1 and progress != 100:
            task_manager.schedule(self, 10)
        else:
            self.log.info("rebalancing was completed with progress: {0}%".format(progress))
            self.state = FINISHED
            self.set_result(True)

class StatsWaitTask(Task):
    EQUAL = '=='
    NOT_EQUAL = '!='
    LESS_THAN = '<'
    LESS_THAN_EQ = '<='
    GREATER_THAN = '>'
    GREATER_THAN_EQ = '>='

    def __init__(self, servers, bucket, param, stat, comparison, value):
        Task.__init__(self, "stats_wait_task")
        self.servers = servers
        self.bucket = bucket
        self.param = param
        self.stat = stat
        self.comparison = comparison
        self.value = value
        self.conns = {}

    def execute(self, task_manager):
        self.state = CHECKING
        task_manager.schedule(self)

    def check(self, task_manager):
        stat_result = 0
        for server in self.servers:
            client = self._get_connection(server)
            try:
                stats = client.stats(self.param)
                if not stats.has_key(self.stat):
                    self.state = FINISHED
                    self.set_exception(Exception("Stat {0} not found".format(self.stat)))
                    return
                if stats[self.stat].isdigit():
                    stat_result += long(stats[self.stat])
                else:
                    stat_result = stats[self.stat]
            except EOFError as ex:
                self.state = FINISHED
                self.set_exception(ex)

        if not self._compare(self.comparison, str(stat_result), self.value):
            self.log.info("Not Ready: %s %s %s %s expected on %s" % (self.stat, stat_result,
                      self.comparison, self.value, self._stringify_servers()))
            task_manager.schedule(self, 5)
            return
        self.log.info("Saw %s %s %s %s expected on %s" % (self.stat, stat_result,
                      self.comparison, self.value, self._stringify_servers()))

        for server, conn in self.conns.items():
            conn.close()
        self.state = FINISHED
        self.set_result(True)

    def _stringify_servers(self):
        return ''.join([`server.ip` for server in self.servers])

    def _get_connection(self, server):
        if not self.conns.has_key(server):
            self.conns[server] = MemcachedClientHelper.direct_client(server, self.bucket)
        return self.conns[server]

    def _compare(self, cmp_type, a, b):
        if isinstance(b, (int, long)) and a.isdigit():
            a = long(a)
        elif isinstance(b, (int, long)) and not a.isdigit():
                return False
        if (cmp_type == StatsWaitTask.EQUAL and a == b) or\
            (cmp_type == StatsWaitTask.NOT_EQUAL and a != b) or\
            (cmp_type == StatsWaitTask.LESS_THAN_EQ and a <= b) or\
            (cmp_type == StatsWaitTask.GREATER_THAN_EQ and a >= b) or\
            (cmp_type == StatsWaitTask.LESS_THAN and a < b) or\
            (cmp_type == StatsWaitTask.GREATER_THAN and a > b):
            return True
        return False

class GenericLoadingTask(Thread, Task):
    def __init__(self, server, bucket, kv_store):
        Thread.__init__(self)
        Task.__init__(self, "load_gen_task")
        self.kv_store = kv_store
        self.client = VBucketAwareMemcached(RestConnection(server), bucket)

    def execute(self, task_manager):
        self.start()
        self.state = EXECUTING

    def check(self, task_manager):
        pass

    def run(self):
        while self.has_next() and not self.done():
            self.next()
        self.state = FINISHED
        self.set_result(True)

    def has_next(self):
        raise NotImplementedError

    def next(self):
        raise NotImplementedError

    def _unlocked_create(self, partition, key, value):
        try:
            value_json = json.loads(value)
            value_json['mutated'] = 0
            value = json.dumps(value_json)
        except ValueError:
            index = random.choice(range(len(value)))
            value = value[0:index] + random.choice(string.ascii_uppercase) + value[index + 1:]

        try:
            self.client.set(key, self.exp, 0, value)
            partition.set(key, value, self.exp)
        except MemcachedError as error:
            self.state = FINISHED
            self.set_exception(error)

    def _unlocked_read(self, partition, key):
        try:
            o, c, d = self.client.get(key)
        except MemcachedError as error:
            if error.status == ERR_NOT_FOUND and partition.get_valid(key) is None:
                pass
            else:
                self.state = FINISHED
                self.set_exception(error)

    def _unlocked_update(self, partition, key):
        value = partition.get_valid(key)
        if value is None:
            return
        try:
            value_json = json.loads(value)
            value_json['mutated'] += 1
            value = json.dumps(value_json)
        except ValueError:
            index = random.choice(range(len(value)))
            value = value[0:index] + random.choice(string.ascii_uppercase) + value[index + 1:]

        try:
            self.client.set(key, self.exp, 0, value)
            partition.set(key, value, self.exp)
        except MemcachedError as error:
            self.state = FINISHED
            self.set_exception(error)

    def _unlocked_delete(self, partition, key):
        try:
            self.client.delete(key)
            partition.delete(key)
        except MemcachedError as error:
            if error.status == ERR_NOT_FOUND and partition.get_valid(key) is None:
                pass
            else:
                self.state = FINISHED
                self.set_exception(error)

class LoadDocumentsTask(GenericLoadingTask):
    def __init__(self, server, bucket, generator, kv_store, op_type, exp):
        GenericLoadingTask.__init__(self, server, bucket, kv_store)
        self.generator = generator
        self.op_type = op_type
        self.exp = exp

    def has_next(self):
        return self.generator.has_next()

    def next(self):
        key, value = self.generator.next()
        partition = self.kv_store.acquire_partition(key)
        if self.op_type == 'create':
            self._unlocked_create(partition, key, value)
        elif self.op_type == 'read':
            self._unlocked_read(partition, key)
        elif self.op_type == 'update':
            self._unlocked_update(partition, key)
        elif self.op_type == 'delete':
            self._unlocked_delete(partition, key)
        else:
            self.state = FINISHED
            self.set_exception(Exception("Bad operation type: %s" % self.op_type))
        self.kv_store.release_partition(key)

class WorkloadTask(GenericLoadingTask):
    def __init__(self, server, bucket, kv_store, num_ops, create, read, update, delete, exp):
        GenericLoadingTask.__init__(self, server, bucket, kv_store)
        self.itr = 0
        self.num_ops = num_ops
        self.create = create
        self.read = create + read
        self.update = create + read + update
        self.delete = create + read + update + delete
        self.exp = exp

    def has_next(self):
        if self.num_ops == 0 or self.itr < self.num_ops:
            return True
        return False

    def next(self):
        self.itr += 1
        rand = random.randint(1, self.delete)
        if rand > 0 and rand <= self.create:
            self._create_random_key()
        elif rand > self.create and rand <= self.read:
            self._get_random_key()
        elif rand > self.read and rand <= self.update:
            self._update_random_key()
        elif rand > self.update and rand <= self.delete:
            self._delete_random_key()

    def _get_random_key(self):
        partition, part_num = self.kv_store.acquire_random_partition()
        if partition is None:
            return

        key = partition.get_random_valid_key()
        if key is None:
            self.kv_store.release_partition(part_num)
            return

        self._unlocked_read(partition, key)
        self.kv_store.release_partition(part_num)

    def _create_random_key(self):
        partition, part_num = self.kv_store.acquire_random_partition(False)
        if partition is None:
            return

        key = partition.get_random_deleted_key()
        if key is None:
            self.kv_store.release_partition(part_num)
            return

        value = partition.get_deleted(key)
        if value is None:
            self.kv_store.release_partition(part_num)
            return

        self._unlocked_create(partition, key, value)
        self.kv_store.release_partition(part_num)

    def _update_random_key(self):
        partition, part_num = self.kv_store.acquire_random_partition()
        if partition is None:
            return

        key = partition.get_random_valid_key()
        if key is None:
            self.kv_store.release_partition(part_num)
            return

        self._unlocked_update(partition, key)
        self.kv_store.release_partition(part_num)

    def _delete_random_key(self):
        partition, part_num = self.kv_store.acquire_random_partition()
        if partition is None:
            return

        key = partition.get_random_valid_key()
        if key is None:
            self.kv_store.release_partition(part_num)
            return

        self._unlocked_delete(partition, key)
        self.kv_store.release_partition(part_num)

class ValidateDataTask(GenericLoadingTask):
    def __init__(self, server, bucket, kv_store):
        GenericLoadingTask.__init__(self, server, bucket, kv_store)
        self.valid_keys, self.deleted_keys = kv_store.key_set()
        self.num_valid_keys = len(self.valid_keys)
        self.num_deleted_keys = len(self.deleted_keys)
        self.itr = 0

    def has_next(self):
        if self.itr < (self.num_valid_keys + self.num_deleted_keys):
            return True
        return False

    def next(self):
        if self.itr < self.num_valid_keys:
            self._check_valid_key(self.valid_keys[self.itr])
        else:
            self._check_deleted_key(self.deleted_keys[self.itr - self.num_valid_keys])
        self.itr += 1

    def _check_valid_key(self, key):
        partition = self.kv_store.acquire_partition(key)

        value = partition.get_valid(key)
        if value is None:
            self.kv_store.release_partition(key)
            return
        value = json.dumps(value)

        try:
            o, c, d = self.client.get(key)
            if d != json.loads(value):
                self.state = FINISHED
                self.set_exception(Exception('Bad result: %s != %s' % (json.dumps(d), value)))
        except MemcachedError as error:
            if error.status == ERR_NOT_FOUND and partition.get_valid(key) is None:
                pass
            else:
                self.state = FINISHED
                self.set_exception(error)
        self.kv_store.release_partition(key)

    def _check_deleted_key(self, key):
        partition = self.kv_store.acquire_partition(key)

        try:
            o, c, d = self.client.delete(key)
            if partition.get_valid(key) is not None:
                self.state = FINISHED
                self.set_exception(Exception('Not Deletes: %s' % (key)))
        except MemcachedError as error:
            if error.status == ERR_NOT_FOUND:
                pass
            else:
                self.state = FINISHED
                self.set_exception(error)
        self.kv_store.release_partition(key)


class VerifyRevIdTask(GenericLoadingTask):
    def __init__(self, src_server, dest_server, bucket, kv_store):
        GenericLoadingTask.__init__(self, src_server, bucket, kv_store)
        self.client_dest = VBucketAwareMemcached(RestConnection(dest_server), bucket)

        self.valid_keys, self.deleted_keys = kv_store.key_set()
        self.num_valid_keys = len(self.valid_keys)
        self.num_deleted_keys = len(self.deleted_keys)
        self.itr = 0
        self.err_count = 0

    def has_next(self):
        if self.itr < self.num_deleted_keys:
            return True
        return False

    def next(self):
        if self.itr < self.num_deleted_keys:
            self._check_deleted_key_revId(self.deleted_keys[self.itr])
        self.itr += 1

    def _check_deleted_key_revId(self, key):
        try:
            src = self.client.memcached(key)
            dest = self.client_dest.memcached(key)
            seqno_src, cas_src, exp_src, flags_src = src.getRev(key)
            seqno_dest, cas_dest, exp_dest, flags_dest = dest.getRev(key)

            if seqno_src != seqno_dest:
                self.err_count += 1

                self.log.error(
                    "Mismatch on sequence numbers for key {0}\t Source Sequence Num:{1}\t Destination Sequence Num:{2}\tError Count{3}".format(
                        key, seqno_src, seqno_dest, self.err_count))
                self.state = FINISHED
            elif cas_src != cas_dest:
                self.err_count += 1
                self.log.error(
                    "Mismatch on CAS for key {0}\t Source CAS:{1}\t Destination CAS:{2}\tError Count{3}".format(key,
                        cas_src,
                        cas_dest, self.err_count))
                self.state = FINISHED
            elif exp_src != exp_dest:
                self.err_count += 1
                self.log.error(
                    "Mismatch on Expiry Flags for key {0}\t Source Expiry Flags:{1}\t Destination Expiry Flags:{2}\tError Count{3}".format(
                        key,
                        exp_src, exp_dest, self.err_count))
                self.state = FINISHED
            elif flags_src != flags_dest:
                self.err_count += 1
                self.log.error(
                    "Mismatch on Flags for key {0}\t Source Flags:{1}\t Destination Flags:{2}\tError Count{3}".format(
                        key,
                        flags_src, flags_dest, self.err_count))
                self.state = FINISHED
        except MemcachedError as error:
            if error.status == ERR_NOT_FOUND:
                pass
            else:
                self.state = FINISHED
                self.set_exception(error)

class ViewCreateTask(Task):
    def __init__(self, server, design_doc_name, view, bucket="default"):
        Task.__init__(self, "create_view_task")
        self.server = server
        self.bucket = bucket
        self.view = view
        prefix = ""
        if self.view:
            prefix = ("", "dev_")[self.view.dev_view]
        self.design_doc_name = prefix + design_doc_name
        self.ddoc_rev_no = 0

    def execute(self, task_manager):

        rest = RestConnection(self.server)

        try:
            # appending view to existing design doc
            content = rest.get_ddoc(self.bucket, self.design_doc_name)
            ddoc = DesignDocument._init_from_json(self.design_doc_name, content)
            #if view is to be updated
            if self.view:
                ddoc.add_view(self.view)
            self.ddoc_rev_no = self._parse_revision(content['_rev'])

        except ReadDocumentException:
            # creating first view in design doc
            if self.view:
                ddoc = DesignDocument(self.design_doc_name, [self.view])
            #create an empty design doc
            else:
                ddoc = DesignDocument(self.design_doc_name, [])

        try:
            rest.create_design_document(self.bucket, ddoc)
            self.state = CHECKING
            task_manager.schedule(self)

        except DesignDocCreationException as e:
            self.state = FINISHED
            self.set_exception(e)

        #catch and set all unexpected exceptions
        except Exception as e:
            self.state = FINISHED
            self.log.info("Unexpected Exception Caught")
            self.set_exception(e)

    def check(self, task_manager):
        rest = RestConnection(self.server)

        try:
            #only query if the DDoc has a view
            if self.view:
                query = {"stale" : "ok"}
                content = \
                    rest.query_view(self.design_doc_name, self.view.name, self.bucket, query)
                self.log.info("view : {0} was created successfully in ddoc: {1}".format(self.view.name, self.design_doc_name))
            else:
                #if we have reached here, it means design doc was successfully updated
                self.log.info("Design Document : {0} was updated successfully".format(self.design_doc_name))

            self.state = FINISHED
            if self._check_ddoc_revision():
                self.set_result(self.ddoc_rev_no)
            else:
                self.set_exception(Exception("failed to update design document"))
        except QueryViewException as e:
            task_manager.schedule(self, 2)

        #catch and set all unexpected exceptions
        except Exception as e:
            self.state = FINISHED
            self.log.info("Unexpected Exception Caught")
            self.set_exception(e)

    def _check_ddoc_revision(self):
        valid = False
        rest = RestConnection(self.server)

        try:
            content = rest.get_ddoc(self.bucket, self.design_doc_name)
            new_rev_id = self._parse_revision(content['_rev'])
            if new_rev_id != self.ddoc_rev_no:
                self.ddoc_rev_no = new_rev_id
                valid = True
        except ReadDocumentException:
            pass

        #catch and set all unexpected exceptions
        except Exception as e:
            self.state = FINISHED
            self.log.info("Unexpected Exception Caught")
            self.set_exception(e)

        return valid

    def _parse_revision(self, rev_string):
        return int(rev_string.split('-')[0])

class ViewDeleteTask(Task):
    def __init__(self, server, design_doc_name, view, bucket="default"):
        Task.__init__(self, "delete_view_task")
        self.server = server
        self.bucket = bucket
        self.view = view
        prefix = ""
        if self.view:
            prefix = ("", "dev_")[self.view.dev_view]
        self.design_doc_name = prefix + design_doc_name

    def execute(self, task_manager):

        rest = RestConnection(self.server)

        try:
            if self.view:
                # remove view from existing design doc
                content = rest.get_ddoc(self.bucket, self.design_doc_name)
                ddoc = DesignDocument._init_from_json(self.design_doc_name, content)
                status = ddoc.delete_view(self.view)
                if not status:
                    self.state = FINISHED
                    self.set_exception(Exception('View does not exist! %s' % (self.view.name)))

                # update design doc
                rest.create_design_document(self.bucket, ddoc)
                self.state = CHECKING
                task_manager.schedule(self, 2)
            else:
                #delete the design doc
                rest.delete_view(self.bucket, self.design_doc_name)
                self.log.info("Design Doc : {0} was successfully deleted".format(self.design_doc_name))
                self.state = FINISHED
                self.set_result(True)

        except (ValueError, ReadDocumentException, DesignDocCreationException) as e:
            self.state = FINISHED
            self.set_exception(e)

        #catch and set all unexpected exceptions
        except Exception as e:
            self.state = FINISHED
            self.log.info("Unexpected Exception Caught")
            self.set_exception(e)

    def check(self, task_manager):
        rest = RestConnection(self.server)

        try:
            # make sure view was deleted
            query = {"stale" : "ok"}
            content = \
                rest.query_view(self.design_doc_name, self.view.name, self.bucket, query)
            self.state = FINISHED
            self.set_result(False)
        except QueryViewException as e:
            self.log.info("view : {0} was successfully deleted in ddoc: {1}".format(self.view.name, self.design_doc_name))
            self.state = FINISHED
            self.set_result(True)

        #catch and set all unexpected exceptions
        except Exception as e:
            self.state = FINISHED
            self.log.info("Unexpected Exception Caught")
            self.set_exception(e)

class ViewQueryTask(Task):
    def __init__(self, server, design_doc_name, view_name,
                 query, expected_rows=None,
                 bucket="default", retry_time=2):
        Task.__init__(self, "query_view_task")
        self.server = server
        self.bucket = bucket
        self.view_name = view_name
        self.design_doc_name = design_doc_name
        self.query = query
        self.expected_rows = expected_rows
        self.retry_time = retry_time

    def execute(self, task_manager):

        rest = RestConnection(self.server)

        try:
            # make sure view can be queried
            content = \
                rest.query_view(self.design_doc_name, self.view_name, self.bucket, self.query)

            if self.expected_rows is None:
                # no verification
                self.state = FINISHED
                self.set_result(content)
            else:
                self.state = CHECKING
                task_manager.schedule(self)

        except QueryViewException as e:
            # initial query failed, try again
            task_manager.schedule(self, self.retry_time)

        #catch and set all unexpected exceptions
        except Exception as e:
            self.state = FINISHED
            self.log.info("Unexpected Exception Caught")
            self.set_exception(e)

    def check(self, task_manager):
        rest = RestConnection(self.server)

        try:
            # query and verify expected num of rows returned
            content = \
                rest.query_view(self.design_doc_name, self.view_name, self.bucket, self.query)

            self.log.info("(%d rows) expected, (%d rows) returned" %\
                          (self.expected_rows, len(content['rows'])))

            if len(content['rows']) == self.expected_rows:
                self.log.info("expected number of rows: '{0}' was found for view query".format(self.
                            expected_rows))
                self.state = FINISHED
                self.set_result(True)
            else:
                if "stale" in self.query:
                    if self.query["stale"].lower() == "false":
                        self.state = FINISHED
                        self.set_result(False)

                # retry until expected results or task times out
                task_manager.schedule(self, self.retry_time)
        except QueryViewException as e:
            # subsequent query failed! exit
            self.state = FINISHED
            self.set_exception(e)

        #catch and set all unexpected exceptions
        except Exception as e:
            self.state = FINISHED
            self.log.info("Unexpected Exception Caught")
            self.set_exception(e)


class ModifyFragmentationConfigTask(Task):

    """
        Given a config dictionary attempt to configure fragmentation settings.
        This task will override the default settings that are provided for
        a given <bucket>.
    """

    def __init__(self, server, config=None, bucket="default"):
        Task.__init__(self, "modify_frag_config_task")

        self.server = server
        self.config = {"parallelDBAndVC" : "false",
                       "dbFragmentThreshold" : None,
                       "viewFragmntThreshold" : None,
                       "dbFragmentThresholdPercentage" : 100,
                       "viewFragmntThresholdPercentage" : 100,
                       "allowedTimePeriodFromHour" : None,
                       "allowedTimePeriodFromMin" : None,
                       "allowedTimePeriodToHour" : None,
                       "allowedTimePeriodToMin" : None,
                       "allowedTimePeriodAbort" : None,
                       "autoCompactionDefined" : "true"}
        self.bucket = bucket

        for key in config:
            self.config[key] = config[key]

    def execute(self, task_manager):
        rest = RestConnection(self.server)

        try:
            rest.set_auto_compaction(parallelDBAndVC=self.config["parallelDBAndVC"],
                                     dbFragmentThreshold=self.config["dbFragmentThreshold"],
                                     viewFragmntThreshold=self.config["viewFragmntThreshold"],
                                     dbFragmentThresholdPercentage=self.config["dbFragmentThresholdPercentage"],
                                     viewFragmntThresholdPercentage=self.config["viewFragmntThresholdPercentage"],
                                     allowedTimePeriodFromHour=self.config["allowedTimePeriodFromHour"],
                                     allowedTimePeriodFromMin=self.config["allowedTimePeriodFromMin"],
                                     allowedTimePeriodToHour=self.config["allowedTimePeriodToHour"],
                                     allowedTimePeriodToMin=self.config["allowedTimePeriodToMin"],
                                     allowedTimePeriodAbort=self.config["allowedTimePeriodAbort"],
                                     bucket=self.bucket)

            self.state = CHECKING
            task_manager.schedule(self, 10)

        except Exception as e:
            self.state = FINISHED
            self.set_exception(e)


    def check(self, task_manager):

        rest = RestConnection(self.server)
        try:
            # verify server accepted settings
            content = rest.get_bucket_json(self.bucket)
            if content["autoCompactionSettings"] == False:
                self.set_exception(Exception("Failed to set auto compaction settings"))
            else:
                # retrieved compaction settings
                self.set_result(True)
            self.state = FINISHED

        except GetBucketInfoFailed as e:
            # subsequent query failed! exit
            self.state = FINISHED
            self.set_exception(e)
        #catch and set all unexpected exceptions
        except Exception as e:
            self.state = FINISHED
            self.log.info("Unexpected Exception Caught")
            self.set_exception(e)


class MonitorViewFragmentationTask(Task):

    """
        Attempt to monitor fragmentation that is occurring for a given design_doc.
        execute stage is just for preliminary sanity checking of values and environment.

        Check function looks at index file accross all nodes and attempts to calculate
        total fragmentation occurring by the views within the design_doc.

        Note: If autocompaction is enabled and user attempts to monitor for fragmentation
        value higher than level at which auto_compaction kicks in a warning is sent and
        it is best user to use lower value as this can lead to infinite monitoring.
    """

    def __init__(self, server, design_doc_name, fragmentation_value=10, bucket="default"):

        Task.__init__(self, "monitor_frag_task")
        self.server = server
        self.bucket = bucket
        self.fragmentation_value = fragmentation_value
        self.design_doc_name = design_doc_name


    def execute(self, task_manager):

        # sanity check of fragmentation value
        if  self.fragmentation_value < 0 or self.fragmentation_value > 100:
            err_msg = \
                "Invalid value for fragmentation %d" % self.fragmentation_value
            self.state = FINISHED
            self.set_exception(Exception(err_msg))

        # warning if autocompaction is less than <fragmentation_value>
        try:
            auto_compact_percentage = self._get_current_auto_compaction_percentage()
            if auto_compact_percentage != "undefined" and auto_compact_percentage < self.fragmentation_value:
                self.log.warn("Auto compaction is set to %s. Therefore fragmentation_value %s may not be reached" % (auto_compact_percentage, self.fragmentation_value))

            self.state = CHECKING
            task_manager.schedule(self, 5)
        except GetBucketInfoFailed as e:
            self.state = FINISHED
            self.set_exception(e)
        #catch and set all unexpected exceptions
        except Exception as e:
            self.state = FINISHED
            self.log.info("Unexpected Exception Caught")
            self.set_exception(e)


    def _get_current_auto_compaction_percentage(self):
        """ check at bucket level and cluster level for compaction percentage """

        auto_compact_percentage = None
        rest = RestConnection(self.server)

        content = rest.get_bucket_json(self.bucket)
        if content["autoCompactionSettings"] != False:
            auto_compact_percentage = \
                content["autoCompactionSettings"]["viewFragmentationThreshold"]["percentage"]
        else:
            # try to read cluster level compaction settings
            content = rest.cluster_status()
            auto_compact_percentage = \
                content["autoCompactionSettings"]["viewFragmentationThreshold"]["percentage"]

        return auto_compact_percentage

    def check(self, task_manager):

        rest = RestConnection(self.server)
        new_frag_value = MonitorViewFragmentationTask.\
            calc_ddoc_fragmentation(rest, self.design_doc_name, self.bucket)

        self.log.info("current amount of fragmentation = %d" % new_frag_value)
        if new_frag_value > self.fragmentation_value:
            self.state = FINISHED
            self.set_result(True)
        else:
            # try again
            task_manager.schedule(self, 2)

    @staticmethod
    def aggregate_ddoc_info(rest, design_doc_name, bucket="default"):

        nodes = rest.node_statuses()
        info = []
        for node in nodes:
            server_info = {"ip" : node.ip,
                           "port" : node.port,
                           "username" : rest.username,
                           "password" : rest.password}
            rest = RestConnection(server_info)
            status, content = rest.set_view_info(bucket, design_doc_name)
            if status:
                info.append(content)

        return info
    @staticmethod
    def calc_ddoc_fragmentation(rest, design_doc_name, bucket="default"):

        total_disk_size = 0
        total_data_size = 0
        total_fragmentation = 0

        nodes_ddoc_info = \
            MonitorViewFragmentationTask.aggregate_ddoc_info(rest,
                                                         design_doc_name,
                                                         bucket)
        total_disk_size = sum([content['disk_size'] for content in nodes_ddoc_info])
        total_data_size = sum([content['data_size'] for content in nodes_ddoc_info])

        if total_disk_size > 0 and total_data_size > 0:
            total_fragmentation = \
                (total_disk_size - total_data_size) / float(total_disk_size) * 100

        return total_fragmentation

class ViewCompactionTask(Task):

    """
        Executes view compaction for a given design doc. This is technicially view compaction
        as represented by the api and also because the fragmentation is generated by the
        keys emitted by map/reduce functions within views.  Task will check that compaction
        history for design doc is incremented and if any work was really done.
    """

    def __init__(self, server, design_doc_name, bucket="default"):

        Task.__init__(self, "view_compaction_task")
        self.server = server
        self.bucket = bucket
        self.design_doc_name = design_doc_name
        self.ddoc_id = "_design%2f" + design_doc_name
        self.num_of_compactions = 0
        self.precompacted_frag_val = 0

    def execute(self, task_manager):
        rest = RestConnection(self.server)

        try:
            self.num_of_compactions, self.precompacted_frag_val = \
                self._get_compaction_details()
            self.log.info("begin compacting design doc %s" % self.design_doc_name)
            rest.ddoc_compaction(self.ddoc_id)
            self.state = CHECKING
            task_manager.schedule(self, 2)
        except (CompactViewFailed, SetViewInfoNotFound) as ex:
            self.state = FINISHED
            self.set_exception(ex)
        #catch and set all unexpected exceptions
        except Exception as e:
            self.state = FINISHED
            self.log.info("Unexpected Exception Caught")
            self.set_exception(e)

    # verify compaction history incremented and some defraging occurred
    def check(self, task_manager):

        try:
            new_compaction_count, compacted_frag_val = self._get_compaction_details()
            if new_compaction_count == self.num_of_compactions and self._is_compacting():
                # compaction ran sucessfully but compactions was not changed
                # perhaps we are still compacting
                self.log.info("design doc {0} is compacting".format(self.design_doc_name))
                task_manager.schedule(self, 2)
            elif new_compaction_count > self.num_of_compactions:
                frag_val_diff = compacted_frag_val - self.precompacted_frag_val
                self.log.info("fragmentation went from %d to %d" % \
                              (self.precompacted_frag_val, compacted_frag_val))

                if frag_val_diff > 0:
                    # compaction ran sucessfully but datasize still same
                    # perhaps we are still compacting
                    if self._is_compacting():
                        task_manager.schedule(self, 2)

                    # probably we already compacted, but no work needed to be done
                    # returning False
                    self.set_result(False)
                else:
                    self.set_result(True)
                    self.state = FINISHED
            else:
                #Sometimes the compacting is not started immediately
                time.sleep(5)
                if self._is_compacting():
                    task_manager.schedule(self, 2)
                else:
                    self.set_exception(Exception("Check system logs, looks like compaction failed to start"))

        except (SetViewInfoNotFound) as ex:
            self.state = FINISHED
            self.set_exception(ex)
        #catch and set all unexpected exceptions
        except Exception as e:
            self.state = FINISHED
            self.log.info("Unexpected Exception Caught")
            self.set_exception(e)

    def _get_compaction_details(self):
        rest = RestConnection(self.server)
        status, content = rest.set_view_info(self.bucket, self.design_doc_name)
        curr_no_of_compactions = content["stats"]["compactions"]
        curr_ddoc_fragemtation = \
            MonitorViewFragmentationTask.calc_ddoc_fragmentation(rest, self.design_doc_name, self.bucket)
        return (curr_no_of_compactions, curr_ddoc_fragemtation)

    def _is_compacting(self):
        rest = RestConnection(self.server)
        status, content = rest.set_view_info(self.bucket, self.design_doc_name)
        return content["compact_running"] == True

'''task class for failover. This task will only failover nodes but doesn't
 rebalance as there is already a task to do that'''
class FailoverTask(Task):
    def __init__(self, servers, to_failover=[], wait_for_pending=20):
        Task.__init__(self, "failover_task")
        self.servers = servers
        self.to_failover = to_failover
        self.wait_for_pending = wait_for_pending

    def execute(self, task_manager):
        try:
            self._failover_nodes(task_manager)
            self.log.info("{0} seconds sleep after failover, for nodes to go pending....".format(self.wait_for_pending))
            time.sleep(self.wait_for_pending)
            self.state = FINISHED
            self.set_result(True)

        except FailoverFailedException as e:
            self.state = FINISHED
            self.set_exception(e)

        except Exception as e:
            self.state = FINISHED
            self.log.info("Unexpected Exception Caught")
            self.set_exception(e)

    def _failover_nodes(self, task_manager):
        rest = RestConnection(self.servers[0])
        #call REST fail_over for the nodes to be failed over
        for server in self.to_failover:
            for node in rest.node_statuses():
                if server.ip == node.ip and int(server.port) == int(node.port):
                    self.log.info("Failing over {0}:{1}".format(node.ip, node.port))
                    rest.fail_over(node.id)
