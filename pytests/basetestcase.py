import logger
import unittest
import copy
import datetime
import logging
import commands
import json
import traceback
from couchbase_helper.cluster import ServerTasks
from TestInput import TestInputSingleton
from membase.api.rest_client import RestHelper
from membase.helper.cluster_helper import ClusterOperationHelper
from membase.helper.rebalance_helper import RebalanceHelper
from couchbase_helper.data_analysis_helper import *
from security.rbac_base import RbacBase
import testconstants
from scripts.collect_server_info import cbcollectRunner
from bucket_utils.bucket_ready_functions import bucket_utils
from cluster_utils.cluster_ready_functions import cluster_utils
from cluster_utils.cluster_ready_functions import CBCluster
from failover_utils.failover_ready_functions import failover_utils
from node_utils.node_ready_functions import node_utils
from views_utils.view_ready_functions import views_utils
from BucketLib.BucketOperations import BucketHelper

log = logging.getLogger()

class BaseTestCase(unittest.TestCase, failover_utils, node_utils, views_utils):
    def setUp(self):
        self.failover_util = failover_utils()
        self.node_util = node_utils()
        self.views_util = views_utils()
        
        self.log = logger.Logger.get_logger()
        self.input = TestInputSingleton.input
        self.primary_index_created = False
        self.use_sdk_client = self.input.param("use_sdk_client", False)
        if self.input.param("log_level", None):
            log.setLevel(level=0)
            for hd in log.handlers:
                if str(hd.__class__).find('FileHandler') != -1:
                    hd.setLevel(level=logging.DEBUG)
                else:
                    hd.setLevel(level=getattr(logging, self.input.param("log_level", None)))
        self.servers = self.input.servers
        if str(self.__class__).find('moxitests') != -1:
            self.moxi_server = self.input.moxis[0]
            self.servers = [server for server in self.servers
                            if server.ip != self.moxi_server.ip]
        self.buckets = []
        self.cluster = CBCluster(servers=self.input.servers)
        self.bucket_util = bucket_utils(self.cluster)
        self.cluster_util = cluster_utils(self.cluster)
        self.task = ServerTasks()
        self.pre_warmup_stats = {}
        self.cleanup = False
        self.nonroot = False
        shell = RemoteMachineShellConnection(self.cluster.master)
        self.os_info = shell.extract_remote_info().type.lower()
        if self.os_info != 'windows':
            if self.cluster.master.ssh_username != "root":
                self.nonroot = True
        shell.disconnect()
        """ some tests need to bypass checking cb server at set up
            to run installation """
        self.skip_init_check_cbserver = \
            self.input.param("skip_init_check_cbserver", False)
        self.data_collector = DataCollector()
        self.data_analyzer = DataAnalyzer()
        self.result_analyzer = DataAnalysisResultAnalyzer()

        try:
            self.skip_setup_cleanup = self.input.param("skip_setup_cleanup", False)
            self.index_quota_percent = self.input.param("index_quota_percent", None)
            self.targetIndexManager = self.input.param("targetIndexManager", False)
            self.targetMaster = self.input.param("targetMaster", False)
            self.reset_services = self.input.param("reset_services", False)
            self.auth_mech = self.input.param("auth_mech", "PLAIN")
            self.wait_timeout = self.input.param("wait_timeout", 60)
            # number of case that is performed from testrunner( increment each time)
            self.case_number = self.input.param("case_number", 0)
            self.default_bucket = self.input.param("default_bucket", True)
            self.parallelism = self.input.param("parallelism", False)
            self.verify_unacked_bytes = self.input.param("verify_unacked_bytes", False)
            self.enable_flow_control = self.input.param("enable_flow_control", False)
            self.num_servers = self.input.param("servers", len(self.cluster.servers))
            # initial number of items in the cluster
            self.nodes_init = self.input.param("nodes_init", 1)
            self.nodes_in = self.input.param("nodes_in", 1)
            self.nodes_out = self.input.param("nodes_out", 1)
            self.services_init = self.input.param("services_init", None)
            self.services_in = self.input.param("services_in", None)
            self.forceEject = self.input.param("forceEject", False)
            self.force_kill_memcached = TestInputSingleton.input.param('force_kill_memcached', False)
            self.num_items = self.input.param("items", 1000)
            self.value_size = self.input.param("value_size", 512)
            self.dgm_run = self.input.param("dgm_run", False)
            self.active_resident_threshold = int(self.input.param("active_resident_threshold", 0))
            # max items number to verify in ValidateDataTask, None - verify all
            self.max_verify = self.input.param("max_verify", None)
            # we don't change consistent_view on server by default
            self.disabled_consistent_view = self.input.param("disabled_consistent_view", None)
            self.rebalanceIndexWaitingDisabled = self.input.param("rebalanceIndexWaitingDisabled", None)
            self.rebalanceIndexPausingDisabled = self.input.param("rebalanceIndexPausingDisabled", None)
            self.maxParallelIndexers = self.input.param("maxParallelIndexers", None)
            self.maxParallelReplicaIndexers = self.input.param("maxParallelReplicaIndexers", None)
            self.quota_percent = self.input.param("quota_percent", None)
            self.port = None
            self.log_message = self.input.param("log_message", None)
            self.log_info = self.input.param("log_info", None)
            self.log_location = self.input.param("log_location", None)
            self.stat_info = self.input.param("stat_info", None)
            self.port_info = self.input.param("port_info", None)
            if not hasattr(self, 'skip_buckets_handle'):
                self.skip_buckets_handle = self.input.param("skip_buckets_handle", False)
            self.nodes_out_dist = self.input.param("nodes_out_dist", None)
            self.absolute_path = self.input.param("absolute_path", True)
            self.test_timeout = self.input.param("test_timeout", 3600)  # kill hang test and jump to next one.
            self.enable_bloom_filter = self.input.param("enable_bloom_filter", False)
#             self.enable_time_sync = self.input.param("enable_time_sync", False)
            self.gsi_type = self.input.param("gsi_type", 'plasma')

            #jre-path for cbas
            self.jre_path=self.input.param("jre_path",None)
            # end of bucket parameters spot (this is ongoing)

            if self.skip_setup_cleanup:
                self.buckets = BucketHelper(self.master).get_buckets()
                return
            if not self.skip_init_check_cbserver:
                self.cb_version = None
                if RestHelper(RestConnection(self.cluster.master)).is_ns_server_running():
                    """ since every new couchbase version, there will be new features
                        that test code will not work on previous release.  So we need
                        to get couchbase version to filter out those tests. """
                    self.cb_version = RestConnection(self.cluster.master).get_nodes_version()
                else:
                    log.info("couchbase server does not run yet")
                self.protocol = self.cluster_util.get_protocol_type()
            self.services_map = None

            log.info("==============  basetestcase setup was started for test #{0} {1}==============" \
                          .format(self.case_number, self._testMethodName))
            if not self.skip_buckets_handle and not self.skip_init_check_cbserver:
                self.cluster_util._cluster_cleanup(self.bucket_util)

            # avoid any cluster operations in setup for new upgrade
            #  & upgradeXDCR tests
            if str(self.__class__).find('newupgradetests') != -1 or \
                            str(self.__class__).find('upgradeXDCR') != -1 or \
                            str(self.__class__).find('Upgrade_EpTests') != -1 or \
                            hasattr(self, 'skip_buckets_handle') and \
                            self.skip_buckets_handle:
                log.info("any cluster operation in setup will be skipped")
                self.primary_index_created = True
                log.info("==============  basetestcase setup was finished for test #{0} {1} ==============" \
                              .format(self.case_number, self._testMethodName))
                return
            # avoid clean up if the previous test has been tear down
            if self.case_number == 1 or self.case_number > 1000:
                if self.case_number > 1000:
                    log.warn("teardDown for previous test failed. will retry..")
                    self.case_number -= 1000
                self.cleanup = True
                if not self.skip_init_check_cbserver:
                    self.tearDownEverything()
                self.task = ServerTasks()
            if not self.skip_init_check_cbserver:
                log.info("initializing cluster")
                self.cluster_util.reset_cluster(self.targetMaster, self.reset_services)
                master_services = self.cluster_util.get_services(self.servers[:1], \
                                                    self.services_init, \
                                                    start_node=0)
                if master_services != None:
                    master_services = master_services[0].split(",")

                self.quota = self._initialize_nodes(self.task, self.cluster.servers, \
                                                    self.disabled_consistent_view, \
                                                    self.rebalanceIndexWaitingDisabled, \
                                                    self.rebalanceIndexPausingDisabled, \
                                                    self.maxParallelIndexers, \
                                                    self.maxParallelReplicaIndexers, \
                                                    self.port, \
                                                    self.quota_percent, \
                                                    services=master_services)

                self.cluster_util.change_env_variables()
                self.cluster_util.change_checkpoint_params()

                # Add built-in user
                if not self.skip_init_check_cbserver:
                    self.add_built_in_server_user(node=self.cluster.master)
                log.info("done initializing cluster")
            else:
                self.quota = ""
            if self.input.param("log_info", None):
                self.change_log_info()
            if self.input.param("log_location", None):
                self.change_log_location()
            if self.input.param("stat_info", None):
                self.change_stat_info()
            if self.input.param("port_info", None):
                self.change_port_info()
            if self.input.param("port", None):
                self.port = str(self.input.param("port", None))
            try:
                if (str(self.__class__).find('rebalanceout.RebalanceOutTests') != -1) or \
                        (str(self.__class__).find('memorysanitytests.MemorySanity') != -1) or \
                                str(self.__class__).find('negativetests.NegativeTests') != -1 or \
                                str(self.__class__).find('warmuptest.WarmUpTests') != -1 or \
                                str(self.__class__).find('failover.failovertests.FailoverTests') != -1 or \
                                str(self.__class__).find('observe.observeseqnotests.ObserveSeqNoTests') != -1 or \
                                str(self.__class__).find('epengine.lwwepengine.LWW_EP_Engine') != -1:

                    self.services = self.get_services(self.servers, self.services_init)
                    # rebalance all nodes into the cluster before each test
                    self.task.rebalance(self.servers[:self.num_servers], self.servers[1:self.num_servers], [],
                                           services=self.services)
                elif self.nodes_init > 1 and not self.skip_init_check_cbserver:
                    self.services = self.get_services(self.servers[:self.nodes_init], self.services_init)
                    self.task.rebalance(self.servers[:1], \
                                           self.servers[1:self.nodes_init], \
                                           [], services=self.services)
                elif str(self.__class__).find('ViewQueryTests') != -1 and \
                        not self.input.param("skip_rebalance", False):
                    self.services = self.get_services(self.servers, self.services_init)
                    self.task.rebalance(self.servers, self.servers[1:],
                                           [], services=self.services)
                self.cluster_util.setDebugLevel(service_type="index")
            except BaseException, e:
                # increase case_number to retry tearDown in setup for the next test
                self.case_number += 1000
                self.fail(e)

            log.info("==============  basetestcase setup was finished for test #{0} {1} ==============" \
                          .format(self.case_number, self._testMethodName))

            if not self.skip_init_check_cbserver:
                self._log_start(self)
                self.sleep(10)
        except Exception, e:
            traceback.print_exc()
            self.task.shutdown(force=True)
            self.fail(e)

    def get_cbcollect_info(self, server):
        """Collect cbcollectinfo logs for all the servers in the cluster.
        """
        path = TestInputSingleton.input.param("logs_folder", "/tmp")
        print "grabbing cbcollect from {0}".format(server.ip)
        path = path or "."
        try:
            cbcollectRunner(server, path).run()
            TestInputSingleton.input.test_params[
                "get-cbcollect-info"] = False
        except Exception as e:
            log.error("IMPOSSIBLE TO GRAB CBCOLLECT FROM {0}: {1}".format(server.ip, e))

    def tearDown(self):
        self.tearDownEverything()
        
    def tearDownEverything(self):
        if self.skip_setup_cleanup:
            return
        try:
            if hasattr(self, 'skip_buckets_handle') and self.skip_buckets_handle:
                return
            test_failed = (hasattr(self, '_resultForDoCleanups') and \
                           len(self._resultForDoCleanups.failures or \
                               self._resultForDoCleanups.errors)) or \
                          (hasattr(self, '_exc_info') and \
                           self._exc_info()[1] is not None)

            if test_failed and TestInputSingleton.input.param("stop-on-failure", False) \
                    or self.input.param("skip_cleanup", False):
                log.warn("CLEANUP WAS SKIPPED")
            else:

                if test_failed:
                    # collect logs here instead of in test runner because we have not shut things down
                    if TestInputSingleton.param("get-cbcollect-info", False):
                        for server in self.servers:
                            log.info("Collecting logs @ {0}".format(server.ip))
                            self.get_cbcollect_info(server)
                        # collected logs so turn it off so it is not done later
                        TestInputSingleton.input.test_params["get-cbcollect-info"] = False

                    if TestInputSingleton.input.param('get_trace', None):
                        for server in self.servers:
                            try:
                                shell = RemoteMachineShellConnection(server)
                                output, _ = shell.execute_command("ps -aef|grep %s" %
                                                                  TestInputSingleton.input.param('get_trace', None))
                                output = shell.execute_command("pstack %s" % output[0].split()[1].strip())
                                print output[0]
                            except:
                                pass
                    if self.input.param('BUGS', False):
                        log.warn("Test failed. Possible reason is: {0}".format(self.input.param('BUGS', False)))

                log.info("==============  basetestcase cleanup was started for test #{0} {1} ==============" \
                              .format(self.case_number, self._testMethodName))
                rest = RestConnection(self.cluster.master)
                alerts = rest.get_alerts()
                if self.force_kill_memcached:
                    self.kill_memcached()
                if self.forceEject:
                    self.force_eject_nodes()
                if alerts is not None and len(alerts) != 0:
                    log.warn("Alerts were found: {0}".format(alerts))
                if rest._rebalance_progress_status() == 'running':
                    self.kill_memcached()
                    log.warning("rebalancing is still running, test should be verified")
                    stopped = rest.stop_rebalance()
                    self.assertTrue(stopped, msg="unable to stop rebalance")
                self.bucket_util.delete_all_buckets(self.servers)
                ClusterOperationHelper.cleanup_cluster(self.servers, master=self.cluster.master)
                ClusterOperationHelper.wait_for_ns_servers_or_assert(self.servers, self)
                log.info("==============  basetestcase cleanup was finished for test #{0} {1} ==============" \
                              .format(self.case_number, self._testMethodName))
        except BaseException:
            # kill memcached
            self.cluster_util.kill_memcached()
            # increase case_number to retry tearDown in setup for the next test
            self.case_number += 1000
        finally:
            if not self.input.param("skip_cleanup", False):
                self.cluster_util.reset_cluster(self.targetMaster, self.reset_services)
            # stop all existing task manager threads
            if self.cleanup:
                self.cleanup = False
            else:
                self.cluster_util.reset_env_variables()
            self.task.shutdown(force=True)
            self._log_finish(self)

    def get_index_map(self):
        return RestConnection(self.master).get_index_status()

    @staticmethod
    def _log_start(self):
        try:
            msg = "{0} : {1} started ".format(datetime.datetime.now(), self._testMethodName)
            RestConnection(self.servers[0]).log_client_error(msg)
        except:
            pass

    @staticmethod
    def _log_finish(self):
        try:
            msg = "{0} : {1} finished ".format(datetime.datetime.now(), self._testMethodName)
            RestConnection(self.servers[0]).log_client_error(msg)
        except:
            pass

    def sleep(self, timeout=15, message=""):
        log.info("sleep for {0} secs. {1} ...".format(timeout, message))
        time.sleep(timeout)

    def _initialize_nodes(self, cluster, servers, disabled_consistent_view=None, rebalanceIndexWaitingDisabled=None,
                          rebalanceIndexPausingDisabled=None, maxParallelIndexers=None, maxParallelReplicaIndexers=None,
                          port=None, quota_percent=None, services=None):
        quota = 0
        init_tasks = []
        for server in servers:
            init_port = port or server.port or '8091'
            assigned_services = services
            if self.cluster.master != server:
                assigned_services = None
            init_tasks.append(cluster.async_init_node(server, disabled_consistent_view, rebalanceIndexWaitingDisabled,
                                                      rebalanceIndexPausingDisabled, maxParallelIndexers,
                                                      maxParallelReplicaIndexers, init_port,
                                                      quota_percent, services=assigned_services,
                                                      index_quota_percent=self.index_quota_percent,
                                                      gsi_type=self.gsi_type))
        for task in init_tasks:
            node_quota = task.get_result()
            if node_quota < quota or quota == 0:
                quota = node_quota
        if quota < 100 and not len(set([server.ip for server in self.servers])) == 1:
            log.warn("RAM quota was defined less than 100 MB:")
            for server in servers:
                remote_client = RemoteMachineShellConnection(server)
                ram = remote_client.extract_remote_info().ram
                log.info("{0}: {1} MB".format(server.ip, ram))
                remote_client.disconnect()

        if self.jre_path:
            for server in servers:
                rest = RestConnection(server)
                rest.set_jre_path(self.jre_path)
        return quota

    def verify_cluster_stats(self, servers=None, master=None, max_verify=None,
                             timeout=None, check_items=True, only_store_hash=True,
                             replica_to_read=None, batch_size=1000, check_bucket_stats=True,
                             check_ep_items_remaining=False, verify_total_items=True):
        servers = self.get_kv_nodes(servers)
        if servers is None:
            servers = self.servers
        if master is None:
            master = self.master
        if max_verify is None:
            max_verify = self.max_verify

        self._wait_for_stats_all_buckets(servers, timeout=(timeout or 120),
                                         check_ep_items_remaining=check_ep_items_remaining)
        if check_items:
            try:
                self._verify_all_buckets(master, timeout=timeout, max_verify=max_verify,
                                         only_store_hash=only_store_hash, replica_to_read=replica_to_read,
                                         batch_size=batch_size)
            except ValueError, e:
                """ get/verify stats if 'ValueError: Not able to get values
                                             for following keys' was gotten """
                self._verify_stats_all_buckets(servers, timeout=(timeout or 120))
                raise e
            if check_bucket_stats:
                self._verify_stats_all_buckets(servers, timeout=(timeout or 120))
            # verify that curr_items_tot corresponds to sum of curr_items from all nodes
            if verify_total_items:
                verified = True
                for bucket in self.buckets:
                    verified &= RebalanceHelper.wait_till_total_numbers_match(master, bucket)
                self.assertTrue(verified, "Lost items!!! Replication was completed but "
                                          "          sum(curr_items) don't match the curr_items_total")
        else:
            log.warn("verification of items was omitted")

    def _stats_befor_warmup(self, bucket_name):
        self.pre_warmup_stats[bucket_name] = {}
        self.stats_monitor = self.input.param("stats_monitor", "")
        self.warmup_stats_monitor = self.input.param("warmup_stats_monitor", "")
        if self.stats_monitor is not '':
            self.stats_monitor = self.stats_monitor.split(";")
        if self.warmup_stats_monitor is not '':
            self.warmup_stats_monitor = self.warmup_stats_monitor.split(";")
        for server in self.servers:
            mc_conn = MemcachedClientHelper.direct_client(server, bucket_name, self.timeout)
            self.pre_warmup_stats[bucket_name]["%s:%s" % (server.ip, server.port)] = {}
            self.pre_warmup_stats[bucket_name]["%s:%s" % (server.ip, server.port)]["uptime"] = mc_conn.stats("")[
                "uptime"]
            self.pre_warmup_stats[bucket_name]["%s:%s" % (server.ip, server.port)]["curr_items_tot"] = \
                mc_conn.stats("")["curr_items_tot"]
            self.pre_warmup_stats[bucket_name]["%s:%s" % (server.ip, server.port)]["curr_items"] = mc_conn.stats("")[
                "curr_items"]
            for stat_to_monitor in self.stats_monitor:
                self.pre_warmup_stats[bucket_name]["%s:%s" % (server.ip, server.port)][stat_to_monitor] = \
                    mc_conn.stats('')[stat_to_monitor]
            if self.without_access_log:
                for stat_to_monitor in self.warmup_stats_monitor:
                    self.pre_warmup_stats[bucket_name]["%s:%s" % (server.ip, server.port)][stat_to_monitor] = \
                        mc_conn.stats('warmup')[stat_to_monitor]
            mc_conn.close()

    def perform_verify_queries(self, num_views, prefix, ddoc_name, query, wait_time=120,
                               bucket="default", expected_rows=None, retry_time=2, server=None):
        tasks = []
        if server is None:
            server = self.master
        if expected_rows is None:
            expected_rows = self.num_items
        for i in xrange(num_views):
            tasks.append(self.task.async_query_view(server, prefix + ddoc_name,
                                                       self.default_view_name + str(i), query,
                                                       expected_rows, bucket, retry_time))
        try:
            for task in tasks:
                task.result(wait_time)
        except Exception as e:
            print e;
            for task in tasks:
                task.cancel()
            raise Exception("unable to get expected results for view queries during {0} sec".format(wait_time))

    def _verify_replica_distribution_in_zones(self, nodes):
        """
        Verify the replica distribution in nodes in different zones.
        Validate that no replicas of a node are in the same zone.
        :param nodes: Map of the nodes in different zones. Each key contains the zone name and the ip of nodes in that zone.
        """
        shell = RemoteMachineShellConnection(self.servers[0])
        info = shell.extract_remote_info().type.lower()
        if info == 'linux':
            cbstat_command = "{0:s}cbstats".format(testconstants.LINUX_COUCHBASE_BIN_PATH)
        elif info == 'windows':
            cbstat_command = "{0:s}cbstats".format(testconstants.WIN_COUCHBASE_BIN_PATH)
        elif info == 'mac':
            cbstat_command = "{0:s}cbstats".format(testconstants.MAC_COUCHBASE_BIN_PATH)
        else:
            raise Exception("OS not supported.")
        saslpassword = ''
        versions = RestConnection(self.master).get_nodes_versions()
        for group in nodes:
            for node in nodes[group]:
                if versions[0][:5] in testconstants.COUCHBASE_VERSION_2:
                    command = "tap"
                    if not info == 'windows':
                        commands = "%s %s:11210 %s -b %s -p \"%s\" | grep :vb_filter: |  awk '{print $1}' \
                            | xargs | sed 's/eq_tapq:replication_ns_1@//g'  | sed 's/:vb_filter://g' \
                            " % (cbstat_command, node, command, "default", saslpassword)
                    else:
                        commands = "%s %s:11210 %s -b %s -p \"%s\" | grep.exe :vb_filter: | gawk.exe '{print $1}' \
                               | sed.exe 's/eq_tapq:replication_ns_1@//g'  | sed.exe 's/:vb_filter://g' \
                               " % (cbstat_command, node, command, "default", saslpassword)
                    output, error = shell.execute_command(commands)
                elif versions[0][:5] in testconstants.COUCHBASE_VERSION_3 or \
                                versions[0][:5] in testconstants.COUCHBASE_FROM_VERSION_4:
                    command = "dcp"
                    if not info == 'windows':
                        commands = "%s %s:11210 %s -b %s -p \"%s\" | grep :replication:ns_1@%s |  grep vb_uuid | \
                                    awk '{print $1}' | sed 's/eq_dcpq:replication:ns_1@%s->ns_1@//g' | \
                                    sed 's/:.*//g' | sort -u | xargs \
                                   " % (cbstat_command, node, command, "default", saslpassword, node, node)
                        output, error = shell.execute_command(commands)
                    else:
                        commands = "%s %s:11210 %s -b %s -p \"%s\" | grep.exe :replication:ns_1@%s |  grep vb_uuid | \
                                    gawk.exe '{print $1}' | sed.exe 's/eq_dcpq:replication:ns_1@%s->ns_1@//g' | \
                                    sed.exe 's/:.*//g' \
                                   " % (cbstat_command, node, command, "default", saslpassword, node, node)
                        output, error = shell.execute_command(commands)
                        output = sorted(set(output))
                shell.log_command_output(output, error)
                output = output[0].split(" ")
                if node not in output:
                    log.info("{0}".format(nodes))
                    log.info("replicas of node {0} are in nodes {1}".format(node, output))
                    log.info("replicas of node {0} are not in its zone {1}".format(node, group))
                else:
                    raise Exception("replica of node {0} are on its own zone {1}".format(node, group))
        shell.disconnect()

    def add_built_in_server_user(self, testuser=None, rolelist=None, node=None):
        """
           From spock, couchbase server is built with some users that handles
           some specific task such as:
               cbadminbucket
           Default added user is cbadminbucket with admin role
        """
        if node is None:
            node = self.master
        rest = RestConnection(node)
        versions = rest.get_nodes_versions()
        if not versions:
            return
        for version in versions:
            if "5" > version:
                log.info("Atleast one of the nodes in the cluster is "
                              "pre 5.0 version. Hence not creating rbac user "
                              "for the cluster. RBAC is a 5.0 feature.")
                return
        if testuser is None:
            testuser = [{'id': 'cbadminbucket', 'name': 'cbadminbucket',
                         'password': 'password'}]
        if rolelist is None:
            rolelist = [{'id': 'cbadminbucket', 'name': 'cbadminbucket',
                         'roles': 'admin'}]

        log.info("**** add built-in '%s' user to node %s ****" % (testuser[0]["name"],
                                                                       node.ip))
        RbacBase().create_user_source(testuser, 'builtin', node)

        log.info("**** add '%s' role to '%s' user ****" % (rolelist[0]["roles"],
                                                                testuser[0]["name"]))
        status = RbacBase().add_user_role(rolelist, RestConnection(node), 'builtin')
        return status

    def get_index_stats(self, perNode=False):
        servers = self.get_nodes_from_services_map(service_type="index", get_all_nodes=True)
        index_map = None
        for server in servers:
            key = "{0}:{1}".format(server.ip, server.port)
            if perNode:
                if index_map == None:
                    index_map = {}
                index_map[key] = RestConnection(server).get_index_stats(index_map=None)
            else:
                index_map = RestConnection(server).get_index_stats(index_map=index_map)
        return index_map

    def generate_map_nodes_out_dist(self):
        self.index_nodes_out = []
        self.nodes_out_list = []
        self.get_services_map(reset=True)
        if self.nodes_out_dist == None:
            if len(self.servers) > 1:
                self.nodes_out_list.append(self.servers[1])
            return
        for service_fail_map in self.nodes_out_dist.split("-"):
            tokens = service_fail_map.split(":")
            count = 0
            service_type = tokens[0]
            service_type_count = int(tokens[1])
            compare_string_master = "{0}:{1}".format(self.master.ip, self.master.port)
            compare_string_index_manager = "{0}:{1}".format(self.indexManager.ip, self.master.port)
            if service_type in self.services_map.keys():
                for node_info in self.services_map[service_type]:
                    for server in self.servers:
                        compare_string_server = "{0}:{1}".format(server.ip, server.port)
                        addNode = False
                        if (self.targetMaster and (not self.targetIndexManager)) \
                                and (
                                                compare_string_server == node_info and compare_string_master == compare_string_server):
                            addNode = True
                            self.master = self.servers[1]
                        elif ((not self.targetMaster) and (not self.targetIndexManager)) \
                                and (
                                                    compare_string_server == node_info and compare_string_master != compare_string_server \
                                                and compare_string_index_manager != compare_string_server):
                            addNode = True
                        elif ((not self.targetMaster) and self.targetIndexManager) \
                                and (
                                                    compare_string_server == node_info and compare_string_master != compare_string_server
                                        and compare_string_index_manager == compare_string_server):
                            addNode = True
                        if addNode and (server not in self.nodes_out_list) and count < service_type_count:
                            count += 1
                            if service_type == "index":
                                if server not in self.index_nodes_out:
                                    self.index_nodes_out.append(server)
                            self.nodes_out_list.append(server)

    def find_nodes_in_list(self):
        self.nodes_in_list = self.servers[self.nodes_init:self.nodes_init + self.nodes_in]
        self.services_in = self.get_services(self.nodes_in_list, self.services_in, start_node=0)

    def filter_nodes(self, filter_nodes):
        src_nodes = []
        for node in filter_nodes:
            check = False
            for src_node in self.servers:
                if node.ip == src_node.ip:
                    src_nodes.append(src_node)
        return src_nodes

    def calculate_data_change_distribution(self, create_per=0, update_per=0,
                                           delete_per=0, expiry_per=0, start=0, end=0):
        count = end - start
        change_dist_map = {}
        create_count = int(count * create_per)
        start_pointer = start
        end_pointer = start
        if update_per != 0:
            start_pointer = end_pointer
            end_pointer = start_pointer + int(count * update_per)
            change_dist_map["update"] = {"start": start_pointer, "end": end_pointer}
        if expiry_per != 0:
            start_pointer = end_pointer
            end_pointer = start_pointer + int(count * expiry_per)
            change_dist_map["expiry"] = {"start": start_pointer, "end": end_pointer}
        if delete_per != 0:
            start_pointer = end_pointer
            end_pointer = start_pointer + int(count * delete_per)
            change_dist_map["delete"] = {"start": start_pointer, "end": end_pointer}
        if (1 - (update_per + delete_per + expiry_per)) != 0:
            start_pointer = end_pointer
            end_pointer = end
            change_dist_map["remaining"] = {"start": start_pointer, "end": end_pointer}
        if create_per != 0:
            change_dist_map["create"] = {"start": end, "end": create_count + end + 1}
        return change_dist_map

    def set_testrunner_client(self):
        self.testrunner_client = self.input.param("testrunner_client", None)
        if self.testrunner_client != None:
            os.environ[testconstants.TESTRUNNER_CLIENT] = self.testrunner_client

    def generate_full_docs_list(self, gens_load=[], keys=[], update=False):
        all_docs_list = []
        for gen_load in gens_load:
            doc_gen = copy.deepcopy(gen_load)
            while doc_gen.has_next():
                key, val = doc_gen.next()
                try:
                    val = json.loads(val)
                    if isinstance(val, dict) and 'mutated' not in val.keys():
                        if update:
                            val['mutated'] = 1
                        else:
                            val['mutated'] = 0
                    else:
                        val['mutated'] += val['mutated']
                except TypeError:
                    pass
                if keys:
                    if not (key in keys):
                        continue
                all_docs_list.append(val)
        return all_docs_list

    def load(self, generators_load, buckets=None, exp=0, flag=0,
             kv_store=1, only_store_hash=True, batch_size=1, pause_secs=1,
             timeout_secs=30, op_type='create', start_items=0, verify_data=True):
        if not buckets:
            buckets = self.buckets
        gens_load = {}
        for bucket in buckets:
            tmp_gen = []
            for generator_load in generators_load:
                tmp_gen.append(copy.deepcopy(generator_load))
            gens_load[bucket] = copy.deepcopy(tmp_gen)
        tasks = []
        items = 0
        for bucket in buckets:
            for gen_load in gens_load[bucket]:
                items += (gen_load.end - gen_load.start)
        for bucket in buckets:
            log.info("%s %s to %s documents..." % (op_type, items, bucket.name))
            tasks.append(self.task.async_load_gen_docs(self.master, bucket.name,
                                                          gens_load[bucket],
                                                          bucket.kvs[kv_store], op_type, exp, flag,
                                                          only_store_hash, batch_size, pause_secs,
                                                          timeout_secs, compression=self.sdk_compression))
        for task in tasks:
            task.get_result()
        self.num_items = items + start_items
        if verify_data:
            self.verify_cluster_stats(self.servers[:self.nodes_init])
        log.info("LOAD IS FINISHED")
