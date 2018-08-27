'''
Created on Sep 26, 2017

@author: riteshagarwal
'''
import copy
import logger
import time
import commands
from memcached.helper.data_helper import MemcachedClientHelper
from TestInput import TestInputSingleton
from membase.helper.cluster_helper import ClusterOperationHelper
from remote.remote_util import RemoteUtilHelper, RemoteMachineShellConnection
from membase.api.rest_client import RestConnection
from membase.api.exception import ServerUnavailableException
from scripts.collect_server_info import cbcollectRunner
from testconstants import LINUX_DISTRIBUTION_NAME

class OS:
    WINDOWS = "windows"
    LINUX = "linux"
    OSX = "osx"

class COMMAND:
    SHUTDOWN = "shutdown"
    REBOOT = "reboot"

def raise_if(cond, ex):
    """Raise Exception if condition is True
    """
    if cond:
        raise ex
    
class NodeHelper:
    log = logger.Logger.get_logger()

    @staticmethod
    def disable_firewall(server):
        """Disable firewall to put restriction to replicate items in XDCR.
        @param server: server object to disable firewall
        @param rep_direction: replication direction unidirection/bidirection
        """
        shell = RemoteMachineShellConnection(server)
        shell.info = shell.extract_remote_info()

        if shell.info.type.lower() == "windows":
            output, error = shell.execute_command('netsh advfirewall set publicprofile state off')
            shell.log_command_output(output, error)
            output, error = shell.execute_command('netsh advfirewall set privateprofile state off')
            shell.log_command_output(output, error)
            output, error = shell.execute_command('netsh advfirewall firewall delete rule name="block erl.exe in"')
            shell.log_command_output(output, error)
            output, error = shell.execute_command('netsh advfirewall firewall delete rule name="block erl.exe out"')
            shell.log_command_output(output, error)
        else:
            o, r = shell.execute_command("/sbin/iptables --list")
            shell.log_command_output(o, r)
            if not o:
                raise("Node not reachable yet")
#             o, r = shell.execute_command(
#                 "/sbin/iptables -A INPUT -p tcp -i eth0 --dport 1000:65535 -j ACCEPT")
#             shell.log_command_output(o, r)
#             o, r = shell.execute_command(
#                 "/sbin/iptables -A INPUT -m state --state ESTABLISHED,RELATED -j ACCEPT")
#             shell.log_command_output(o, r)
#             # self.log.info("enabled firewall on {0}".format(server))
            o, r = shell.execute_command("iptables -F")
            shell.log_command_output(o, r)
            o, r = shell.execute_command("/sbin/iptables -t nat -F")
            shell.log_command_output(o, r)
        shell.disconnect()

    @staticmethod
    def reboot_server_new(server, test_case, wait_timeout=120):
        """Reboot a server and wait for couchbase server to run.
        @param server: server object, which needs to be rebooted.
        @param test_case: test case object, since it has assert() function
                        which is used by wait_for_ns_servers_or_assert
                        to throw assertion.
        @param wait_timeout: timeout to whole reboot operation.
        """
        # self.log.info("Rebooting server '{0}'....".format(server.ip))
        shell = RemoteMachineShellConnection(server)
        shell.info = shell.extract_remote_info()
        
        if shell.info.type.lower() == OS.WINDOWS:
            o, r = shell.execute_command(
                "{0} -r -f -t 0".format(COMMAND.SHUTDOWN))
        elif shell.info.type.lower() == OS.LINUX:
            o, r = shell.execute_command(COMMAND.REBOOT)
        shell.log_command_output(o, r)
        # wait for restart and warmup on all server
        if shell.info.type.lower() == OS.WINDOWS:
            time.sleep(wait_timeout * 5)
        else:
            time.sleep(wait_timeout/6)
        end_time = time.time() + 400
        while time.time() < end_time:
            try:
                if shell.info.type.lower() == "windows":
                    o, r = shell.execute_command('netsh advfirewall set publicprofile state off')
                    shell.log_command_output(o, r)
                    o, r = shell.execute_command('netsh advfirewall set privateprofile state off')
                    shell.log_command_output(o, r)
                else:
                # disable firewall on these nodes
                    o, r = shell.execute_command("iptables -F")
                    shell.log_command_output(o, r)
                    o, r = shell.execute_command("/sbin/iptables --list")
                    shell.log_command_output(o, r)
                if not o:
                    raise("Node not reachable yet")
                break
            except:
                print "Node not reachable yet, will try after 10 secs"
                time.sleep(10)
        
        o, r = shell.execute_command("iptables -F")
        # wait till server is ready after warmup
        ClusterOperationHelper.wait_for_ns_servers_or_assert(
            [server],
            test_case,
            wait_if_warmup=True)
        
    @staticmethod
    def reboot_server(server, test_case, wait_timeout=120):
        """Reboot a server and wait for couchbase server to run.
        @param server: server object, which needs to be rebooted.
        @param test_case: test case object, since it has assert() function
                        which is used by wait_for_ns_servers_or_assert
                        to throw assertion.
        @param wait_timeout: timeout to whole reboot operation.
        """
        # self.log.info("Rebooting server '{0}'....".format(server.ip))
        shell = RemoteMachineShellConnection(server)
        if shell.extract_remote_info().type.lower() == OS.WINDOWS:
            o, r = shell.execute_command(
                "{0} -r -f -t 0".format(COMMAND.SHUTDOWN))
        elif shell.extract_remote_info().type.lower() == OS.LINUX:
            o, r = shell.execute_command(COMMAND.REBOOT)
        shell.log_command_output(o, r)
        # wait for restart and warmup on all server
        if shell.extract_remote_info().type.lower() == OS.WINDOWS:
            time.sleep(wait_timeout * 5)
        else:
            time.sleep(wait_timeout/6)
        while True:
            try:
                # disable firewall on these nodes
                NodeHelper.disable_firewall(server)
                break
            except BaseException:
                print "Node not reachable yet, will try after 10 secs"
                time.sleep(10)
        # wait till server is ready after warmup
        ClusterOperationHelper.wait_for_ns_servers_or_assert(
            [server],
            test_case,
            wait_if_warmup=True)

    @staticmethod
    def enable_firewall(server, bidirectional=False, xdcr=False):
        """Enable firewall
        @param server: server object to enable firewall
        @param rep_direction: replication direction unidirection/bidirection
        """
        
        """ Check if user is root or non root in unix """
        shell = RemoteMachineShellConnection(server)
        shell.info = shell.extract_remote_info()
        if shell.info.type.lower() == "windows":
            o, r = shell.execute_command('netsh advfirewall set publicprofile state on')
            shell.log_command_output(o, r)
            o, r = shell.execute_command('netsh advfirewall set privateprofile state on')
            shell.log_command_output(o, r)
            NodeHelper.log.info("enabled firewall on {0}".format(server))
            suspend_erlang = shell.windows_process_utils("pssuspend.exe", "erl.exe", option="")
            if suspend_erlang:
                NodeHelper.log.info("erlang process is suspended")
            else:
                NodeHelper.log.error("erlang process failed to suspend")
        else:
            copy_server = copy.deepcopy(server)
            command_1 = "/sbin/iptables -A INPUT -p tcp -i eth0 --dport 1000:65535 -j REJECT"
            command_2 = "/sbin/iptables -A OUTPUT -p tcp -o eth0 --sport 1000:65535 -j REJECT"
            command_3 = "/sbin/iptables -A INPUT -m state --state ESTABLISHED,RELATED -j ACCEPT"
            if shell.info.distribution_type.lower() in LINUX_DISTRIBUTION_NAME \
                             and server.ssh_username != "root":
                copy_server.ssh_username = "root"
                shell.disconnect()
                NodeHelper.log.info("=== connect to server with user %s " % copy_server.ssh_username)
                shell = RemoteMachineShellConnection(copy_server)
                o, r = shell.execute_command("whoami")
                shell.log_command_output(o, r)
            # Reject incoming connections on port 1000->65535
            o, r = shell.execute_command(command_1)
            shell.log_command_output(o, r)
            # Reject outgoing connections on port 1000->65535
            if bidirectional:
                o, r = shell.execute_command(command_2)
                shell.log_command_output(o, r)
            if xdcr:
                o, r = shell.execute_command(command_3)
                shell.log_command_output(o, r)
            NodeHelper.log.info("enabled firewall on {0}".format(server))
            o, r = shell.execute_command("/sbin/iptables --list")
            shell.log_command_output(o, r)
            shell.disconnect()
            
    @staticmethod
    def do_a_warm_up(server):
        """Warmp up server
        """
        shell = RemoteMachineShellConnection(server)
        shell.stop_couchbase()
        time.sleep(5)
        shell.start_couchbase()
        shell.disconnect()

    @staticmethod
    def start_couchbase(server):
        """Warmp up server
        """
        shell = RemoteMachineShellConnection(server)
        shell.start_couchbase()
        shell.disconnect()

    @staticmethod
    def stop_couchbase(server):
        """Warmp up server
        """
        shell = RemoteMachineShellConnection(server)
        shell.stop_couchbase()
        shell.disconnect()

    @staticmethod
    def set_cbft_env_fdb_options(server):
        shell = RemoteMachineShellConnection(server)
        shell.stop_couchbase()
        cmd = "sed -i 's/^export CBFT_ENV_OPTIONS.*$/" \
              "export CBFT_ENV_OPTIONS=bleveMaxResultWindow=10000000," \
              "forestdbCompactorSleepDuration={0},forestdbCompactionThreshold={1}/g'\
              /opt/couchbase/bin/couchbase-server".format(
            int(TestInputSingleton.input.param("fdb_compact_interval", None)),
            int(TestInputSingleton.input.param("fdb_compact_threshold", None)))
        shell.execute_command(cmd)
        shell.start_couchbase()
        shell.disconnect()

    @staticmethod
    def wait_service_started(server, wait_time=120):
        """Function will wait for Couchbase service to be in
        running phase.
        """
        shell = RemoteMachineShellConnection(server)
        os_type = shell.extract_remote_info().distribution_type
        if os_type.lower() == 'windows':
            cmd = "sc query CouchbaseServer | grep STATE"
        else:
            cmd = "service couchbase-server status"
        now = time.time()
        while time.time() - now < wait_time:
            output, _ = shell.execute_command(cmd)
            if str(output).lower().find("running") != -1:
                # self.log.info("Couchbase service is running")
                return
            time.sleep(10)
        raise Exception(
            "Couchbase service is not running after {0} seconds".format(
                wait_time))

    @staticmethod
    def wait_warmup_completed(warmupnodes, bucket_names=["default"]):
        if isinstance(bucket_names, str):
            bucket_names = [bucket_names]
        start = time.time()
        for server in warmupnodes:
            for bucket in bucket_names:
                while time.time() - start < 150:
                    try:
                        mc = MemcachedClientHelper.direct_client(server, bucket)
                        if mc.stats()["ep_warmup_thread"] == "complete":
                            NodeHelper._log.info(
                                "Warmed up: %s items on %s on %s" %
                                (mc.stats("warmup")["ep_warmup_key_count"], bucket, server))
                            time.sleep(10)
                            break
                        elif mc.stats()["ep_warmup_thread"] == "running":
                            NodeHelper._log.info(
                                "Still warming up .. ep_warmup_key_count : %s" % (
                                    mc.stats("warmup")["ep_warmup_key_count"]))
                            continue
                        else:
                            NodeHelper._log.info(
                                "Value of ep_warmup_thread does not exist, exiting from this server")
                            break
                    except Exception as e:
                        NodeHelper._log.info(e)
                        time.sleep(10)
                if mc.stats()["ep_warmup_thread"] == "running":
                    NodeHelper._log.info(
                        "ERROR: ep_warmup_thread's status not complete")
                mc.close()

    @staticmethod
    def wait_node_restarted(
            server, test_case, wait_time=120, wait_if_warmup=False,
            check_service=False):
        """Wait server to be re-started
        """
        now = time.time()
        if check_service:
            NodeHelper.wait_service_started(server, wait_time)
            wait_time = now + wait_time - time.time()
        num = 0
        while num < wait_time / 10:
            try:
                ClusterOperationHelper.wait_for_ns_servers_or_assert(
                    [server], test_case, wait_time=wait_time - num * 10,
                    wait_if_warmup=wait_if_warmup)
                break
            except ServerUnavailableException:
                num += 1
                time.sleep(10)

    @staticmethod
    def kill_erlang(server):
        """Kill erlang process running on server.
        """
        NodeHelper._log.info("Killing erlang on server: {0}".format(server))
        shell = RemoteMachineShellConnection(server)
        os_info = shell.extract_remote_info()
        shell.kill_erlang(os_info)
        shell.start_couchbase()
        shell.disconnect()
        NodeHelper.wait_warmup_completed([server])

    @staticmethod
    def kill_memcached(server):
        """Kill memcached process running on server.
        """
        shell = RemoteMachineShellConnection(server)
        shell.kill_memcached()
        shell.disconnect()

    @staticmethod
    def kill_cbft_process(server):
        NodeHelper._log.info("Killing cbft on server: {0}".format(server))
        shell = RemoteMachineShellConnection(server)
        shell.kill_cbft_process()
        shell.disconnect()

    @staticmethod
    def get_log_dir(node):
        """Gets couchbase log directory, even for cluster_run
        """
        _, dir = RestConnection(node).diag_eval(
            'filename:absname(element(2, application:get_env(ns_server,error_logger_mf_dir))).')
        return str(dir)

    @staticmethod
    def get_data_dir(node):
        """Gets couchbase data directory, even for cluster_run
        """
        _, dir = RestConnection(node).diag_eval(
            'filename:absname(element(2, application:get_env(ns_server,path_config_datadir))).')

        return str(dir).replace('\"','')

    @staticmethod
    def rename_nodes(servers):
        """Rename server name from ip to their hostname
        @param servers: list of server objects.
        @return: dictionary whose key is server and value is hostname
        """
        hostnames = {}
        for server in servers:
            shell = RemoteMachineShellConnection(server)
            try:
                hostname = shell.get_full_hostname()
                rest = RestConnection(server)
                renamed, content = rest.rename_node(
                    hostname, username=server.rest_username,
                    password=server.rest_password)
                raise_if(
                    not renamed,
                    Exception(
                        "Server %s is not renamed! Hostname %s. Error %s" % (
                            server, hostname, content)
                    )
                )
                hostnames[server] = hostname
                server.hostname = hostname
            finally:
                shell.disconnect()
        return hostnames

    # Returns version like "x.x.x" after removing build number
    @staticmethod
    def get_cb_version(node):
        rest = RestConnection(node)
        version = rest.get_nodes_self().version
        return version[:version.rfind('-')]

    @staticmethod
    def get_cbcollect_info(server):
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
            NodeHelper._log.error(
                "IMPOSSIBLE TO GRAB CBCOLLECT FROM {0}: {1}".format(
                    server.ip,
                    e))

    @staticmethod
    def collect_logs(server):
        """Grab cbcollect before we cleanup
        """
        NodeHelper.get_cbcollect_info(server)


class node_utils():
    def kill_server_memcached(self, node):
        """ Method to start a server which is subject to failover """
        for server in self.servers:
            if server.ip == node.ip:
                remote_client = RemoteMachineShellConnection(server)
                remote_client.kill_memcached()
                remote_client.disconnect()

    def start_firewall_on_node(self, node):
        """ Method to start a server which is subject to failover """
        for server in self.servers:
            if server.ip == node.ip:
                NodeHelper.enable_firewall(server)

    def stop_firewall_on_node(self, node):
        """ Method to start a server which is subject to failover """
        for server in self.servers:
            if server.ip == node.ip:
                remote_client = RemoteMachineShellConnection(server)
                remote_client.disable_firewall()
                remote_client.disconnect()

    def get_victim_nodes(self, nodes, master=None, chosen=None, victim_type="master", victim_count=1):
        victim_nodes = [master]
        if victim_type == "graceful_failover_node":
            victim_nodes = [chosen]
        elif victim_type == "other":
            victim_nodes = self.add_remove_servers(nodes, nodes, [chosen, self.master], [])
            victim_nodes = victim_nodes[:victim_count]
        return victim_nodes

    def get_services_map(self, reset=True, master=None):
        if not reset:
            return
        else:
            self.services_map = {}
        if not master:
            master = self.master
        rest = RestConnection(master)
        map = rest.get_nodes_services()
        for key, val in map.iteritems():
            for service in val:
                if service not in self.services_map.keys():
                    self.services_map[service] = []
                self.services_map[service].append(key)

    def get_nodes_services(self):
        list_nodes_services = {}
        rest = RestConnection(self.master)
        map = rest.get_nodes_services()
        for k, v in map.iteritems():
            if "8091" in k:
                k = k.replace(":8091", "")
            if len(v) == 1:
                v = v[0]
            elif len(v) > 1:
                v = ",".join(v)
            list_nodes_services[k] = v
        """ return {"IP": "kv", "IP": "index,kv"} """
        return list_nodes_services

    def _kill_nodes(self, nodes, servers, bucket_name):
        self.reboot = self.input.param("reboot", False)
        if not self.reboot:
            for node in nodes:
                _node = {"ip": node.ip, "port": node.port, "username": self.servers[0].rest_username,
                         "password": self.servers[0].rest_password}
                node_rest = RestConnection(_node)
                _mc = MemcachedClientHelper.direct_client(_node, bucket_name)
                self.log.info("restarted the node %s:%s" % (node.ip, node.port))
                pid = _mc.stats()["pid"]
                command = "os:cmd(\"kill -9 {0} \")".format(pid)
                self.log.info(command)
                killed = node_rest.diag_eval(command)
                self.log.info("killed ??  {0} ".format(killed))
                _mc.close()
        else:
            for server in servers:
                shell = RemoteMachineShellConnection(server)
                command = "reboot"
                output, error = shell.execute_command(command)
                shell.log_command_output(output, error)
                shell.disconnect()
                time.sleep(self.wait_timeout * 8)
                shell = RemoteMachineShellConnection(server)
                command = "/sbin/iptables -F"
                output, error = shell.execute_command(command)
                command = "iptables -F"
                output, error = shell.execute_command(command)
                shell.log_command_output(output, error)
                shell.disconnect()

    def _restart_memcache(self, bucket_name):
        rest = RestConnection(self.master)
        nodes = rest.node_statuses()
        is_partial = self.input.param("is_partial", "True")
        _nodes = []
        if len(self.servers) > 1:
            skip = 2
        else:
            skip = 1
        if is_partial:
            _nodes = nodes[:len(nodes):skip]
        else:
            _nodes = nodes

        _servers = []
        for server in self.servers:
            for _node in _nodes:
                if server.ip == _node.ip:
                    _servers.append(server)

        self._kill_nodes(_nodes, _servers, bucket_name)

        start = time.time()
        memcached_restarted = False

        for server in _servers:
            while time.time() - start < (self.wait_timeout * 2):
                mc = None
                try:
                    mc = MemcachedClientHelper.direct_client(server, bucket_name)
                    stats = mc.stats()
                    new_uptime = int(stats["uptime"])
                    self.log.info("New warmup uptime %s:%s" % (
                        new_uptime, self.pre_warmup_stats[bucket_name]["%s:%s" % (server.ip, server.port)]["uptime"]))
                    if new_uptime < self.pre_warmup_stats[bucket_name]["%s:%s" % (server.ip, server.port)]["uptime"]:
                        self.log.info("memcached restarted...")
                        memcached_restarted = True
                        break;
                except Exception:
                    self.log.error("unable to connect to %s:%s for bucket %s" % (server.ip, server.port, bucket_name))
                    if mc:
                        mc.close()
                    time.sleep(5)
            if not memcached_restarted:
                self.fail("memcached did not start %s:%s for bucket %s" % (server.ip, server.port, bucket_name))
                
    def wait_service_started(self, server, wait_time=120):
        shell = RemoteMachineShellConnection(server)
        type = shell.extract_remote_info().distribution_type
        if type.lower() == 'windows':
            cmd = "sc query CouchbaseServer | grep STATE"
        else:
            cmd = "service couchbase-server status"
        now = time.time()
        while time.time() - now < wait_time:
            output, error = shell.execute_command(cmd)
            if str(output).lower().find("running") != -1:
                self.log.info("Couchbase service is running")
                shell.disconnect()
                return
            else:
                self.log.warn("couchbase service is not running. {0}".format(output))
                self.sleep(10)
        shell.disconnect()
        self.fail("Couchbase service is not running after {0} seconds".format(wait_time))


    '''
    Returns ip address of the requesting machine
    '''

    def getLocalIPAddress(self):
        status, ipAddress = commands.getstatusoutput(
            "ifconfig eth0 | grep 'inet addr:' | cut -d: -f2 |awk '{print $1}'")
        return ipAddress


    # get the dot version e.g. x.y
    def _get_version(self):
        rest = RestConnection(self.master)
        version = rest.get_nodes_self().version
        return float(version[:3])

    def wait_node_restarted(self, server, wait_time=120, wait_if_warmup=False, check_service=False):
        now = time.time()
        if check_service:
            self.wait_service_started(server, wait_time)
            wait_time = now + wait_time - time.time()
        num = 0
        while num < wait_time / 10:
            try:
                ClusterOperationHelper.wait_for_ns_servers_or_assert(
                    [server], self, wait_time=wait_time - num * 10, wait_if_warmup=wait_if_warmup)
                break
            except ServerUnavailableException:
                num += 1
                self.sleep(10)
