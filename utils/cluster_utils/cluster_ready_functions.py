'''
Created on Sep 26, 2017

@author: riteshagarwal
'''
import copy

from couchbase_cli import CouchbaseCLI
from membase.api.rest_client import RestConnection, RestHelper
from remote.remote_util import RemoteMachineShellConnection
from ClusterLib.ClusterOperations import ClusterHelper
import logger
import time


class CBCluster:
    
    def __init__(self, name="default",username="Administrator", 
                 password="password", paths=None, servers=None):
        self.name = name
        self.username = username
        self.password = password
        self.paths = paths
        self.ram_settings = {}
        self.servers = servers
        self.nodes_in_cluster = []
        self.master = servers[0]
    
    def update_master(self,master):
        self.master = master
        
class cluster_utils():
    def __init__(self, server):
        self.master = server
        self.rest = RestConnection(server)
        self.log = logger.Logger.get_logger()
        
    def get_nodes_in_cluster(self, master_node=None):
        rest = None
        if master_node == None:
            rest = RestConnection(self.master)
        else:
            rest = RestConnection(master_node)
        nodes = rest.node_statuses()
        server_set = []
        for node in nodes:
            for server in self.servers:
                if server.ip == node.ip:
                    server_set.append(server)
        return server_set

    def change_checkpoint_params(self):
        self.chk_max_items = self.input.param("chk_max_items", None)
        self.chk_period = self.input.param("chk_period", None)
        if self.chk_max_items or self.chk_period:
            for server in self.servers:
                rest = RestConnection(server)
                if self.chk_max_items:
                    rest.set_chk_max_items(self.chk_max_items)
                if self.chk_period:
                    rest.set_chk_period(self.chk_period)

    def change_password(self, new_password="new_password"):
        nodes = RestConnection(self.master).node_statuses()


        cli = CouchbaseCLI(self.master, self.master.rest_username, self.master.rest_password  )
        output, err, result = cli.setting_cluster(data_ramsize=False, index_ramsize=False, fts_ramsize=False, cluster_name=None,
                         cluster_username=None, cluster_password=new_password, cluster_port=False)

        self.log.info(output)
        # MB-10136 & MB-9991
        if not result:
            raise Exception("Password didn't change!")
        self.log.info("new password '%s' on nodes: %s" % (new_password, [node.ip for node in nodes]))
        for node in nodes:
            for server in self.servers:
                if server.ip == node.ip and int(server.port) == int(node.port):
                    server.rest_password = new_password
                    break

    def change_port(self, new_port="9090", current_port='8091'):
        nodes = RestConnection(self.master).node_statuses()
        remote_client = RemoteMachineShellConnection(self.master)
        options = "--cluster-port=%s" % new_port
        cli_command = "cluster-edit"
        output, error = remote_client.execute_couchbase_cli(cli_command=cli_command, options=options,
                                                            cluster_host="localhost:%s" % current_port,
                                                            user=self.master.rest_username,
                                                            password=self.master.rest_password)
        self.log.info(output)
        # MB-10136 & MB-9991
        if error:
            raise Exception("Port didn't change! %s" % error)
        self.port = new_port
        self.log.info("new port '%s' on nodes: %s" % (new_port, [node.ip for node in nodes]))
        for node in nodes:
            for server in self.servers:
                if server.ip == node.ip and int(server.port) == int(node.port):
                    server.port = new_port
                    break

    def change_env_variables(self):
        if self.vbuckets != 1024 or self.upr != None:
            for server in self.servers:
                dict = {}
                if self.vbuckets:
                    dict["COUCHBASE_NUM_VBUCKETS"] = self.vbuckets
                if self.upr != None:
                    if self.upr:
                        dict["COUCHBASE_REPL_TYPE"] = "upr"
                    else:
                        dict["COUCHBASE_REPL_TYPE"] = "tap"
                if len(dict) >= 1:
                    remote_client = RemoteMachineShellConnection(server)
                    remote_client.change_env_variables(dict)
                remote_client.disconnect()
            self.log.info("========= CHANGED ENVIRONMENT SETTING ===========")

    def reset_env_variables(self):
        if self.vbuckets != 1024 or self.upr != None:
            for server in self.servers:
                if self.upr or self.vbuckets:
                    remote_client = RemoteMachineShellConnection(server)
                    remote_client.reset_env_variables()
                    remote_client.disconnect()
            self.log.info("========= RESET ENVIRONMENT SETTING TO ORIGINAL ===========")

    def change_log_info(self):
        for server in self.servers:
            remote_client = RemoteMachineShellConnection(server)
            remote_client.stop_couchbase()
            remote_client.change_log_level(self.log_info)
            remote_client.start_couchbase()
            remote_client.disconnect()
        self.log.info("========= CHANGED LOG LEVELS ===========")

    def change_log_location(self):
        for server in self.servers:
            remote_client = RemoteMachineShellConnection(server)
            remote_client.stop_couchbase()
            remote_client.configure_log_location(self.log_location)
            remote_client.start_couchbase()
            remote_client.disconnect()
        self.log.info("========= CHANGED LOG LOCATION ===========")

    def change_stat_info(self):
        for server in self.servers:
            remote_client = RemoteMachineShellConnection(server)
            remote_client.stop_couchbase()
            remote_client.change_stat_periodicity(self.stat_info)
            remote_client.start_couchbase()
            remote_client.disconnect()
        self.log.info("========= CHANGED STAT PERIODICITY ===========")

    def change_port_info(self):
        for server in self.servers:
            remote_client = RemoteMachineShellConnection(server)
            remote_client.stop_couchbase()
            remote_client.change_port_static(self.port_info)
            server.port = self.port_info
            self.log.info("New REST port %s" % server.port)
            remote_client.start_couchbase()
            remote_client.disconnect()
        self.log.info("========= CHANGED ALL PORTS ===========")

    def force_eject_nodes(self):
        for server in self.servers:
            if server != self.servers[0]:
                try:
                    rest = RestConnection(server)
                    rest.force_eject_node()
                except BaseException, e:
                    self.log.error(e)

    def set_upr_flow_control(self, flow=True, servers=[]):
        servers = self.get_kv_nodes(servers)
        for bucket in self.buckets:
            for server in servers:
                rest = RestConnection(server)
                rest.set_enable_flow_control(bucket=bucket.name, flow=flow)
        for server in self.servers:
            remote_client = RemoteMachineShellConnection(server)
            remote_client.stop_couchbase()
            remote_client.start_couchbase()
            remote_client.disconnect()

    def kill_memcached(self):
        for server in self.servers:
            remote_client = RemoteMachineShellConnection(server)
            remote_client.kill_memcached()
            remote_client.disconnect()
            
    def get_nodes(self, server):
        """ Get Nodes from list of server """
        rest = RestConnection(self.master)
        nodes = rest.get_nodes()
        return nodes
    
    def stop_server(self, node):
        """ Method to stop a server which is subject to failover """
        for server in self.servers:
            if server.ip == node.ip:
                shell = RemoteMachineShellConnection(server)
                if shell.is_couchbase_installed():
                    shell.stop_couchbase()
                    self.log.info("Couchbase stopped")
                else:
                    shell.stop_membase()
                    self.log.info("Membase stopped")
                shell.disconnect()
                break

    def start_server(self, node):
        """ Method to start a server which is subject to failover """
        for server in self.servers:
            if server.ip == node.ip:
                shell = RemoteMachineShellConnection(server)
                if shell.is_couchbase_installed():
                    shell.start_couchbase()
                    self.log.info("Couchbase started")
                else:
                    shell.start_membase()
                    self.log.info("Membase started")
                shell.disconnect()
                break
            
    def reset_cluster(self):
        if self.targetMaster or self.reset_services:
            try:
                for node in self.servers:
                    shell = RemoteMachineShellConnection(node)
                    # Start node
                    rest = RestConnection(node)
                    data_path = rest.get_data_path()
                    # Stop node
                    self.stop_server(node)
                    # Delete Path
                    shell.cleanup_data_config(data_path)
                    self.start_server(node)
                time.sleep(10)
            except Exception, ex:
                self.log.info(ex)

    def get_nodes_from_services_map(self, service_type="n1ql", get_all_nodes=False, servers=None, master=None):
        if not servers:
            servers = self.servers
        if not master:
            master = self.master
        self.get_services_map(master=master)
        if (service_type not in self.services_map):
            self.log.info("cannot find service node {0} in cluster " \
                          .format(service_type))
        else:
            list = []
            for server_info in self.services_map[service_type]:
                tokens = server_info.split(":")
                ip = tokens[0]
                port = int(tokens[1])
                for server in servers:
                    """ In tests use hostname, if IP in ini file use IP, we need
                        to convert it to hostname to compare it with hostname
                        in cluster """
                    if "couchbase.com" in ip and "couchbase.com" not in server.ip:
                        shell = RemoteMachineShellConnection(server)
                        hostname = shell.get_full_hostname()
                        self.log.info("convert IP: {0} to hostname: {1}" \
                                      .format(server.ip, hostname))
                        server.ip = hostname
                        shell.disconnect()
                    if (port != 8091 and port == int(server.port)) or \
                            (port == 8091 and server.ip == ip):
                        list.append(server)
            self.log.info("list of all nodes in cluster: {0}".format(list))
            if get_all_nodes:
                return list
            else:
                return list[0]

    def get_services(self, tgt_nodes, tgt_services, start_node=1):
        services = []
        if tgt_services == None:
            for node in tgt_nodes[start_node:]:
                if node.services != None and node.services != '':
                    services.append(node.services)
            if len(services) > 0:
                return services
            else:
                return None
        if tgt_services != None and "-" in tgt_services:
            services = tgt_services.replace(":", ",").split("-")[start_node:]
        elif tgt_services != None:
            for node in range(start_node, len(tgt_nodes)):
                services.append(tgt_services.replace(":", ","))
        return services

    def setDebugLevel(self, index_servers=None, service_type="kv"):
        index_debug_level = self.input.param("index_debug_level", None)
        if index_debug_level == None:
            return
        if index_servers == None:
            index_servers = self.get_nodes_from_services_map(service_type=service_type, get_all_nodes=True)
        json = {
            "indexer.settings.log_level": "debug",
            "projector.settings.log_level": "debug",
        }
        for server in index_servers:
            RestConnection(server).set_index_settings(json)

    def _version_compatability(self, compatible_version):
        rest = RestConnection(self.master)
        versions = rest.get_nodes_versions()
        for version in versions:
            if compatible_version <= version:
                return True
        return False

    def get_kv_nodes(self, servers=None, master=None):
        if not master:
            master = self.master
        rest = RestConnection(master)
        versions = rest.get_nodes_versions()
        for version in versions:
            if "3.5" > version:
                return servers
        if servers == None:
            servers = self.servers
        kv_servers = self.get_nodes_from_services_map(service_type="kv", get_all_nodes=True,servers=servers, master=master)
        new_servers = []
        for server in servers:
            for kv_server in kv_servers:
                if kv_server.ip == server.ip and kv_server.port == server.port and (server not in new_servers):
                    new_servers.append(server)
        return new_servers

    def get_protocol_type(self):
        rest = RestConnection(self.master)
        versions = rest.get_nodes_versions()
        for version in versions:
            if "3" > version:
                return "tap"
        return "dcp"

    def generate_services_map(self, nodes, services=None):
        map = {}
        index = 0
        if services == None:
            return map
        for node in nodes:
            map[node.ip] = node.services
            index += 1
        return map

    def find_node_info(self, master, node):
        """ Get Nodes from list of server """
        target_node = None
        rest = RestConnection(master)
        nodes = rest.get_nodes()
        for server in nodes:
            if server.ip == node.ip:
                target_node = server
        return target_node

    def add_remove_servers(self, servers=[], list=[], remove_list=[], add_list=[]):
        """ Add or Remove servers from server list """
        initial_list = copy.deepcopy(list)
        for add_server in add_list:
            for server in self.servers:
                if add_server != None and server.ip == add_server.ip:
                    initial_list.append(add_server)
        for remove_server in remove_list:
            for server in initial_list:
                if remove_server != None and server.ip == remove_server.ip:
                    initial_list.remove(server)
        return initial_list

    def add_all_nodes_then_rebalance(self, nodes, wait_for_completion=True):
        otpNodes = []
        if len(nodes)>=1:
            for server in nodes:
                '''This is the case when master node is running cbas service as well'''
                if self.master.ip != server.ip:
                    otpNodes.append(self.rest.add_node(user=server.rest_username,
                                               password=server.rest_password,
                                               remoteIp=server.ip,
                                               port=8091,
                                               services=server.services.split(",")))
    
            self.rebalance(wait_for_completion)
        else:
            self.log.info("No Nodes provided to add in cluster")

        return otpNodes
    
    def rebalance(self, wait_for_completion=True, ejected_nodes = []):
        nodes = self.rest.node_statuses()
        started = self.rest.rebalance(otpNodes=[node.id for node in nodes],ejectedNodes=ejected_nodes)
        if started and wait_for_completion:
            result = self.rest.monitorRebalance()
#             self.assertTrue(result, "Rebalance operation failed after adding %s cbas nodes,"%self.cbas_servers)
            self.log.info("successfully rebalanced cluster {0}".format(result))
        else:
            result = started
        return result
#             self.assertTrue(started, "Rebalance operation started and in progress %s,"%self.cbas_servers)
        
    def remove_all_nodes_then_rebalance(self,otpnodes=None, rebalance=True ):
        return self.remove_node(otpnodes,rebalance) 
        
    def add_node(self, node=None, services=None, rebalance=True, wait_for_rebalance_completion=True):
        if not node:
            self.fail("There is no node to add to cluster.")
        if not services:
            services = node.services.split(",")        
        otpnode = self.rest.add_node(user=node.rest_username,
                               password=node.rest_password,
                               remoteIp=node.ip,
                               port=8091,
                               services=services
                               )
        if rebalance:
            self.rebalance(wait_for_completion=wait_for_rebalance_completion)
        return otpnode
    
    def remove_node(self,otpnode=None, wait_for_rebalance=True):
        nodes = self.rest.node_statuses()
        '''This is the case when master node is running cbas service as well'''
        if len(nodes) <= len(otpnode):
            return
        
        helper = RestHelper(self.rest)
        try:
            removed = helper.remove_nodes(knownNodes=[node.id for node in nodes],
                                              ejectedNodes=[node.id for node in otpnode],
                                              wait_for_rebalance=wait_for_rebalance)
        except Exception as e:
            self.log.info("First time rebalance failed on Removal. Wait and try again. THIS IS A BUG.")
            time.sleep(5)
            removed = helper.remove_nodes(knownNodes=[node.id for node in nodes],
                                              ejectedNodes=[node.id for node in otpnode],
                                              wait_for_rebalance=wait_for_rebalance)
        if wait_for_rebalance:
            removed
#             self.assertTrue(removed, "Rebalance operation failed while removing %s,"%otpnode)
        