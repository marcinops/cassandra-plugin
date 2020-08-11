#
# Copyright (c) 2020 by Delphix. All rights reserved.
#

import os
import time
import json
import re
import itertools
from dlpx.virtualization.platform.exceptions import UserError
from dlpx.virtualization.platform import Status
from controller.helper import execute_bash
from cassandra.config_file import config_file
from cassandra.medusa import Medusa
from cassandra.backup_op import Backup_factory
from controller.helper import make_nonprimary_connection

import logging
logger = logging.getLogger(__name__)


class Node_ctl(object):
    """
    Class for managing Cassandra cluster node
    It has methods for node operations like start, stop, create or restore
    """

    def __init__(self, node_conf, config, node_no):
        """
        Class constructor
        param1: node_conf: a dict with node name and node port
        param2: config: a cluster wide configuration inherited from cluster object
        param3: node_no: number of node ( based on this a node path is created)
        """

        logger.debug("Creating node with name {} port {} and number {}".format(node_conf["nodeName"], node_conf["port"], node_no))
        self.config = config

        if config.staged_source is not None:
            # this node is part of the staging cluster
            self.__mount_path = config.staged_source.mount.mount_path
            self.__cluster_name = config.staged_source.parameters.staging_cluster_name
            self.__schema_file = config.staged_source.parameters.schema_file_name
            
        elif config.virtual_source is not None:
            # this node is part of the VDB cluster
            self.__cluster_name = config.virtual_source.parameters.cluster_name
            self.__mount_path = os.path.join(config.virtual_source.parameters.mount_location, self.__cluster_name)
        else:
            # for test
            self.__mount_path = "/mnt"
            self.__cluster_name = "test"   

        if config.repository is not None:
            self.__cassandra_home = config.repository.cassandra_home
        else:
            # for test
            self.__cassandra_home = "/cassandra"

        self.__node_name = node_conf["nodeName"]
        self.__node_port = node_conf["port"]

        if "environment" in node_conf:
            self.__node_environment = node_conf["environment"]
            self.__node_envuser = node_conf["environmentUser"]
            self.__node_local = False
        else:
            self.__node_local = True
            self.__node_environment = None
            self.__node_envuser = None

        self.__node_dir = os.path.join(self.__mount_path, "127.0.0.{}".format(node_no))
        
        self.__env = {
            "PATH": "{}/bin".format(self.__cassandra_home),
            "CASSANDRA_CONF": os.path.join(self.__node_dir, "conf")
        }
        self.__node_no = node_no

        if config.dSource:
            self.__node_jmx_port = 7000 + int(node_no)*100
        else:
            # one cassandra per node so leave default
            self.__node_jmx_port = 7199
        
        self.__backup_drv = Backup_factory(self)
        logger.debug("cluster name: {}".format(self.__cluster_name))
        logger.debug("node_dir: {}".format(self.__node_dir))
        logger.debug("cassandra conf: {}".format(os.path.join(self.__node_dir, "conf")))
        logger.debug("local node: {}".format(str(self.__node_local)))
        logger.debug("node environment: {}".format(str(self.__node_environment)))
        logger.debug("node env user: {}".format(str(self.__node_envuser)))


    @property
    def delphix_dir(self):
        return "{}/.delphix".format(self.__mount_path)

    @property
    def node_dir(self):
        return self.__node_dir

    @property
    def node_name(self):
        return self.__node_name

    @property
    def mount_path(self):
        return self.__mount_path

    @mount_path.setter
    def mount_path(self, value):
        self.__mount_path = value

    def is_node_exists(self):
        """
        Check if node existing by checking directory structrure
        """
        ret = execute_bash(self.config.connection, "check_dir", path=self.node_dir)
        if ret.exit_code == 0:
            return True
        else:
            return False


    def create_node(self):
        """
        Create a staging node 
        """
        
        # create all cassandra cluster directory to create a new cluster
        for dir in ['data','commitlog','save_caches','hints_directory', 'logs', 'bin', 'conf']:
            node_dir_tree = os.path.join(self.node_dir, dir)
            ret = execute_bash(self.config.connection, "create_dir", path=node_dir_tree)
            if ret.exit_code != 0:
                raise UserError(
                    'Failed to create Cassandra node directory',
                    'Make sure the user has appropriate permissions',
                    '{}\n{}'.format(ret.stdout, ret.stderr))

        # create delphix special dir
        ret = execute_bash(self.config.connection, "create_dir", path="{}/.delphix".format(self.mount_path))

        # generate a configuration and start a node
        self.copy_cassandra_files()
        self.save_config()
        self.generate_logging()
        self.generate_env()
        self.start_node()

    def delete_node(self):
        """
        Delete a staging node before resync
        """
        
        for dir in ['data','commitlog','save_caches','hints_directory']:
            node_dir_tree = os.path.join(self.node_dir, dir)
            ret = execute_bash(self.config.connection, "delete_dir", path=node_dir_tree)
            if ret.exit_code != 0:
                raise UserError(
                    'Failed to delete Cassandra node directory',
                    'Make sure the user has appropriate permissions',
                    '{}\n{}'.format(ret.stdout, ret.stderr))
            ret = execute_bash(self.config.connection, "create_dir", path=node_dir_tree)
            if ret.exit_code != 0:
                raise UserError(
                    'Failed to create Cassandra node directory',
                    'Make sure the user has appropriate permissions',
                    '{}\n{}'.format(ret.stdout, ret.stderr))

        self.delete_loaded_manifest()

    def create_vdb_node(self):
        """
        Create a VDB node 
        """

        # generate a new config and logging file but keep all data files
        self.copy_cassandra_files() 
        self.save_config()
        self.generate_logging()


        # delete local and peer directories to rename cluster and peer list
        # TODO
        # error handling
        path = os.path.join(self.node_dir, "data", "system", "local-*")
        ret = execute_bash(self.config.connection, "delete_dir", path=path)        
        self.fix_peers()  

        # check if commit logs are formated and clean up if not
        self.check_commit_log()
        self.start_node()


        
    def fix_peers(self):
        """
        Delete a peers table to clean up a list of peers
        Cassandra will recreate it automatically after start
        TODO
        check a replace IP option in env 
        """
        path = os.path.join(self.node_dir, "data", "system", "peers-*")
        ret = execute_bash(self.config.connection, "delete_dir", path=path)        


    def check_commit_log(self):
        """
        Check if commit log is fully formated
        If commit log is created but never used - it has only zeros
        and this prevent node to restart and this commit log has to be deleted
        """
        path = os.path.join(self.node_dir, "commitlog", "CommitLog*")
        ret = execute_bash(self.config.connection, "read_header", file=path, bytes=16)
        todelete = None
        lprev = None   
        # go through output of head command run on all commit logs      
        for l1 in ret.stdout.split('\n'):
            if "==>" in l1:
                lprev = l1
                continue
            
            if l1 == '\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00':
                logger.debug("commit log {} header is zero - remove it".format(lprev))
                todelete = lprev

        # if a 2nd commit log has 0 in header, output is not displayed but it needs to be removed anyway
        if todelete is None and len(ret.stdout.split('\n'))<4:
            todelete = ret.stdout.split('\n')[-1]

        if todelete is not None:
            todelete = todelete.replace("==>", '').replace("<==", '').strip()

            logger.debug("Deleting log - {}".format(todelete))
            ret = execute_bash(self.config.connection, "delete_dir", path=todelete)  
            # TODO
            # add error support



    def copy_cassandra_files(self):
        """
        Copy a binary and configuration files from Cassandra directory into mount point
        """

        for subdir in ['bin', 'conf']:
            src_bin_dir = os.path.join(self.__cassandra_home, subdir, '*')
            tgt_bin_dir = os.path.join(self.node_dir, subdir)
            ret = execute_bash(self.config.connection, "copy_files", source_path=src_bin_dir, target_path=tgt_bin_dir)
            if ret.exit_code != 0:
                raise UserError(
                    'Failed to copy Cassandra binary and config files',
                    'Make sure the user has appropriate permissions',
                    '{}\n{}'.format(ret.stdout, ret.stderr))


    def generate_config(self):
        """
        Generate a configuration
        based on the node parameters
        """

        seed = ",".join([x["nodeName"] for x in self.config.node_list])

        if self.config.dSource:
            (num_token, initial_token) = self.get_initial_token()
            params = {
                "cluster_name" : self.__cluster_name,
                "seed" : seed,
                "listen_address": self.__node_name,
                "transport_port": self.__node_port,
                "data_dir": os.path.join(self.__mount_path, self.__node_name, "data"),
                "log_dir": os.path.join(self.__mount_path, self.__node_name, "commitlog"),
                "saved_dir": os.path.join(self.__mount_path, self.__node_name, "save_caches"),
                "hint_dir": os.path.join(self.__mount_path, self.__node_name, "hints_directory"),
                "num_token": num_token,
                "initial_token": initial_token
            }
        else:
            (num_token, initial_token) = self.get_initial_token_for_vdb()
            params = {
                "cluster_name" : self.__cluster_name,
                "seed" : seed,
                "listen_address": self.__node_name,
                "transport_port": self.__node_port,
                "data_dir": os.path.join(self.__node_dir, "data"),
                "log_dir": os.path.join(self.__node_dir, "commitlog"),
                "saved_dir": os.path.join(self.__node_dir, "save_caches"),
                "hint_dir": os.path.join(self.__node_dir, "hints_directory"),
                "num_token": num_token,
                "initial_token": initial_token
            }  
        return config_file.format(**params)

    def get_initial_token_for_vdb(self):
        """
        Return a list of the initial_token list for node
        """

        # TODO
        # error handling

        # take list of tokens from snapshot
        tokens = self.config.snapshot.init_tokens
        node_tokens = tokens[self.__node_no-1]
        node_count = node_tokens["tokens"].count(',')+1
        logger.debug("Tokens for node {}: no of tokens: {} \n list: {}".format(self.node_name, node_count, node_tokens))

        return (node_count, node_tokens["tokens"])


    def generate_logging(self):
        """
        Generate a logging configuration
        Change a path to mount point/node_name/logging directory 
        """
        try:
            logback_path = os.path.join(self.node_dir, "conf", "logback.xml")
            logger.debug("Reading logback.xml file from {}".format(logback_path))
            ret = execute_bash(self.config.connection, "read_file", file=logback_path)
            if ret.exit_code != 0:
                raise RuntimeError

            logger.debug("changing path to {}".format(os.path.join(self.node_dir, "logs")))
            newlog = ret.stdout.replace("\"", "\\\"")
            newlog = newlog.replace("${cassandra.logdir}", os.path.join(self.node_dir, "logs"))

            logger.debug("Writing logback.xml file to {}".format(logback_path))
            ret = execute_bash(self.config.connection, "create_file", content=newlog, file=logback_path)
            if ret.exit_code != 0:
                raise RuntimeError       
        
        except RuntimeError:
            raise UserError(
                'Failed to process logback.xml file',
                'Make sure the user has appropriate permissions',
                '{}\n{}'.format(ret.stdout, ret.stderr))      


    def generate_env(self):
        """
        Generate an environment node file
        Change a port of JMX to one specified in node
        this is executed only on staging cluster only 
        """
        try:
            cass_env_file = os.path.join(self.node_dir, "conf", "cassandra-env.sh")
            logger.debug("Fixing cassandra-env.sh file in {}".format(cass_env_file))
            ret = execute_bash(self.config.connection, "sed", find="JMX_PORT=\\\"7199\\\"", replace="JMX_PORT=\"{}\"".format(self.__node_jmx_port), file=cass_env_file)
            if ret.exit_code != 0:
                raise RuntimeError
        
        except RuntimeError:
            raise UserError(
                'Failed to process cassandra-env.sh file',
                'Make sure the user has appropriate permissions',
                '{}\n{}'.format(ret.stdout, ret.stderr))   


    def save_config(self):
        """
        Save config file for node
        """
        lines = self.generate_config()
        tgt_bin_dir = os.path.join(self.node_dir, "conf", "cassandra.yaml")
        ret = execute_bash(self.config.connection, "create_file", content=lines, file=tgt_bin_dir)
        if ret.exit_code != 0:
            raise UserError(
                'Failed to create config file',
                'Make sure the user has appropriate permissions',
                '{}\n{}'.format(ret.stdout, ret.stderr))

        
    def start_node(self):
        """
        Start a node
        """
        logger.debug("Start node {}".format(self.node_name))

        if self.__node_local:
            logger.debug("it will start on main envioronment")
            connection = self.config.connection
        else:
            logger.debug("it will start on an additional environment {}".format(str(self.__node_environment)))
            connection=make_nonprimary_connection(self.config.connection, self.__node_environment, self.__node_envuser)
            
        if self.node_status() == Status.INACTIVE:
            # start a node
            pid_file = os.path.join(self.node_dir, "{}.pid".format(self.node_name))
            ret = execute_bash(connection, "start_cassandra", node_location=self.node_dir, pid_file=pid_file, environment_vars=self.__env)
            if ret.exit_code != 0:
                raise UserError(
                    'Failed to start a staging cassandra node {}'.format(self.node_name),
                    'Make sure the user has appropriate permissions',
                    '{}\n{}'.format(ret.stdout, ret.stderr))

            # wait until node is in running mode
            for i in range(1,10):
                status = self.node_status() 
                if status == Status.INACTIVE:
                    time.sleep(30)
                else:
                    break

            # not able to start node after 300 sec - raise an error
            if status == Status.INACTIVE:
                raise UserError(
                    'Failed to start a staging cassandra node {}'.format(self.node_name),
                    'Make sure the user has appropriate permissions',
                    '{}\n{}'.format(ret.stdout, ret.stderr))

    def stop_node(self):
        """
        Stop a node
        """
        logger.debug("Stop node {}".format(self.node_name))

        if self.__node_local:
            logger.debug("it will start on main envioronment")
            connection = self.config.connection
        else:
            logger.debug("it will start on an additional environment {}".format(str(self.__node_environment)))
            connection=make_nonprimary_connection(self.config.connection, self.__node_environment, self.__node_envuser)

        if self.node_status() == Status.ACTIVE:
            # if node was active - kick off nodetool shutdown command
            ret = execute_bash(connection, "stop_cassandra", node_port=self.__node_jmx_port,  environment_vars=self.__env)
            if "Cassandra has shutdown" in ret.stdout: 
                logger.debug("Cassandra deamon is down")
            else:
                raise UserError(
                    'Failed to stop a cassandra node {}'.format(self.node_name),
                    'Make sure the user has appropriate permissions',
                    '{}\n{}'.format(ret.stdout, ret.stderr))


    def node_status(self):
        """
        Check node status
        """
        logger.debug("Checking status of node {}".format(self.node_name))
        if self.__node_local:
            logger.debug("it will start on main envioronment")
            connection = self.config.connection
        else:
            logger.debug("it will start on an additional environment {}".format(str(self.__node_environment)))
            connection=make_nonprimary_connection(self.config.connection, self.__node_environment, self.__node_envuser)

        # use nodetool to check node status
        ret = execute_bash(connection, "check_cassandra", node_port=self.__node_jmx_port,  environment_vars=self.__env)  
        if ret.exit_code != 0:
            logger.debug("Node {} is down".format(self.node_name))
            return Status.INACTIVE

        for line in ret.stdout.split('\n'):
            if self.node_name in line:
                # this is nodestatus for node
                if line[0:2] == "UN":
                    logger.debug("Node {} is up".format(self.node_name))
                    return Status.ACTIVE
                else:
                    logger.debug("Node {} is down".format(self.node_name))
                    return Status.INACTIVE

        # node to found
        logger.debug("Node {} not found so reporting that it is down".format(self.node_name))
        return Status.INACTIVE
                    

    def load_schema(self):
        """
        Load schema from backup into a staging cluster
        """
        logger.debug("load schema started")
        
        # copy a schema file from backup into a local path 
        # it will use a backup driver depend find_schema function to locate a schema file
        # local file is local_schema_file
        local_schema_file = os.path.join(self.node_dir, "schema.cql")
        schema_s3_path = self.__backup_drv.find_schema()
        ret = execute_bash(self.config.connection, "copy_file_s3", src=schema_s3_path, dest=local_schema_file)
        if ret.exit_code != 0:
            raise UserError(
                'Failed to find metadata file',
                'Make sure the user has appropriate permissions and path is correct',
                '{}\n{}'.format(ret.stdout, ret.stderr))

        # run cqlsh to load schema file and ignore errors for already existing objects
        ret = execute_bash(self.config.connection, "cqlsh", file="-f {}".format(local_schema_file), host=self.node_name, environment_vars=self.__env)
        if ret.exit_code != 0:
            # OK - we need to parse stderr to get rid of Already exist errors and code=2100
            logger.debug("Parsing status of schema load - removing system keyspaces errors")
            withoutsystem = [ x for x in ret.stderr.split('\n') if "keyspace is not user-modifiable" not in x ] 
            logger.debug(withoutsystem)
            logger.debug("Parsing status of schema load - removing AlreadyExists errors")
            withoutexists = [ x for x in withoutsystem if "AlreadyExists" not in x ]
            logger.debug(withoutexists)
            if len(withoutexists) > 0:
                raise UserError(
                    'Failed to load schema',
                    'Make sure the user has appropriate permissions and path is correct',
                    '{}\n{}'.format(ret.stdout, ret.stderr))          
        else:
            # to we need to check anything here ?
            pass


    def run_cql(self, command):
        """
        run a cql command and return only list of rows and each row in json format
        command needs to have a have json formating otherwise this won't work
        TODO:
        add check if command has a json formating 
        """
        logger.debug("run_cql started executing command {}".format(command))
        ret = execute_bash(self.config.connection, "cqlcommand", command=command, host=self.node_name, environment_vars=self.__env)
        output = []
        if ret.exit_code == 0:
            # now in stdout we have not formated output
            for line in ret.stdout.split('\n'):
                try:
                    j = json.loads(line)
                    output.append(j)
                except ValueError:
                    # just skip not json lines
                    pass

        else:
            raise UserError(
                    'Failed to execute query cql',
                    'Make sure the user has appropriate permissions and path is correct',
                    '{}\n{}'.format(ret.stdout, ret.stderr))

        logger.debug("cql output is {}".format(output))
        return output


    def list_nonsystem_keyspaces(self):
        """
        Return a list of non system keyspaces
        """

        logger.debug("list_nonsystem_keyspaces started")
        kslist = self.run_cql("select json keyspace_name from system_schema.keyspaces")
        nonkslist = [ x["keyspace_name"] for x in kslist if "system" not in x["keyspace_name"] ]
        logger.debug("list of non system keyspaces {}".format(str(nonkslist)))
        return nonkslist


    def load_table(self, s3path, keyspace, tablename):
        """
        Load table from S3
        It will copy a part of sstable into /tmp and load into a staging cluster
        """

        local_dir = os.path.join("/tmp", keyspace, tablename)
        ret = execute_bash(self.config.connection, "delete_dir", path=local_dir)
        ret = execute_bash(self.config.connection, "create_dir", path=local_dir)

        s3dir = os.path.dirname(s3path)
        s3wild = os.path.basename(s3path)

        ret = execute_bash(self.config.connection, "sync_files_s3", src=s3dir, dest=local_dir, wildcard="{}*".format(s3wild))
        if ret.exit_code != 0:
            raise UserError(
                'Failed to copy table {}.{} from {}'.format(keyspace, tablename, s3path),
                'Make sure the user has appropriate permissions and path is correct',
                '{}\n{}'.format(ret.stdout, ret.stderr))

        ret = execute_bash(self.config.connection, "sstableloader", host=self.node_name, dir=local_dir, environment_vars=self.__env)
        if ret.exit_code != 0:
            raise UserError(
                'Failed to sstableload from {}'.format(local_dir),
                'Make sure the user has appropriate permissions and path is correct',
                '{}\n{}'.format(ret.stdout, ret.stderr))

        ret = execute_bash(self.config.connection, "delete_dir", path=local_dir)


    def compact_node(self, keyspace, table):
        """
        Compact keyspace / table on the node
        """
        ret = execute_bash(self.config.connection, "compact", node_port=self.__node_jmx_port, keyspace=keyspace, table=table, environment_vars=self.__env)  
        # should we rasie an error if compact will fail ? 

    def flush_node(self):
        """
        Flush node
        """

        logger.debug("flush_node started")
        if self.__node_local:
            logger.debug("it will start on main envioronment")
            connection = self.config.connection
        else:
            logger.debug("it will start on an additional environment {}".format(str(self.__node_environment)))
            connection=make_nonprimary_connection(self.config.connection, self.__node_environment, self.__node_envuser)

        ret = execute_bash(connection, "flush", node_port=self.__node_jmx_port,  environment_vars=self.__env)  
        if ret.exit_code != 0:
            raise UserError(
                'Failed to flush node {}'.format(self.node_name),
                'Make sure the user has appropriate permissions and path is correct',
                '{}\n{}'.format(ret.stdout, ret.stderr))


    def get_token_list(self):
        """
        Generate and return a list of tokens from node
        as dict ( "nodeName": xxx, "tokens": comma separated token list )
        """
        ret = execute_bash(self.config.connection, "ring", node_port=self.__node_jmx_port,  environment_vars=self.__env)  
        if ret.exit_code != 0:
            raise UserError(
                'Failed to get initial tokens from node {}'.format(self.node_name),
                'Make sure the user has appropriate permissions and path is correct',
                '{}\n{}'.format(ret.stdout, ret.stderr))

        token_list = []

        for line in ret.stdout.split("\n"):
            r = re.compile(r'^{}.* ([-]?\d+)'.format(self.node_name))
            token = re.match(r, line)
            if token:
                logger.debug("adding token to list {}".format(token.group(1)))
                token_list.append(token.group(1))

        return {"nodeName": self.node_name, "tokens":",".join(token_list)}

    def read_loaded_manifest(self):
        loaded_meta = '{}/.delphix/loaded.json'.format(self.mount_path)
        ret = execute_bash(self.config.connection, "read_file", file=loaded_meta)
        if ret.exit_code != 0:
            return None
        else:
            return ret.stdout

    def delete_loaded_manifest(self):
        loaded_meta = '{}/.delphix/loaded.json'.format(self.mount_path)
        loaded_meta_backup = '{}/.delphix/loaded.json.backup'.format(self.mount_path)
        ret = execute_bash(self.config.connection, "copy_files", source_path=loaded_meta, target_path=loaded_meta_backup)
        ret = execute_bash(self.config.connection, "delete_dir", path=loaded_meta)
        if ret.exit_code != 0:
            return None
        else:
            return ret.stdout


    def save_loaded_manifest(self, obj):
        loaded_meta = '{}/.delphix/loaded.json'.format(self.mount_path)
        loaded_meta_backup = '{}/.delphix/loaded.json.backup'.format(self.mount_path)
        newobj = obj.replace("\"", "\\\"")
        ret = execute_bash(self.config.connection, "copy_files", source_path=loaded_meta, target_path=loaded_meta_backup)
        ret = execute_bash(self.config.connection, "create_file", file=loaded_meta, content=newobj)
        return ret.exit_code


    def read_last_backup(self):
        loaded_backup = '{}/.delphix/last_backup'.format(self.mount_path)
        ret = execute_bash(self.config.connection, "read_file", file=loaded_backup)
        if ret.exit_code != 0:
            return None
        else:
            return ret.stdout

    def save_last_backup(self, obj):
        loaded_backup = '{}/.delphix/last_backup'.format(self.mount_path)
        loaded_backup_backup = '{}/.delphix/last_backup.backup'.format(self.mount_path)
        newobj = obj.replace("\"", "\\\"")
        ret = execute_bash(self.config.connection, "copy_files", source_path=loaded_backup, target_path=loaded_backup_backup)
        ret = execute_bash(self.config.connection, "create_file", file=loaded_backup, content=newobj)
        return ret.exit_code

    # list of backup vendor specific methods 
    # all those needs to be implemented in backup_drv class

    def load_meta(self, path, nonkslist):
        return self.__backup_drv.load_meta(path, nonkslist)

    def find_manifest(self):
        return self.__backup_drv.find_manifest()

    def get_initial_token(self):
        return self.__backup_drv.get_initial_token()

    def list_backups(self):
        return self.__backup_drv.list_backups()