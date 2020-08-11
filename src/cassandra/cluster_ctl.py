#
# Copyright (c) 2020 by Delphix. All rights reserved.
#


import logging
import os
import re
import json

from cassandra.node_ctl import Node_ctl
from cassandra.clustermeta import Cluster_meta
from dlpx.virtualization.platform import Status
from dlpx.virtualization.platform.exceptions import PluginRuntimeError
from dlpx.virtualization.platform.exceptions import UserError
from controller.helper import decode_dict

logger = logging.getLogger(__name__)

class Cluster_ctl(object):
    """
    Class for managing Cassandra clusters
    It has methods for cluster wide operations like start, stop, create or restore
    """


    def __init__(self, **kargs):
        """
        Class constructor
        Depends on workflow it's initialized with the following arguments
        For dSource:
            - staged_source
            - repository 
            - source_config or snapshot
        For VDB:
            - virtual_source
            - repository 
            - source_config or snapshot
        """

        self.__config = Cluster_meta(**kargs)

        # list of nodes in cluster - ordered by node name 
        self.__nodelist = []
        
        if self.__config.dSource:
            # for staging host - nodes are generated automatically, based on no of nodes provided
            for i in range(1, self.__config.staged_source.parameters.no_of_nodes+1):
                self.__nodelist.append({"nodeName": "127.0.0.{}".format(i), "port":"9042"})
            logger.debug("List of configured nodes {}".format(str(self.__nodelist)))
            # initial backup name used for initial ingestion or rsync
            # it's validated in check_backup
            self.__config.backup_name = self.__config.staged_source.parameters.full_backup_name
        else:
            # for VDB list of nodes is genereted from user input
            self.__nodelist = self.__config.virtual_source.parameters.node_list
            self.__nodelist.append({"nodeName": self.__config.virtual_source.parameters.node_name, "port":self.__config.virtual_source.parameters.port})
            

            # read no of nodes from snapshot if creating to set virtual sourceconfig
            # or from source config then

            if self.__config.snapshot is not None:
                dSource_nodes_no = len(self.__config.snapshot.init_tokens)
            else:
                # this is cover a unconfigure workflow
                dSource_nodes_no = self.__config.source_config.cluster_size


            if len(self.__nodelist) != dSource_nodes_no:
                raise UserError(message="Critical error - number of VDB nodes {} different from staging nodes {}".format(len(self.__nodelist), dSource_nodes_no), 
                                action="Please provision VDB with {} nodes".format(dSource_nodes_no))

            self.__nodelist = sorted(self.__nodelist, key=lambda x: x["nodeName"])
            logger.debug("List of configured nodes {}".format(str(self.__nodelist)))



        self.__config.node_list = self.__nodelist

        if self.__config.repository is not None:
            self.__cassandra_home = self.__config.repository.cassandra_home



    def create_cluster(self):
        """
        Create a staging cluster
        This method is called during an initial load
        or resync
        """

        i = 1

        for node in self.__nodelist:
            node_obj = Node_ctl(node_conf = node,
                                config = self.__config,
                                node_no=i)

            # TODO:
            # do we need to clean up files ? 
            # what if someone will add a node ? 
            node_obj.create_node()
            i=i+1


    def delete_cluster_data(self):
        """
        Delete cluster data from share
        This method is called during resync
        """

        i = 1

        for node in self.__nodelist:
            node_obj = Node_ctl(node_conf = node,
                                config = self.__config,
                                node_no=i)
            node_obj.delete_node()
            i=i+1


    def create_vdb_cluster(self):
        """
        Create a VDB cluster from source (dSource or other VDB)
        """

        i = 1

        for node in self.__nodelist:
            node_obj = Node_ctl(node_conf = node,
                                config = self.__config,
                                node_no=i)
            node_obj.create_vdb_node()
            i=i+1



        # TODO 
        # check of any parent node is on peers and stop and remove peers again

        parent_nodes = [x["nodeName"] for x in self.__config.snapshot.init_tokens]
        logger.debug("parent nodes names {}".format(str(parent_nodes)))

        fix_peers = False

        for node in self.__nodelist:
            node_obj = Node_ctl(node_conf = node,
                                config = self.__config,
                                node_no=i)

            peers = node_obj.run_cql("select json peer from system.peers;")
            set_peers = set( [ x["peer"] for x in peers ] )
            set_parents = set(parent_nodes)

            if set_parents.intersection(set_peers):
                fix_peers = True

            logger.debug("peers nodes set {}".format(str(set_peers)))
            logger.debug("parent nodes set {}".format(str(set_parents)))

        if fix_peers:
            self.stop_cluster()
            i = 1
            for node in self.__nodelist:
                node_obj = Node_ctl(node_conf = node,
                                    config = self.__config,
                                    node_no=i)       
                node_obj.fix_peers()
                i=i+1
            self.start_cluster()

    def is_cluster_exist(self):
        """
        Check if cluster exists ( all directories on share )
        """

        i = 1
        for node in self.__nodelist:
            node_obj = Node_ctl(node_conf = node,
                                config = self.__config,
                                node_no = i)
            if not node_obj.is_node_exists():
                # at least one node doesn't exists
                return False
            i=i+1

        # all directories exists
        return True


    def start_cluster(self):
        """
        Start all nodes from cluster
        """
        logger.debug("Start cluster")
        i = 1
        for node in self.__nodelist:
            node_obj = Node_ctl(node_conf = node,
                                config = self.__config,
                                node_no = i)
            node_obj.check_commit_log()
            node_obj.start_node()
            i=i+1


    def stop_cluster(self):
        """
        Stop all nodes in cluster
        """
        logger.debug("Stop cluster")
        i = 1
        for node in self.__nodelist:
            node_obj = Node_ctl(node_conf = node,
                                config = self.__config,
                                node_no = i)

            if node_obj.node_status() == Status.ACTIVE:                    
                node_obj.flush_node()
            node_obj.stop_node()
            i=i+1


    def flush_cluster(self):
        """
        Flush all nodes in cluster
        """
        logger.debug("Flush cluster")
        i = 1
        for node in self.__nodelist:
            node_obj = Node_ctl(node_conf = node,
                                config = self.__config,
                                node_no = i)
            i=i+1
            if node_obj.node_status() == Status.ACTIVE:                    
                node_obj.flush_node()




    def cluster_status(self):
        """
        Check status of all nodes in cluster
        """
        logger.debug("cluster status")
        i = 1

        status = Status.ACTIVE
        for node in self.__nodelist:
            node_obj = Node_ctl(node_conf = node,
                                config = self.__config,
                                node_no = i)
            if node_obj.node_status() == Status.INACTIVE:
                logger.debug("Node {} is down".format(node_obj.node_name))
                status = Status.INACTIVE
            i=i+1

        return status

    def load_backups(self):
        """
        Load a list of backups from S3 or other provider
        Backup list has to be an ordered list of tuples ( backupname, backup time, differential)
        """
        logger.debug("Load backups")

        #take a node of cluster to execute an action
        first_node = self.__nodelist[0]
        node_obj = Node_ctl(node_conf = first_node,
                            config = self.__config,
                            node_no = 1)
        self.s3backups = node_obj.list_backups()


    def check_backup(self):
        """
        Check if backup can be used during resync
        For resync - only full backups are allowed and it can be a last full backup
        or backup specified by user ( may require a force option if older than last loaded)
        Last loaded backup is keep in /mount_point/.delphix/last_backup
        """
        logger.debug("Check backup")

        #take a node of cluster to execute an action
        first_node = self.__nodelist[0]
        node_obj = Node_ctl(node_conf = first_node,
                            config = self.__config,
                            node_no = 1)

        force_backup = self.__config.staged_source.parameters.force_backup
        use_last_full = self.__config.staged_source.parameters.use_last_full

        if force_backup and use_last_full:
            raise UserError("Parameter force_backup and use last backup are mutally exclusive")

        last_backup = node_obj.read_last_backup()
        # if there is no file - we just loading a backup provided from GUI
        if last_backup:
            logger.debug("Last backup used by staging {}".format(last_backup))  
            last_backup_index = map(lambda x: x[0], self.s3backups).index(last_backup) + 1
            newer_backups = self.s3backups[last_backup_index:]
            # now filter only full
            newer_full = [ i for i in newer_backups if i[2]==False]
            logger.debug("List of newer full backups {}".format(newer_full)) 

            if use_last_full:
                # set last full backup as backup name
                if len(newer_full) > 0:
                    self.__config.backup_name = newer_full[-1][0]
                else:
                    raise UserError("No newer full backups")
            else:
                # check if backup name provided by user exist
                if not [ x for x in self.s3backups if x[0]==self.__config.backup_name]:
                    raise UserError("Backup {} not found".format(self.__config.backup_name))

                # check if backup name provided by user is newer or force flag is set to true
                older_backup = self.s3backups[:last_backup_index]
                logger.debug("older backups {}".format(older_backup))
                # now if list is not empty we are taking a last one     
                if [ x for x in older_backup if x[0]==self.__config.backup_name]:
                    if not force_backup:
                        raise UserError("Backup specified in GUI is older than last loaded. If you want to load it enable force")


        logger.info("Backup to restore {}".format(self.__config.backup_name))


    def restore_cluster(self, full):
        """
        Restore cluster using sstableloader
        Depend on parameter full: it restore will use a full backup (initial load / resync)
        or only an incremental backup
        """
        logger.debug("Restore cluster")
        #take a node of cluster to execute an action
        first_node = self.__nodelist[0]
        node_obj = Node_ctl(node_conf = first_node,
                            config = self.__config,
                            node_no = 1)

        if not full:
            # find a last incremental backup loaded from /mount_point/.delphix/last_backup
            # anc check if there is a newer one loaded from backup provider 

            last_backup = node_obj.read_last_backup()
            if last_backup:
                logger.debug("Last backup {}".format(last_backup))  
                last_backup_index = map(lambda x: x[0], self.s3backups).index(last_backup) + 1  
                newer_backups = self.s3backups[last_backup_index:]
                # now filter by differential
                newer_diff = [ i for i in newer_backups if i[2]]
                logger.debug("List of newer diff backups {}".format(newer_diff)) 
                # now if list is not empty we are taking a last one     
                if len(newer_diff) > 0:
                    self.__config.backup_name = newer_diff[-1][0]
                else:
                    raise UserError("There is no new backups")

        logger.debug("Backup to restore {}".format(self.__config.backup_name))
        logger.debug("Schema name to find in backup {}".format(self.__config.staged_source.parameters.schema_file_name))
        node_obj.load_schema()
        nonsysks = node_obj.list_nonsystem_keyspaces()

        manifest_files = node_obj.find_manifest()

        # this load_list contains a list of all manifests from all nodes 
        # from the particular backup
        load_list = []
        for manifest_file in manifest_files:
            load_list = load_list + node_obj.load_meta(manifest_file, nonsysks)

        # find all tables to load   

        logger.debug("All manifests are loaded")
        #logger.debug("{}".format(str(load_list)))

        # from list of keyspaces/tables find unique pairs ks/table
        pair_to_load = list(set([ (x["keyspace"], x["table"]) for x in load_list ] ))
        logger.debug("Unique list of keyspace / table to load {}".format(str(pair_to_load)))

        # next processing will find ks/table in all manifests, 
        # download it into /tmp
        # check if suitable for load
        #     load it using sstableloader
        # or move to next one
        # delete from /tmp
        # and compact a ks/table

        # read all already loaded sstables from /mount_point/.delphix/loaded.json
        loaded_manifest = node_obj.read_loaded_manifest()

        # build a dict of lists based on md5, sstable name and node
        if loaded_manifest is not None:
            md5dict = json.loads(loaded_manifest)
        else:
            md5dict = {}

        # set to true if any new table is loaded ( this will force to save a new list of loaded tables )
        newentry = False


        for (ks, table) in pair_to_load:
            # for each pair find SStable path from all manifests to process by ks and table name
            tables_to_process = [ x for x in load_list if (ks == x["keyspace"]) and (table == x["table"]) ]

            compact_table = False
            
            # for each table in list of paths - download and load using sstableloader
            for fileset in tables_to_process:

                logger.debug("tables to restore: {} {} {} {}".format(fileset["keyspace"], fileset["table"], fileset["size"], fileset["files"]))

                # now we assume we can copy one sstable file into /tmp

                if fileset["size"] > 0:
                    for sstablepath in fileset["files"]:
                        if sstablepath["md5"] in md5dict:
                            # file is potentially loaded maybe we can skip
                            currentlist = md5dict[sstablepath["md5"]]
                            currentfile = { "filename": os.path.basename(sstablepath["path"]), "table": table, "keyspace": ks, "nodename": fileset["nodename"]}
                            if currentfile in currentlist:
                                logger.debug("Skip table load - table {}  keyspace {} path {} md5 {}".format(
                                             table, ks, sstablepath["path"], sstablepath["md5"]))
                                continue

                            logger.debug("Ading table to loaded - table {}  keyspace {} path {} md5 {}".format(
                                         table, ks, sstablepath["path"], sstablepath["md5"]))
                            currentlist.append({ "filename": os.path.basename(sstablepath["path"]), "table": table, "keyspace": ks, "nodename": fileset["nodename"] })  
                            newentry = True
                            compact_table = True
                        else:
                            # new file - add to dictionary
                            md5dict[sstablepath["md5"]] = [ { "filename": os.path.basename(sstablepath["path"]), "table": table, "keyspace": ks, "nodename": fileset["nodename"] } ]
                            newentry = True
                            compact_table = True
                            logger.debug("Ading table to loaded - table {}  keyspace {} path {} md5 {}".format(
                                         table, ks, sstablepath["path"], sstablepath["md5"]))    

                        # load a table using sstableloader
                        node_obj.load_table(sstablepath["path"], fileset["keyspace"], fileset["table"])

            if compact_table:
                # new sstable was loaded from any of source nodes - we need to compact a staging
                logger.debug("Start compacatation if {} {}".format(ks, table))
                i = 1
                for node in self.__nodelist:
                    node_obj = Node_ctl(node_conf = node,
                                        config = self.__config,
                                        node_no = i)
                    
                    t = re.match(r'^(\S*)-.*',table)
                    node_obj.compact_node(ks, t.group(1))
                    i=i+1

        # new data loaded - save a new copy of loaded tables
        if newentry:
            node_obj.save_loaded_manifest(json.dumps(md5dict))
            
        logger.debug("Start flush")
        i = 1
        for node in self.__nodelist:
            node_obj = Node_ctl(node_conf = node,
                                config = self.__config,
                                node_no = i)
            node_obj.flush_node()
            i=i+1

        node_obj.save_last_backup(self.__config.backup_name)


    def get_init_tokens(self):
        """
        Build a cluster wide list of initial_tokens
        return all tokens as list of dicts {nodeName:list}
        """

        logger.debug("Start get_init token")
        i = 1

        token_list = []

        for node in self.__nodelist:
            node_obj = Node_ctl(node_conf = node,
                                config = self.__config,
                                node_no = i)
            tokens = node_obj.get_token_list()
            logger.debug("got list of tokens for node {} : {}".format(node["nodeName"], tokens))
            token_list.append(tokens)
            i=i+1

        return token_list


