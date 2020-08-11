#
# Copyright (c) 2020 by Delphix. All rights reserved.
#

import logging
import time

from generated.definitions import SnapshotDefinition, SourceConfigDefinition
from cassandra.cluster_ctl import Cluster_ctl
from dlpx.virtualization.platform import Status


logger = logging.getLogger(__name__)

"""
This file contains a list of plugin operations for VDB
"""



def configure(virtual_source, snapshot, repository):
    """
    Run for provision and refresh of the VDB
    """

    logger.debug("in virtual configure")

    clust_obj = Cluster_ctl(virtual_source=virtual_source,
                            snapshot = snapshot,
                            repository = repository)    
    clust_obj.create_vdb_cluster()
    s = SourceConfigDefinition(cluster_size=len(snapshot.init_tokens), name=virtual_source.parameters.cluster_name)
    logger.debug("New SourceConfig for VDB is created as follow: {}".format(str(s)))
    return s

def post_snapshot(virtual_source, repository, source_config):
    """
    Post_snapshot operation run after every VDB snapshot
    It will save a snapshot metadata
    """
    logger.info("In virtual Post snapshot...")
    clust_obj = Cluster_ctl(virtual_source=virtual_source,
                            source_config = source_config,
                            repository = repository)    

    init_tokens = clust_obj.get_init_tokens()

    logger.info(SnapshotDefinition(node_list = [], node_mapping=[], init_tokens=init_tokens))
    return SnapshotDefinition(node_list = [], node_mapping=[], init_tokens=init_tokens)


def start_vdb(virtual_source, repository, source_config):
    """
    start_vdb operation run on start and enable of the VDB
    """
    logger.debug("start_vdb")
    clust_obj = Cluster_ctl(virtual_source=virtual_source,
                            repository = repository,
                            source_config = source_config)

    if clust_obj.is_cluster_exist():
        clust_obj.start_cluster()
    else:
        #to add exception
        logger.debug("start_vdb without cluster configuration")


def vdb_status(virtual_source, repository, source_config):
    """
    vdb_status check a status of the VDB cluster
    """
    clust_obj = Cluster_ctl(virtual_source=virtual_source,
                        repository = repository,
                        source_config = source_config)
    return clust_obj.cluster_status()


def stop_vdb(virtual_source, repository, source_config):
    """
    stop_vdb operation run on stop, rewind, refresh and disable of the VDB
    """
    logger.debug("stop_vdb")
    clust_obj = Cluster_ctl(virtual_source=virtual_source,
                            repository = repository,
                            source_config = source_config)

    if clust_obj.is_cluster_exist():
        clust_obj.stop_cluster()
    else:
        #to add exception
        logger.debug("start_vdb without cluster configuration")


def reconfigure(virtual_source, repository, source_config, snapshot):
    """
    reconfigure operation run on rewind operation after unconfigure
    """
    logger.debug("in reconfigure")

    start_vdb(virtual_source, repository, source_config)
    s = SourceConfigDefinition(cluster_size=len(snapshot.init_tokens), name=virtual_source.parameters.cluster_name)
    logger.debug("-----------------------------------------------------------------")
    logger.debug(str(s))
    return s


def unconfigure(virtual_source, repository, source_config):
    """
    unconfigure operation run on rewind, delete and refresh operation
    """
    logger.debug("in unconfigure")
    stop_vdb(virtual_source, repository, source_config)

def pre_snapshot(virtual_source, repository, source_config):
    """
    pre_snapshot operation run before snapshot of VDB
    """
    logger.info("In virtual Pre snapshot...")
    # flush to help with commitlogs issue
    logger.debug(str(source_config))
    clust_obj = Cluster_ctl(virtual_source=virtual_source,
                            repository = repository,
                            source_config = source_config)
    clust_obj.flush_cluster()
    time.sleep(5)

