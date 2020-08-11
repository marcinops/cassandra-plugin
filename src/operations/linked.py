#
# Copyright (c) 2020 by Delphix. All rights reserved.
#

import logging

from generated.definitions import SnapshotDefinition
from cassandra.cluster_ctl import Cluster_ctl
from controller.plugin_exceptions import exception_decorator

logger = logging.getLogger(__name__)

@exception_decorator
def resync(staged_source, repository, source_config):
    """
    Resync operation run for Resync API call (initial ingestion or force full backup)
    It will create a new cluster if doesn't exist or restore a full backup
    """
    logger.debug("In Re-sync...")
    clust_obj = Cluster_ctl(staged_source=staged_source,
                            repository = repository,
                            source_config = source_config)


    # load backup lists from backup vendor
    clust_obj.load_backups()

    if clust_obj.is_cluster_exist():
        # check backup - if no newer full backup stop rsync
        # if force is enabled - use backup specified in GUI

        clust_obj.check_backup()        
        clust_obj.stop_cluster()
        clust_obj.delete_cluster_data()
        clust_obj.start_cluster()
    else:
        clust_obj.create_cluster()
    
    # here we need to restore full backup
    clust_obj.restore_cluster(True)
        



def pre_snapshot(staged_source, repository, source_config, snapshot_parameters):
    """
    Presnapshot operation run for normal snapshots (see plugin_runner.py)
    It will restore a differential backup
    """
    logger.info("In Pre snapshot...")
    clust_obj = Cluster_ctl(staged_source=staged_source,
                            repository = repository,
                            source_config = source_config)
    
    # here we need to restore an incremental backup
    clust_obj.load_backups()
    clust_obj.restore_cluster(False)
    clust_obj.flush_cluster()

     

def post_snapshot(staged_source, repository, source_config):
    """
    Post_snapshot operation run after restore
    It will save a snapshot metadata for VDB's
    """
    logger.info("In Post snapshot...")
    clust_obj = Cluster_ctl(staged_source=staged_source,
                            repository = repository,
                            source_config = source_config)
    init_tokens = clust_obj.get_init_tokens()
    return SnapshotDefinition(node_list = [], node_mapping=[], init_tokens=init_tokens)


def staging_status(staged_source, repository, source_config):
    #TODO
    # add monitoring of staging database
    pass


def start_staging(staged_source, repository, source_config):
    """
    start_staging operation run on enable dSource
    """
    logger.debug("start_staging")
    clust_obj = Cluster_ctl(staged_source=staged_source,
                            repository = repository,
                            source_config = source_config)

    if clust_obj.is_cluster_exist():
        clust_obj.start_cluster()
    else:
        #to add exception
        logger.debug("start_staging without cluster configuration")


def stop_staging(staged_source, repository, source_config):
    """
    stop_staging operation run on disable or delete dSource
    """
    logger.debug("stop staging")
    clust_obj = Cluster_ctl(staged_source=staged_source,
                            repository = repository,
                            source_config = source_config)

    if clust_obj.is_cluster_exist():
        clust_obj.stop_cluster()
    else:
        #to add exception
        logger.debug("stop_staging without cluster configuration")