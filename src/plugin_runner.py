#
# Copyright (c) 2020 by Delphix. All rights reserved.
#

#!/usr/bin/env python
# -*- coding: utf-8 -*-

import traceback
import os
import logging

from dlpx.virtualization.platform import Mount, MountSpecification, Plugin
from dlpx.virtualization.common import RemoteEnvironment
from dlpx.virtualization.common import RemoteHost
from dlpx.virtualization.common import RemoteUser
from dlpx.virtualization.common import RemoteConnection
from dlpx.virtualization import libs
from operations import discovery
from operations import linked
from operations import virtual
from controller.helper import setup_logger
from dlpx.virtualization.platform.exceptions import UserError
from controller.plugin_exceptions import exception_decorator
from generated.definitions import SnapshotDefinition, SourceConfigDefinition


plugin = Plugin()
setup_logger()
logger = logging.getLogger(__name__)


@plugin.discovery.repository()
def repository_discovery(source_connection):
    return discovery.find_repos(source_connection)


@plugin.discovery.source_config()
def source_config_discovery(source_connection, repository):
    # Source config for dSource 
    # There is no automation here. Production cluster needs to be added manually 
    return []


@plugin.linked.post_snapshot()
def linked_post_snapshot(staged_source, repository, source_config, snapshot_parameters):
    return linked.post_snapshot(staged_source, repository, source_config)



@plugin.linked.pre_snapshot()
def linked_pre_snapshot(staged_source, repository, source_config, snapshot_parameters):
    if int(snapshot_parameters.resync) == 1:
        linked.resync(staged_source, repository, source_config)
    else:
        linked.pre_snapshot(staged_source, repository, source_config, snapshot_parameters)


@plugin.linked.mount_specification()
def linked_mount_specification(staged_source, repository):
    mount_path = "{}/{}".format(staged_source.parameters.staging_base_dir, staged_source.parameters.staging_cluster_name)
    environment = staged_source.staged_connection.environment
    mounts = [Mount(environment, mount_path)]

    return MountSpecification(mounts)


@plugin.linked.start_staging()
def start_staging(staged_source, repository, source_config):
    logger.debug("start staging {} {} {}".format(staged_source, repository, source_config))
    linked.start_staging(staged_source, repository, source_config)


@plugin.linked.stop_staging()
def stop_staging(staged_source, repository, source_config):
    logger.debug("stop staging {} {} {}".format(staged_source, repository, source_config))
    linked.stop_staging(staged_source, repository, source_config)


@plugin.virtual.post_snapshot()
def virtual_post_snapshot(virtual_source, repository, source_config):
    return virtual.post_snapshot(virtual_source, repository, source_config)

@plugin.virtual.pre_snapshot()
def virtual_pre_snapshot(virtual_source, repository, source_config):
    return virtual.pre_snapshot(virtual_source, repository, source_config)

@plugin.virtual.mount_specification()
def virtual_mount_specification(virtual_source, repository):
    
    # all checks needs to be done here 

    mount_location = os.path.join(virtual_source.parameters.mount_location,virtual_source.parameters.cluster_name)

    mountlist = [ Mount(virtual_source.connection.environment, mount_location) ]

    logger.debug("in mouinting: {}".format(str(virtual_source.parameters.node_list)))

    if len(virtual_source.parameters.node_list) > 0:
        # more nodes
        for m in virtual_source.parameters.node_list:
            logger.debug("in loop: {}".format(str(m)))
            node_host = RemoteHost(name='foo',
                                   reference=m["environment"].replace('_ENVIRONMENT', ''),
                                   binary_path="",
                                   scratch_path=""
                                   )
            e = RemoteEnvironment("foo", m["environment"], node_host )
            mount = Mount(e, mount_location)
            mountlist.append(mount)


    return MountSpecification(mountlist)


@plugin.virtual.configure()
def configure_new_vdb(virtual_source, snapshot, repository):
    logger.debug("in configure")
    return virtual.configure(virtual_source, snapshot, repository)


@plugin.virtual.start()
def start_vdb(virtual_source, repository, source_config):
    logger.debug("in start_vdb")
    return virtual.start_vdb(virtual_source, repository, source_config)


@plugin.virtual.stop()
def stop_vdb(virtual_source, repository, source_config):
    logger.debug("in stop_vdb")
    return virtual.stop_vdb(virtual_source, repository, source_config)


@plugin.virtual.status()
def virtual_status(virtual_source, repository, source_config):
    logger.debug("in virtual_status")
    return virtual.vdb_status(virtual_source, repository, source_config)


@plugin.virtual.reconfigure()
def reconfigure(virtual_source, repository, source_config, snapshot):
    # for enable / rollback
    return virtual.reconfigure(virtual_source, repository, source_config, snapshot)

@plugin.virtual.unconfigure()
def unconfigure(virtual_source, repository, source_config):
    # to stop before refres, delete and rollback
    virtual.unconfigure(virtual_source, repository, source_config)