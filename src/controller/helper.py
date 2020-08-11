#
# Copyright (c) 2020 by Delphix. All rights reserved.
#

import logging
import re

from dlpx.virtualization import libs
from dlpx.virtualization.libs import exceptions
from cassandra.os_commands import CommandHandler
from controller.os_command_response import OS_Command_Response
from dlpx.virtualization.common import RemoteEnvironment
from dlpx.virtualization.common import RemoteHost
from dlpx.virtualization.common import RemoteUser
from dlpx.virtualization.common import RemoteConnection
from dlpx.virtualization.libs import PlatformHandler

# logger object
logger = logging.getLogger(__name__)
command_list = CommandHandler()


def setup_logger():

    log_message_format = '[%(asctime)s] [%(levelname)s] [%(filename)s:%(lineno)d] %(message)s'
    log_message_date_format = '%Y-%m-%d %H:%M:%S'

    # Create a custom formatter. This will help in diagnose the problem.
    formatter = logging.Formatter(log_message_format, datefmt=log_message_date_format)

    platform_handler = libs.PlatformHandler()
    platform_handler.setFormatter(formatter)

    logger = logging.getLogger()
    logger.addHandler(platform_handler)

    # The root logger's default level is logging.WARNING.
    # Without the line below, logging statements of levels
    # lower than logging.WARNING will be suppressed.
    logger.setLevel(logging.DEBUG)



def find_binary_path(source_connection):
    logger.debug("Finding Binary Path...")
    ret = execute_bash(source_connection, command_name='check_cassandra_home')
    if ret.stdout == "":
        logger.warn("Please check environment variable CASSANDRA_HOME is defined. Checking in default location")
        binary_paths = ""
    else:
        binary_paths = ret.stdout
        logger.debug("List of couchbase path found are {}".format(binary_paths.split(';')))
    logger.debug("Finding Binary: {}".format(binary_paths))
    return binary_paths

def command_builder(command_name, **kwargs):
    """

    :param command_name: It specify the command name to search from cbase_command.py
    :param kwargs: required parameters for this command
    :return:  command with parameters
    """
    return command_list.commands[command_name].format(**kwargs)


def make_nonprimary_connection(primary_connection, secondary_env_ref, secondary_user_ref):
    dummy_host = primary_connection.environment.host
    user = RemoteUser(name="unused", reference=secondary_user_ref)
    environment = RemoteEnvironment(name="unused", reference=secondary_env_ref, host=dummy_host)
    return RemoteConnection(environment=environment, user=user)


def execute_bash(source_connection, command_name, **kwargs ):
    """
    :param source_connection: Connection object for the source environment
    :param command_name: Command to be search from dictionary of bash command
    :param kwargs: Dictionary to hold key-value pair for this command
    :return: list of stdout, stderr, exit code
    """

    if type(kwargs) != dict :
        raise exceptions.PluginScriptError("Parameters should be type of dictionary")

    if(source_connection is None):
        raise exceptions.PluginScriptError("Connection object cannot be empty")

    command = command_builder(command_name, **kwargs)
    logger.debug("Executing command is {}".format(command))

    # Putting if block because in some cases, environment_vars is not defined in kwargs then we need to pass empty
    # dict. Otherwise it will raise Exception.
    if 'environment_vars' in kwargs.keys():
        environment_vars = kwargs['environment_vars']
        if type(environment_vars) != dict:
            raise exceptions.PluginScriptError("environment_vars should be type of dictionary. Current type is{}".format(type(environment_vars)))
    else:
        #making empty environment_variable for this command
        environment_vars = {}
    result = libs.run_bash(source_connection, command=command, variables=environment_vars, use_login_shell=True)

    return OS_Command_Response(result.stdout, result.stderr, result.exit_code)



def decode_list(data):
    rv = []
    for item in data:
        if isinstance(item, unicode):
            item = item.encode('utf-8')
        elif isinstance(item, list):
            item = decode_list(item)
        elif isinstance(item, dict):
            item = decode_dict(item)
        rv.append(item)
    return rv

def decode_dict(data):
    rv = {}
    for key, value in data.iteritems():
        if isinstance(key, unicode):
            key = key.encode('utf-8')
        if isinstance(value, unicode):
            value = value.encode('utf-8')
        elif isinstance(value, list):
            value = decode_list(value)
        elif isinstance(value, dict):
            value = decode_dict(value)
        rv[key] = value
    return rv