#
# Copyright (c) 2020 by Delphix. All rights reserved.
#



class ReadOnlyDict(dict):
    def __readonly__(self, *args, **kwargs):
        raise RuntimeError("This information/dictionary is used across toolkit. Can't allow to update ")
    __setitem__ = __readonly__
    __delitem__ = __readonly__


class CommandHandler(object):

    def __init__(self):
        self.__commands = {
            'check_cassandra_home' : "echo $CASSANDRA_HOME",
            'copy_files' : "cp -rp {source_path} {target_path}",
            'create_dir' : "mkdir -p {path}",
            'delete_dir' : "rm -rf {path}",
            'check_dir'   : "ls -d {path}",
            'get_version' : "{install_path} --version",
            "create_file" : "echo \"{content}\" > {file}",
            "read_file": "cat {file}",
            "read_header": "head -c {bytes} {file}",
            "start_cassandra": "nohup {node_location}/bin/cassandra -p {pid_file} > {node_location}/nohup.out",
            "stop_cassandra": "nodetool -h 127.0.0.1 -p {node_port} stopdaemon",
            "check_process": "env > /tmp/dupa", 
            "check_cassandra": "nodetool -h 127.0.0.1 -p {node_port} status",
            "copy_file_s3": "aws s3 cp {src} {dest}",
            "sync_files_s3": "aws s3 sync {src} {dest} --exclude \'*\' --include \'{wildcard}\'",
            "find_s3_file": "aws s3 sync --dryrun --exclude \'*\' --include \'{file}\' {bucket} {local}",
            "sed": "sed -ibak -e s/{find}/{replace}/ {file}",
            "cqlsh": "cqlsh {file} {host}",
            "cqlcommand": "cqlsh --no-color -e \"{command}\" {host}",
            "sstableloader": "sstableloader -d {host} {dir}",
            "compact": "nodetool -h 127.0.0.1 -p {node_port} compact {keyspace} {table}",
            "flush": "nodetool -h 127.0.0.1 -p {node_port} flush",
            "ring": "nodetool -h 127.0.0.1 -p {node_port} ring",
            "iplist": "hostname -I"
        }
        self.__commands = ReadOnlyDict(self.__commands)

    @property
    def commands(self):
        return self.__commands
