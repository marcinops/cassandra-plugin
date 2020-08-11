#
# Copyright (c) 2020 by Delphix. All rights reserved.
#

from generated.definitions import RepositoryDefinition, SourceConfigDefinition
from controller.helper import find_binary_path


def find_repos(source_connection):
    """
    Run a discovery on server to find a Cassandra binaries - ex. apache-cassandra-X.X.X.jar
    CASSANDRA_HOME variable is used for discovery - no full file system scan
    Returns:
        Object of RepositoryDefinition class
    """

    #TODO
    # Check version to avoid hard coding
    # 

    cassandra_home = find_binary_path(source_connection)
    repositories = []

    if cassandra_home != "":
        # cassandra home is defined
        name = "Cassnadra ({})".format("3.6")
        repository_definition = RepositoryDefinition(cassandra_home = cassandra_home, pretty_name=name)
        repositories.append(repository_definition)

    return repositories