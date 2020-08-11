# cassandra-plugin

This plugin is supporting Cassandra databases with Delphix Data Platform.
Any cluster backed up using a supported backup vendor, can be restored
into staging server and then easily cloned into a non-production environment.

## Requirements:

### Backup
- Cassandra backup created by Medusa tool - https://github.com/thelastpickle/cassandra-medusa

### Staging server:
- aws cli installed and configured with S3 access
- Apache Cassandra binaries unzipped (untarred)
- CASSANDRA_HOME environment variable set to Apache Cassandra binaries

### Target servers:
- Apache Cassandra binaries unzipped (untarred)
- CASSANDRA_HOME environment variable set to Apache Cassandra binaries


## Limitations:

- Number of nodes in the staging cluster, is a number of nodes for each child VDB.

- Incremental restores are supported only without schema changes.
  If schema change, restore from full backup is required (resync operation) 