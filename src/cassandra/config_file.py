#
# Copyright (c) 2020 by Delphix. All rights reserved.
#

config_file = """cluster_name: {cluster_name}
seed_provider:
    - class_name: org.apache.cassandra.locator.SimpleSeedProvider
      parameters:
          - seeds: "{seed}"
storage_port: 7000
listen_address: {listen_address}
native_transport_port: {transport_port}
data_file_directories: 
 - {data_dir}
commitlog_directory: {log_dir}
saved_caches_directory: {saved_dir} 
hints_directory: {hint_dir}
commitlog_sync: batch
commitlog_sync_batch_window_in_ms: 2
#commitlog_sync_period_in_ms: 10000
commitlog_segment_size_in_mb: 32
partitioner: org.apache.cassandra.dht.Murmur3Partitioner
endpoint_snitch: SimpleSnitch
authenticator: AllowAllAuthenticator
authorizer: AllowAllAuthorizer
counter_cache_size_in_mb:
rpc_address: localhost
start_native_transport: true
role_manager: CassandraRoleManager
disk_failure_policy: stop
commit_failure_policy: stop
num_tokens: {num_token}
initial_token: {initial_token}
rpc_address: {listen_address}
rpc_port: 9160
"""