# io_tester_visualizer

## Running

Run

```bash
python3 ./main.py suite --benchmark configuration/suite.yaml --config configuration/configs
```
or
```bash
python3 ./main.py suite --benchmark configuration/suite.yaml --config config_1.yaml
```
or
```bash
python3 ./main.py suite --benchmark configuration/suite.yaml --config config_1.yaml config_2.yaml config_3.yaml
```

to check the required arguments

## Benchmark suite configuration

The benchmark suite mode requires two .yaml files:

### `--benchmark`

Consists of a list of the following elements:

```yaml
- type: # "io" or "rpc"
  name: # used for the result directory name
  iterations: # number of iterations to run, optional, default: 1
  config:
  ... # Configuration to be passed to the tester
```

### Configs
#### simple-query
```yaml
random-seed: 1             # Random number generator seed
partitions: 10000          # number of partitions
duration: 5                # test duration in seconds
concurrency: 100           # workers per core
operations-per-shard: #arg # run this many operations per shard (overrides 
                            #  duration)
initial-tablets:  128      # initial number of tablets
memtable-partitions: #arg  # apply this number of partitions to memtable, 
                            #  then flush
enable-cache: 1            # enable row cache
stop-on-error: 1           # stop after encountering the first error
timeout:  #arg             # use timeout
audit: #arg                # value for audit config entry
audit-keyspaces: #arg      # value for audit_keyspaces config entry
audit-tables: #arg         # value for audit_tables config entry
audit-categories: #arg     # value for audit_categories config entry
flags:
- write                      # test write path instead of read path
- delete                     # test delete path instead of read path
- query-single-key           # test reading with a single key instead of random keys
- counters                   # test counters
- tablets                    # use tablets
- flush                      # flush memtables before test
- bypass-cache               # use bypass cache when querying

```
where ommited keys are set to default values.
All values could be ommited, but this is not recommended for clarity.

### `--config`

Must contain the following elements:
```yaml
config_version: ...
output_dir: ...
params:
  skip_async_workers_cpuset: true/false
backends:
  - ...
  - ...
  ...
io:
  tester_path: ...
  storage_dir: ...
  asymmetric_app_cpuset: ...
  asymmetric_async_worker_cpuset: ...
  symmetric_cpuset: ...
rpc:
  tester_path: ...
  ip_address: ...
  asymmetric_server_app_cpuset: ...
  asymmetric_server_async_worker_cpuset: ...
  symmetric_server_cpuset: ...
  asymmetric_client_app_cpuset: ...
  asymmetric_client_async_worker_cpuset: ...
  symmetric_client_cpuset: ...
scylla:
  path: ... # Path to scylla executable
  asymmetric_app_cpuset: ...
  asymmetric_async_worker_cpuset: ...
  symmetric_cpuset: ...
```
