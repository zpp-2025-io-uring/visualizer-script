# Seastar Benchmark Visualizer

[![Test](https://github.com/zpp-2025-io-uring/visualizer-script/actions/workflows/test.yaml/badge.svg)](https://github.com/zpp-2025-io-uring/visualizer-script/actions/workflows/test.yaml)
[![Lint](https://github.com/zpp-2025-io-uring/visualizer-script/actions/workflows/lint.yaml/badge.svg)](https://github.com/zpp-2025-io-uring/visualizer-script/actions/workflows/lint.yaml)

Generate graphs and run benchmark suites for Seastar-based testers.

## Running

The main functionality is provided by the `suite` subcommand of the `main.py` script - running a benchmark suite based on provided configuration and benchmark files.
See the [Benchmark suite](#benchmark-suite) section for more details.

There are also other subcommands available:

- `redraw` - redraw charts for single run of some benchmark
- `redraw_suite` - redraw charts for some benchmark

### Help

Run

```bash
python3 main.py --help
```

to check the available subcommands

or

```bash
python3 main.py <subcommand> --help
```

to check the options for a specific subcommand.

## Usage

### suite

Run a benchmark suite. The benchmark suite mode requires config and benchmark files in YAML format. `--config` accepts one or more YAML files or a directory (all `*.yaml` will be used):

```bash
python3 ./main.py suite --benchmark configuration/suite.yaml --config configuration/configs --generate-graphs --generate-summary-graphs --pdf
```

You can also pass multiple specific config files (at least one):

```bash
python3 ./main.py suite --benchmark configuration/suite.yaml --config config_1.yaml config_2.yaml
```

### redraw

Redraw from explicit backend output files (provide any combination of backends):

```bash
python3 ./main.py redraw --io_uring results/<run_dir>/io_uring.out --epoll results/<run_dir>/epoll.out --output-dir generated/graphs
```

### redraw_suite

Redraw all benchmarks and runs found under a results directory (recursively finds per-benchmark runs):

```bash
python3 ./main.py redraw_suite --dir results/timestamp/config_name
```

### Flags exposed by subcommands
- `redraw` flags: `--asymmetric_io_uring`, `--io_uring`, `--linux-aio`, `--epoll`, `--output-dir` (required).
- `redraw_suite` flags: `--dir` (required).
- `suite` flags: `--benchmark` (required), `--config` (required, one or more), `--generate-graphs`, `--generate-summary-graphs`, `--pdf`, `--legacy-cores-per-worker`.

### Benchmark suite (`--benchmark`)

Consists of a list of the following elements:

```yaml
- type: # "io", "rpc" or "simple-query"
  name: # used for the result directory name
  iterations: # number of iterations to run, optional, default: 1, used for charts with stddev
  config:
  ... # Configuration to be passed to the tester
```

### Cpumask config (`--config`)

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

## Development

### Testing

To run the tests, use the following command:
> pytest

#### Parallel testing

You can use `pytest-xdist` to run tests in parallel. To do this, run:
> pytest -n *number_of_cores*
or
> pytest -n auto

### Linting

We're using `ruff` for linting and code formatting. To check the code for linting issues, run:

> ruff check .

To automatically format the code, run:

> ruff format .

Please refer to the [Ruff documentation](https://ruff.rs/docs/) for more details on configuration and usage.
