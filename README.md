# io_tester_visualizer

## Running

Run
```bash
./main --help
```
to check the required arguments

## Benchmark suite configuration
The benchmark suite mode requires two .yaml files:

### `--banchmark`

Consists of a list of the following elements:
```yaml
- type: # "io" or "rpc"
  name: # used for the result directory name
  config:
  ... # Configuration to be passed to the tester
```

### `--config`

Must contain the following elements:
```yaml
io_tester_path: ...
rpc_tester_path: ...
output_dir: ...
storage_dir: ...
ip_address: ...
io_asymmetric_cpuset: ...
io_symmetric_cpuset: ...
rpc_asymmetric_server_cpuset: ...
rpc_symmetric_server_cpuset: ...
rpc_asymmetric_client_cpuset: ...
rpc_symmetric_client_cpuset: ...
```