import argparse
from pathlib import Path
from yaml import safe_load, safe_dump
from run_io import run_io_test
from run_rpc import run_rpc_test

class benchmark_suite_runner:
    def __init__(self, benchmarks, config: dict):
        self.io_tester_path: Path = Path(config['io_tester_path']).expanduser().resolve()
        self.rpc_tester_path: Path = Path(config['rpc_tester_path']).expanduser().resolve()
        self.output_dir: Path = Path(config['output_dir']).resolve()
        self.storage_dir: Path = Path(config['storage_dir']).resolve()
        self.ip_address = config['ip_address']
        self.io_asymmetric_cpuset = config['io_asymmetric_cpuset']
        self.io_symmetric_cpuset = config['io_symmetric_cpuset']
        self.rpc_asymmetric_server_cpuset = config['rpc_asymmetric_server_cpuset']
        self.rpc_symmetric_server_cpuset = config['rpc_symmetric_server_cpuset']
        self.rpc_asymmetric_client_cpuset = config['rpc_asymmetric_client_cpuset'] 
        self.rpc_symmetric_client_cpuset = config['rpc_symmetric_client_cpuset'] 

        self.benchmarks = benchmarks

    def run(self):
        for benchmark in self.benchmarks:
            test_name = benchmark['name']

            output_dir: Path = self.output_dir / test_name

            output_dir.mkdir(exist_ok=True, parents=True)

            config_path = output_dir / "conf.yaml"

            with open(config_path, "w") as f:
                print(safe_dump(benchmark['config']), file=f)

            print(f"Running benchmark {test_name}")
            match benchmark['type']:
                case "io":
                    run_io_test(self.io_tester_path, config_path, output_dir, self.storage_dir, self.io_asymmetric_cpuset, self.io_symmetric_cpuset)
                case "rpc":
                    run_rpc_test(self.rpc_tester_path, config_path, output_dir, self.ip_address, self.rpc_asymmetric_client_cpuset, self.rpc_symmetric_server_cpuset, self.rpc_asymmetric_client_cpuset, self.rpc_symmetric_client_cpuset)
                case _:
                    raise Exception(f"Unknown benchmark type {benchmark['type']}")

def run_benchmark_suite_args(args):
    benchmark_path = Path(args.benchmark).resolve()
    with open(benchmark_path, "r") as f:
        benchmark_yaml = f.read()

    config_path = Path(args.config).resolve()
    with open(config_path, "r") as f:
        config_yaml = f.read()

    runner = benchmark_suite_runner(
        safe_load(benchmark_yaml),
        safe_load(config_yaml)
    )

    runner.run()

def configure_run_benchmark_suite_parser(parser: argparse.ArgumentParser):
    parser.add_argument("--benchmark", help="path to .yaml file with the benchmark suite", required=True)
    parser.add_argument("--config", help="path to .yaml file with configuration for the test suite", required=True)
    parser.set_defaults(func=run_benchmark_suite_args)