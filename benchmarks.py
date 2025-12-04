import argparse
from pathlib import Path
from yaml import safe_load, safe_dump
from run_io import run_io_test
from run_rpc import run_rpc_test

class benchmark_suite_runner:
    def __init__(self, benchmarks, io_tester_path: Path, rpc_tester_path: Path, output_dir: Path, storage_dir: Path, ip_address: str, server_cpuset: str, client_cpuset: str, io_asymmetric_cpuset: str, io_symmetric_cpuset):
        self.io_tester_path: Path = io_tester_path.expanduser().resolve()
        self.rpc_tester_path: Path = rpc_tester_path.expanduser().resolve()
        self.output_dir: Path = output_dir.resolve()
        self.storage_dir: Path = storage_dir.resolve()
        self.ip_address = ip_address
        self.server_cpuset = server_cpuset
        self.client_cpuset = client_cpuset
        self.io_asymmetric_cpuset = io_asymmetric_cpuset
        self.io_symmetric_cpuset = io_symmetric_cpuset

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
                    run_rpc_test(self.rpc_tester_path, config_path, output_dir, self.ip_address, self.server_cpuset, self.client_cpuset)
                case _:
                    raise Exception(f"Unknown benchmark type {benchmark['type']}")

def run_benchmark_suite_args(args):
    benchmark_path = Path(args.benchmark).resolve()
    with open(benchmark_path, "r") as f:
        benchmark_yaml = f.read()

    config_path = Path(args.config).resolve()
    with open(config_path, "r") as f:
        config_yaml = f.read()

    config = safe_load(config_yaml)
    runner = benchmark_suite_runner(
        safe_load(benchmark_yaml),
        Path(config['io_tester_path']),
        Path(config['rpc_tester_path']),
        Path(config['output_dir']),
        Path(config['storage_dir']),
        config['ip_address'],
        config['server_cpuset'],
        config['client_cpuset']
    )

    runner.run()

def configure_run_benchmark_suite_parser(parser: argparse.ArgumentParser):
    parser.add_argument("--benchmark", help="path to .yaml file with the benchmark suite", required=True)
    parser.add_argument("--config", help="path to .yaml file with configuration for the test suite", required=True)
    parser.set_defaults(func=run_benchmark_suite_args)