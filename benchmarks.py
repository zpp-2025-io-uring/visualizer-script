import argparse
from pathlib import Path
from datetime import datetime
import subprocess
from yaml import safe_load, safe_dump
from run_io import run_io_test
from run_rpc import run_rpc_test
from generate import generate_graphs
from parse import load_data, auto_generate_data_points, save_results_for_benchmark
from stats import join_stats, join_metrics

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
        self.backends = config['backends']

        self.benchmarks = benchmarks

    def run(self):
        for benchmark in self.benchmarks:
            test_name = benchmark['name']
            iterations= benchmark.get('iterations', 1)

            test_output_dir: Path = self.output_dir / test_name
            test_output_dir.mkdir(exist_ok=True, parents=True)

            config_path = test_output_dir / "conf.yaml"
            with open(config_path, "w") as f:
                print(safe_dump(benchmark['config']), file=f)

            metrics_runs = []
            for i in range(iterations):
                print(f"Running benchmark {test_name}, i={i}")

                run_output_dir: Path = test_output_dir / f"run_{i}"
                run_output_dir.mkdir(exist_ok=True, parents=True)

                result: dict = None    

                if benchmark['type'] == "io":
                    result = run_io_test(self.io_tester_path, config_path, run_output_dir, self.storage_dir, self.io_asymmetric_cpuset, self.io_symmetric_cpuset, self.backends)
                elif benchmark['type'] == "rpc":
                    result = run_rpc_test(self.rpc_tester_path, config_path, run_output_dir, self.ip_address, self.rpc_asymmetric_server_cpuset, self.rpc_symmetric_server_cpuset, self.rpc_asymmetric_client_cpuset, self.rpc_symmetric_client_cpuset, self.backends)
                else:
                    raise Exception(f"Unknown benchmark type {benchmark['type']}")

                backends_parsed = {}
                for backend, raw in result.items():
                    parsed = load_data(raw)
                    backends_parsed[backend] = auto_generate_data_points(parsed)

                [shardless_metrics, sharded_metrics] = join_metrics(backends_parsed)
                metrics_runs.append({'run_id': i, 'sharded': sharded_metrics, 'shardless': shardless_metrics})
                generate_graphs(sharded_metrics, shardless_metrics, run_output_dir)

            (combined_sharded, combined_shardless) = join_stats(metrics_runs)
            benchmark_info = {'id': test_name, 'properties': {'iterations': iterations}}
            save_results_for_benchmark(test_output_dir, combined_sharded, combined_shardless, benchmark_info)

def dump_environment(dir_for_config: Path, dir_to_seastar: Path):
    """
    Dumps environment information into files in dir_for_config.
    dir_to_seastar is the path to the seastar repository, used to get git log.

    Dumps:
    - lscpu output
    - lscpu -e output
    - hostname output
    - git log of seastar repo since 2025-12-01
    """

    lscpu = subprocess.run(
            ['lscpu'],
            capture_output=True,
            text=True,
    )

    with open(dir_for_config / 'lscpu.txt', 'w') as f:
        print(lscpu.stdout, file=f)

    if lscpu.returncode != 0:
        raise Exception("lscpu failed")
    
    lscpu_e = subprocess.run(
            ['lscpu', '-e'],
            capture_output=True,
            text=True,
    )

    with open(dir_for_config / 'lscpu_e.txt', 'w') as f:
        print(lscpu_e.stdout, file=f)

    if lscpu_e.returncode != 0:
        raise Exception("lscpu -e failed")
    
    hostname = subprocess.run(
            ['hostname'],
            capture_output=True,
            text=True,
    )

    with open(dir_for_config / 'hostname.txt', 'w') as f:
        print(hostname.stdout, file=f)

    if hostname.returncode != 0:
        raise Exception("hostname failed")
    
    git_log = subprocess.run(
            ['git', 'log', '--since="2025-12-01"'],
            cwd=Path(dir_to_seastar).expanduser().resolve(),
            capture_output=True,
            text=True,
    )

    with open(dir_for_config / 'git_log.txt', 'w') as f:
        print(git_log.stdout, file=f)

    if git_log.returncode != 0:
        raise Exception("git_log failed")

def run_benchmark_suite_args(args):
    benchmark_path = Path(args.benchmark).resolve()
    with open(benchmark_path, "r") as f:
        benchmark_yaml = f.read()

    config_path = Path(args.config).resolve()
    with open(config_path, "r") as f:
        config_yaml = f.read()

    config = safe_load(config_yaml)
    output_dir = Path(config['output_dir']).resolve()

    timestamped_output_dir: Path = output_dir / datetime.now().strftime("%d-%m-%Y_%H:%M:%S")
    timestamped_output_dir.mkdir(exist_ok=True, parents=True)
    config['output_dir'] = timestamped_output_dir

    with open(timestamped_output_dir / 'suite.yaml', 'w') as f:
        print(benchmark_yaml, end='', file=f)

    with open(timestamped_output_dir / 'config.yaml', 'w') as f:
        print(config_yaml, end='', file=f)

    dump_environment(timestamped_output_dir, Path(config['io_tester_path']).expanduser().resolve().parent)

    if 'backends' not in config:
        config['backends'] = ['asymmetric_io_uring', 'io_uring']
        print(f"Warning: backends selecton not detected, assuming {config['backends']}")

    runner = benchmark_suite_runner(
        safe_load(benchmark_yaml),
        config
    )

    runner.run()

def configure_run_benchmark_suite_parser(parser: argparse.ArgumentParser):
    parser.add_argument("--benchmark", help="path to .yaml file with the benchmark suite", required=True)
    parser.add_argument("--config", help="path to .yaml file with configuration for the test suite", required=True)
    parser.set_defaults(func=run_benchmark_suite_args)