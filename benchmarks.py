import argparse
from pathlib import Path
from datetime import datetime
import subprocess
from yaml import safe_load, safe_dump
from run_io import run_io_test
from run_rpc import run_rpc_test
from generate import generate_graphs, generate_graphs_for_summary
from parse import load_data, auto_generate_data_points
from benchmark import compute_benchmark_summary, benchmark
from stats import join_stats, join_metrics, stats
from config_versioning import get_config_version, upgrade_version1_to_version2, make_proportional_splitter
from pdf_summary import generate_benchmark_summary_pdf, merge_pdfs

class benchmark_suite_runner:
    def __init__(self, benchmarks, config: dict, generate_graphs: bool, generate_summary_graphs: bool, generate_pdf: bool):
        self.output_dir: Path = Path(config['output_dir']).resolve()
        self.io_config = config['io']
        self.rpc_config = config['rpc']
        self.backends = config['backends']
        self.params = config['params']

        self.benchmarks = benchmarks
        self.generate_graphs = generate_graphs
        self.generate_summary_graph = generate_summary_graphs
        self.generate_pdf = generate_pdf

    def run(self):
        per_benchmark_pdfs: list[Path] = []

        for benchmark in self.benchmarks:
            test_name = benchmark['name']
            iterations = benchmark.get('iterations', 1)

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
                    result = run_io_test(self.io_config, config_path, run_output_dir, self.backends, self.params['skip_async_workers_cpuset'])
                elif benchmark['type'] == "rpc":
                    result = run_rpc_test(self.rpc_config, config_path, run_output_dir, self.backends, self.params['skip_async_workers_cpuset'])
                else:
                    raise Exception(f"Unknown benchmark type {benchmark['type']}")

                backends_parsed = {}
                for backend, raw in result.items():
                    parsed = load_data(raw)
                    backends_parsed[backend] = auto_generate_data_points(parsed)

                [shardless_metrics, sharded_metrics] = join_metrics(backends_parsed)
                metrics_runs.append({'run_id': i, 'sharded': sharded_metrics, 'shardless': shardless_metrics})

                if self.generate_graphs:
                    generate_graphs(sharded_metrics, shardless_metrics, run_output_dir)

            (combined_sharded, combined_shardless) = join_stats(metrics_runs)
            benchmark_info = {'id': test_name, 'properties': {'iterations': iterations}}
            summary = compute_benchmark_summary(combined_sharded, combined_shardless, benchmark_info)

            if self.generate_summary_graph:
                generate_graphs_for_summary(summary.get_runs(), summary.get_stats(), test_output_dir, image_format="svg")

            if self.generate_pdf:
                generate_graphs_for_summary(summary.get_runs(), summary.get_stats(), test_output_dir, image_format="png")
                summary_images = sorted(test_output_dir.glob("auto_*.png"))
                pdf_path = generate_benchmark_summary_pdf(
                    benchmark_name=test_name,
                    images=summary_images,
                    output_pdf=test_output_dir / "summary.pdf",
                )
                per_benchmark_pdfs.append(pdf_path)

            dump_summary(test_output_dir, summary)

        if self.generate_pdf and per_benchmark_pdfs:
            merge_pdfs(input_pdfs=per_benchmark_pdfs, output_pdf=self.output_dir / "suite_summary.pdf")

BENCHMARK_SUMMARY_FILENAME = "metrics_summary.yaml"

def dump_summary(benchmark_output_dir: Path, summary: dict):
    """
    Dumps the benchmark summary into benchmark_output_dir/metrics_summary.yaml
    """
    benchmark_output_dir.mkdir(parents=True, exist_ok=True)
    with open(benchmark_output_dir / BENCHMARK_SUMMARY_FILENAME, 'w') as f:
        f.write(safe_dump(summary))

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
    timestamp_for_suite : str = datetime.now().strftime("%d-%m-%Y_%H:%M:%S")
    
    benchmark_path = Path(args.benchmark).resolve()
    with open(benchmark_path, "r") as f:
        benchmark_yaml = f.read()

    config_paths : list[Path] = []
    for config_arg in args.config:
        config_path = Path(config_arg).resolve()
        if config_path.is_dir():
            for config_file in config_path.glob('*.yaml'):
                config_paths.append(config_file)
        else:
            config_paths.append(config_path)
    
    for config_path in config_paths:
        with open(config_path, "r") as f:
            config_yaml = f.read()

        config = safe_load(config_yaml)

        match get_config_version(config):
            case 1:
                if "legacy_cores_per_worker" not in args:
                    raise RuntimeError("Missing legacy_cores_per_worker value")
                
                print(f"Warning: automatically calculating async worker cpused based on cores_per_worker value {args.legacy_cores_per_worker}")

                config = upgrade_version1_to_version2(config, make_proportional_splitter(int(args.legacy_cores_per_worker)))
            case 2:
                pass
            case other:
                raise ValueError(f"Unknown config version: {other}")

        output_dir = Path(config['output_dir']).resolve()

        timestamped_output_dir: Path = output_dir / timestamp_for_suite / config_path.stem
        timestamped_output_dir.mkdir(exist_ok=True, parents=True)

        with open(timestamped_output_dir / 'suite.yaml', 'w') as f:
            print(benchmark_yaml, end='', file=f)

        with open(timestamped_output_dir / f'config_{config_path.name}', 'w') as f:
            print(safe_dump(config), end='', file=f)

        config['output_dir'] = timestamped_output_dir
        
        dump_environment(timestamped_output_dir, Path(config['io']['tester_path']).expanduser().resolve().parent)

        if 'backends' not in config:
            config['backends'] = ['asymmetric_io_uring', 'io_uring']
            print(f"Warning: backends selecton not detected, assuming {config['backends']}")

        runner = benchmark_suite_runner(
            safe_load(benchmark_yaml),
            config,
            args.generate_graphs,
            args.generate_summary_graphs,
            args.pdf
        )

        runner.run()

def configure_run_benchmark_suite_parser(parser: argparse.ArgumentParser):
    parser.add_argument("--benchmark", help="path to .yaml file with the benchmark suite", required=True)
    parser.add_argument("--config", help="path to .yaml file with configuration for the test suite", required=True, nargs='+')
    parser.add_argument("--generate-graphs", help="generate graphs for each run metric", action='store_true')
    parser.add_argument("--legacy-cores-per-worker", help="used to calculate async worker cpuset when using a version 1 config")
    parser.add_argument("--generate-summary-graphs", help="generate summary graphs for each benchmark", action='store_true')
    parser.add_argument("--pdf", help="generate per-benchmark summary PDFs and a merged suite PDF", action='store_true')
    parser.set_defaults(func=run_benchmark_suite_args)