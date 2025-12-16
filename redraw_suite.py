import argparse
import re
from pathlib import Path
from generate import generate_graphs
from parse import load_data, auto_generate_data_points
from stats import join_metrics

def redraw_run(run_dir: Path):
    print(f"Redrawing {run_dir}")
    backend_names = ['asymmetric_io_uring', 'io_uring', 'linux-aio', 'epoll']

    regexes = [rf'({backend}.out|{backend}.client.out)' for backend in backend_names]


    backend_data_raw: dict[str, str] = dict()
    for file in run_dir.iterdir():
        for backend, regex in zip(backend_names, regexes):
            if re.fullmatch(regex, str(file.name)):
                print(f"Found data for backend {backend} in {file.name}")
                with open(file, 'r') as f:
                    backend_data_raw[backend] = f.read()

    backends_parsed = {}
    for backend, raw in backend_data_raw.items():
        parsed = load_data(raw)
        backends_parsed[backend] = auto_generate_data_points(parsed)

    [shardless_metrics, sharded_metrics] = join_metrics(backends_parsed)
    generate_graphs(sharded_metrics, shardless_metrics, run_dir)

    

def run_redraw_suite(dir):
    dir = Path(dir)

    for benchmark_dir in dir.iterdir():
        if benchmark_dir.is_dir():
            for run_dir in benchmark_dir.iterdir():
                if run_dir.is_dir():
                    redraw_run(run_dir)

def run_redraw_suite_args(args):
    run_redraw_suite(args.dir)

def configure_redraw_suite_parser(parser: argparse.ArgumentParser):
    parser.add_argument("--dir", help="directory to save the output to", required=True)
    parser.set_defaults(func=run_redraw_suite_args)
