import argparse
from pathlib import Path
from generate import generate_graphs
from parse import parse_metrics_from_raw_map

def run_redraw(backend_paths: dict, output_dir):
    output_dir = Path(output_dir)

    backends_data_raw = dict()
    for backend, path in backend_paths.items():
        with open(Path(path), "r") as f:
            backends_data_raw[backend] = f.read()

    (shardless_metrics, sharded_metrics) = parse_metrics_from_raw_map(backends_data_raw)
    generate_graphs(sharded_metrics, shardless_metrics, output_dir)

def run_redraw_args(args):
    backend_names = ['asymmetric_io_uring', 'io_uring', 'linux-aio', 'epoll']

    backend_paths = dict()

    args_dict = vars(args)
    for backend in backend_names:
        if backend in args_dict and args_dict[backend] is not None:
            backend_paths[backend] = args_dict[backend]

    run_redraw(backend_paths, args.output_dir)

def configure_redraw_parser(parser: argparse.ArgumentParser):
    parser.add_argument("--asymmetric_io_uring", help="path to asymmetric_io_uring results", default=None)
    parser.add_argument("--io_uring", help="path to asymmetric_io_uring results", default=None)
    parser.add_argument("--linux-aio", help="path to linux-aio results", default=None)
    parser.add_argument("--epoll", help="path to epoll results", default=None)
    parser.add_argument("--output-dir", help="directory to save the output to", required=True)
    parser.set_defaults(func=run_redraw_args)
