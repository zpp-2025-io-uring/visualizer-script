import argparse
from pathlib import Path
from generate import generate_graphs
from parse import load_data, auto_generate_data_points, join_metrics

def run_redraw(backend_paths: dict, output_dir):
    output_dir = Path(output_dir)

    backends_data_raw = dict()
    for backend, path in backend_paths.items():
        with open(Path(path), "r") as f:
            backends_data_raw[backend] = f.read()

    # Convert raw outputs to metrics mapping expected by generate_graphs
    backends_parsed = {}
    for backend, raw in backends_data_raw.items():
        parsed = load_data(raw)
        backends_parsed[backend] = auto_generate_data_points(parsed)

    metrics = join_metrics(backends_parsed)
    generate_graphs(metrics, output_dir)

def run_redraw_args(args):
    backend_names = ['asymmetric_io_uring', 'io_uring', 'linux-aio', 'epoll']

    backend_paths = dict()

    args_dict = vars(args)
    for backend in backend_names:
        if args_dict[backend] is not None:
            backend_paths[backend] = args_dict[backend]

    run_redraw(backend_paths, args.output_dir)

def configure_redraw_parser(parser: argparse.ArgumentParser):
    parser.add_argument("--asymmetric_io_uring", help="path to asymmetric_io_uring results", default=None)
    parser.add_argument("--io_uring", help="path to asymmetric_io_uring results", default=None)
    parser.add_argument("--linux-aio", help="path to linux-aio results", default=None)
    parser.add_argument("--epoll", help="path to epoll results", default=None)
    parser.add_argument("--output-dir", help="directory to save the output to", required=True)
    parser.set_defaults(func=run_redraw_args)
