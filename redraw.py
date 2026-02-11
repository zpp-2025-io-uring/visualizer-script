import argparse
from pathlib import Path

from generate import PlotGenerator
from metadata import BACKENDS_NAMES, Metadata
from parse import auto_generate_data_points, join_metrics, load_data


def run_redraw(metadata: Metadata, backend_paths: dict, output_dir):
    output_dir = Path(output_dir)

    backends_data_raw = {}
    for backend, path in backend_paths.items():
        with open(Path(path)) as f:
            backends_data_raw[backend] = f.read()

    # Convert raw outputs to metrics mapping expected by generate_graphs
    backends_parsed = {}
    for backend, raw in backends_data_raw.items():
        parsed = load_data(raw)
        backends_parsed[backend] = auto_generate_data_points(parsed)

    (shardless_metrics, sharded_metrics) = join_metrics(backends_parsed)

    plot_generator = PlotGenerator(metadata)
    plot_generator.schedule_generate_graphs(sharded_metrics, shardless_metrics, output_dir)
    plot_generator.plot()


def run_redraw_args(args: argparse.Namespace, metadata: Metadata) -> None:
    backend_paths = {}

    args_dict = vars(args)
    for backend in BACKENDS_NAMES:
        if backend in args_dict and args_dict[backend] is not None:
            backend_paths[backend] = args_dict[backend]

    run_redraw(metadata, backend_paths, args.output_dir)


def configure_redraw_parser(parser: argparse.ArgumentParser) -> None:
    for backend in BACKENDS_NAMES:
        parser.add_argument(f"--{backend}", help=f"path to {backend} results", default=None)
    parser.add_argument("--output-dir", help="directory to save the output to", required=True)
    parser.set_defaults(func=run_redraw_args)
