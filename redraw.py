import argparse
from pathlib import Path

from benchmark import compute_benchmark_summary
from generate import PlotGenerator
from metadata import BACKENDS_NAMES, BenchmarkMetadataHolder
from parse import auto_generate_data_points, join_metrics, load_data
from stats import join_stats


def run_redraw(metadata_holder: BenchmarkMetadataHolder, backend_paths: dict, output_dir):
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
    metrics_runs = [{"run_id": 0, "sharded": sharded_metrics, "shardless": shardless_metrics}]

    (combined_sharded, combined_shardless) = join_stats(metrics_runs)
    benchmark_info = {"id": "redraw", "properties": {"iterations": 1}}
    summary = compute_benchmark_summary(combined_sharded, combined_shardless, benchmark_info)

    plot_generator = PlotGenerator(metadata_holder)
    plot_generator.schedule_graphs_for_run(summary.runs[0].results, output_dir)
    plot_generator.plot()


def run_redraw_args(args, metadata_holder: BenchmarkMetadataHolder):
    backend_paths = {}

    args_dict = vars(args)
    for backend in BACKENDS_NAMES:
        if backend in args_dict and args_dict[backend] is not None:
            backend_paths[backend] = args_dict[backend]

    run_redraw(metadata_holder, backend_paths, args.output_dir)


def configure_redraw_parser(parser: argparse.ArgumentParser):
    for backend in BACKENDS_NAMES:
        parser.add_argument(f"--{backend}", help=f"path to {backend} results", default=None)
    parser.add_argument("--output-dir", help="directory to save the output to", required=True)
    parser.set_defaults(func=run_redraw_args)
