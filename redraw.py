import argparse
from pathlib import Path

from generate import PlotGenerator
from pdf_summary import generate_benchmark_summary_pdf
from parse import auto_generate_data_points, load_data
from stats import join_metrics


def run_redraw(backend_paths: dict, output_dir, generate_pdf: bool):
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

    plot_generator = PlotGenerator()
    plot_generator.schedule_generate_graphs(sharded_metrics, shardless_metrics, output_dir)
    if generate_pdf:
        plot_generator.schedule_generate_graphs(sharded_metrics, shardless_metrics, output_dir, image_format="png")

    plot_generator.plot()

    if generate_pdf:
        summary_images = sorted(output_dir.glob("*.png"))
        generate_benchmark_summary_pdf(
            benchmark_name=output_dir.name,
            images=summary_images,
            output_pdf=output_dir / "summary.pdf",
        )


def run_redraw_args(args):
    backend_names = ["asymmetric_io_uring", "io_uring", "linux-aio", "epoll"]

    backend_paths = {}

    args_dict = vars(args)
    for backend in backend_names:
        if backend in args_dict and args_dict[backend] is not None:
            backend_paths[backend] = args_dict[backend]

    run_redraw(backend_paths, args.output_dir, args.pdf)


def configure_redraw_parser(parser: argparse.ArgumentParser):
    parser.add_argument("--asymmetric_io_uring", help="path to asymmetric_io_uring results", default=None)
    parser.add_argument("--io_uring", help="path to asymmetric_io_uring results", default=None)
    parser.add_argument("--linux-aio", help="path to linux-aio results", default=None)
    parser.add_argument("--epoll", help="path to epoll results", default=None)
    parser.add_argument("--output-dir", help="directory to save the output to", required=True)
    parser.add_argument("--pdf", help="generate a summary PDF from the redrawn graphs", action="store_true")
    parser.set_defaults(func=run_redraw_args)
