import argparse
import re
from pathlib import Path

from benchmark import Benchmark
from benchmarks import BENCHMARK_SUMMARY_FILENAME, benchmark_summary_pdf_filename, suite_summary_pdf_filename
from generate import PlotGenerator
from log import get_logger
from parse import auto_generate_data_points, load_data
from pdf_summary import generate_benchmark_summary_pdf, merge_pdfs
from stats import join_metrics

logger = get_logger()


class RedrawSuiteRunner:
    def __init__(self, generate_pdf: bool):
        self.plot_generator = PlotGenerator()
        self.generate_pdf = generate_pdf
        self.summary_pdf_targets: list[Path] = []

    def redraw_run(self, run_dir: Path):
        logger.info(f"Redrawing {run_dir}")
        backend_names = ["asymmetric_io_uring", "io_uring", "linux-aio", "epoll"]

        regexes = [rf"({backend}.out|{backend}.client.out)" for backend in backend_names]
        backend_data_raw: dict[str, str] = {}
        for file in run_dir.iterdir():
            for backend, regex in zip(backend_names, regexes):
                if re.fullmatch(regex, str(file.name)):
                    logger.info(f"Found data for backend {backend} in {file.name}")
                    with open(file) as f:
                        backend_data_raw[backend] = f.read()

        backends_parsed = {}
        for backend, raw in backend_data_raw.items():
            parsed = load_data(raw)
            backends_parsed[backend] = auto_generate_data_points(parsed)

        [shardless_metrics, sharded_metrics] = join_metrics(backends_parsed)
        self.plot_generator.schedule_generate_graphs(sharded_metrics, shardless_metrics, run_dir)

    def run_redraw_suite(self, dir):
        dir = Path(dir)

        for benchmark_dir in dir.iterdir():
            if benchmark_dir.is_dir():
                summary_file = benchmark_dir / BENCHMARK_SUMMARY_FILENAME
                if summary_file.is_file():
                    self.redraw_summary(Path(summary_file), Path(benchmark_dir))

                for run_dir in benchmark_dir.iterdir():
                    if run_dir.is_dir():
                        self.redraw_run(run_dir)

        self.plot_generator.plot()

        if self.generate_pdf and self.summary_pdf_targets:
            per_benchmark_pdfs: list[Path] = []
            for benchmark_dir in self.summary_pdf_targets:
                summary_images = sorted(benchmark_dir.glob("*.png"))
                pdf_path = generate_benchmark_summary_pdf(
                    benchmark_name=benchmark_dir.name,
                    images=summary_images,
                    output_pdf=benchmark_dir / benchmark_summary_pdf_filename(benchmark_dir.name),
                )
                per_benchmark_pdfs.append(pdf_path)

            if per_benchmark_pdfs:
                merge_pdfs(
                    input_pdfs=per_benchmark_pdfs,
                    output_pdf=dir / suite_summary_pdf_filename(dir.name),
                )

    def redraw_summary(self, summary_file: Path, output_dir: Path):
        logger.info(f"Redrawing summary from {summary_file}")

        with open(summary_file) as file:
            summary = Benchmark.load_from_file(file)
        self.plot_generator.schedule_graphs_for_summary(summary.get_stats(), output_dir)
        if self.generate_pdf:
            self.plot_generator.schedule_graphs_for_summary(summary.get_stats(), output_dir, image_format="png")
            self.summary_pdf_targets.append(output_dir)


def run_redraw_suite_args(args):
    runner = RedrawSuiteRunner(args.pdf)
    runner.run_redraw_suite(args.dir)


def configure_redraw_suite_parser(parser: argparse.ArgumentParser):
    parser.add_argument("--dir", help="directory to save the output to", required=True)
    parser.add_argument("--pdf", help="generate per-benchmark summary PDFs and a merged suite PDF", action="store_true")
    parser.set_defaults(func=run_redraw_suite_args)
