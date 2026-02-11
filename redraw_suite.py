import argparse
import re
from pathlib import Path

from benchmark import Benchmark
from benchmarks import BENCHMARK_SUMMARY_FILENAME
from generate import PlotGenerator
from log import get_logger
from metadata import BACKENDS_NAMES, Metadata
from parse import auto_generate_data_points, join_metrics, load_data

logger = get_logger()


class RedrawSuiteRunner:
    def __init__(self, metadata: Metadata):
        self.plot_generator = PlotGenerator(metadata)

    def redraw_run(self, run_dir: Path):
        logger.info(f"Redrawing {run_dir}")

        regexes = [rf"({backend}.out|{backend}.client.out)" for backend in BACKENDS_NAMES]
        backend_data_raw: dict[str, str] = {}
        for file in run_dir.iterdir():
            for backend, regex in zip(BACKENDS_NAMES, regexes):
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

    def redraw_summary(self, summary_file: Path, output_dir: Path):
        logger.info(f"Redrawing summary from {summary_file}")

        with open(summary_file) as file:
            summary = Benchmark.load_from_file(file)
        self.plot_generator.schedule_graphs_for_summary(summary.get_stats(), output_dir)


def run_redraw_suite_args(args: argparse.Namespace, metadata: Metadata) -> None:
    runner = RedrawSuiteRunner(metadata)
    runner.run_redraw_suite(args.dir)


def configure_redraw_suite_parser(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--dir", help="directory to save the output to", required=True)
    parser.set_defaults(func=run_redraw_suite_args)
