import argparse
from pathlib import Path

from benchmark import Benchmark
from benchmarks import BENCHMARK_SUMMARY_FILENAME
from generate import PlotGenerator
from log import get_logger
from metadata import BenchmarkMetadataHolder

logger = get_logger()


class RedrawSuiteRunner:
    def __init__(self, metadata_holder: BenchmarkMetadataHolder) -> None:
        self.plot_generator = PlotGenerator(metadata_holder)

    def run_redraw_suite(self, dir: Path) -> None:
        for benchmark_dir in dir.iterdir():
            if not benchmark_dir.is_dir():
                continue
            summary_file = benchmark_dir / BENCHMARK_SUMMARY_FILENAME
            if not summary_file.is_file():
                logger.warning(f"Missing summary file {summary_file} in benchmark directory {benchmark_dir}, skipping")
                continue

            self.redraw_summary(summary_file, benchmark_dir)

        self.plot_generator.plot()

    def redraw_summary(self, summary_file: Path, output_dir: Path):
        logger.info(f"Redrawing summary from {summary_file}")

        with open(summary_file) as file:
            summary = Benchmark.load_from_file(file)
        self.plot_generator.schedule_graphs_for_summary(summary.get_stats(), output_dir)

        for run in summary.get_runs():
            run_id = run.id
            run_output_dir = output_dir / f"run_{run_id}"
            run_output_dir.mkdir(exist_ok=True, parents=True)
            self.plot_generator.schedule_graphs_for_run(run.results, run_output_dir)


def run_redraw_suite_args(args: argparse.Namespace, metadata_holder: BenchmarkMetadataHolder) -> None:
    runner = RedrawSuiteRunner(metadata_holder)
    runner.run_redraw_suite(Path(args.dir))


def configure_redraw_suite_parser(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--dir", help="directory to save the output to", required=True)
    parser.set_defaults(func=run_redraw_suite_args)
