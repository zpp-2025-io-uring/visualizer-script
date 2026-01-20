import argparse
import re
from pathlib import Path

from benchmark import Benchmark
from benchmarks import BENCHMARK_SUMMARY_FILENAME
from generate import PlotGenerator
from log import get_logger
from parse import auto_generate_data_points, load_data
from stats import join_metrics

logger = get_logger(__name__)


class RedrawSuiteRunner:
    def __init__(self):
        self.plot_generator = PlotGenerator()

    def run_redraw_suite(self, dir):
        dir = Path(dir)

        for benchmark_dir in dir.iterdir():
            if benchmark_dir.is_dir():
                summary_file = benchmark_dir / BENCHMARK_SUMMARY_FILENAME
                if not summary_file.is_file():
                    raise Exception(f"Benchmark summary file not found at: {summary_file}")
                    
                logger.info(f"Redrawing summary from {summary_file}")
                with open(summary_file) as file:
                    summary = Benchmark.load_from_file(file)
                    self.plot_generator.schedule_graphs_for_summary(summary.get_stats(), benchmark_dir)
                    
                    for run in summary.get_runs():
                        run_dir = benchmark_dir / f"run_{run["id"]}"
                        if not run_dir.is_dir():
                            raise Exception(f"Run directory not found at: {run_dir}")
                        
                        self.plot_generator.schedule_generate_graphs(run["results"]["sharded_metrics"], run["results"]["shardless_metrics"], run_dir)
        
        self.plot_generator.plot()


def run_redraw_suite_args(args):
    runner = RedrawSuiteRunner()
    runner.run_redraw_suite(args.dir)


def configure_redraw_suite_parser(parser: argparse.ArgumentParser):
    parser.add_argument("--dir", help="directory to save the output to", required=True)
    parser.set_defaults(func=run_redraw_suite_args)
