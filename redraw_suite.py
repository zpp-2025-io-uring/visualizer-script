import argparse
from pathlib import Path

from benchmark import Benchmark
from benchmarks import BENCHMARK_SUMMARY_FILENAME, SUITE_SUMMARY_PDF_FILENAME
from generate import PlotGenerator
from log import get_logger
from metadata import BenchmarkMetadataHolder
from pdf_summary import generate_benchmark_summary_pdf, merge_pdfs

logger = get_logger()


class RedrawSuiteRunner:
    def __init__(self, metadata_holder: BenchmarkMetadataHolder) -> None:
        self.plot_generator = PlotGenerator(metadata_holder)

    def run_redraw_suite(self, dir: Path) -> None:
        benchmark_dirs_to_render: list[tuple[str, Path]] = []
        for benchmark_dir in sorted(dir.iterdir()):
            if not benchmark_dir.is_dir():
                continue
            summary_file = benchmark_dir / BENCHMARK_SUMMARY_FILENAME
            if not summary_file.is_file():
                logger.warning(f"Missing summary file {summary_file} in benchmark directory {benchmark_dir}, skipping")
                continue

            benchmark_name = self.redraw_summary(summary_file, benchmark_dir)
            benchmark_dirs_to_render.append((benchmark_name, benchmark_dir))

        if benchmark_dirs_to_render:
            self.plot_generator.plot()

        per_benchmark_pdfs: list[Path] = []
        for benchmark_name, benchmark_dir in benchmark_dirs_to_render:
            logger.info(f"Generating PDF for {benchmark_name}")
            summary_images = sorted(benchmark_dir.glob("*.png"))
            pdf_path = generate_benchmark_summary_pdf(
                benchmark_name=benchmark_name,
                images=summary_images,
                output_pdf=benchmark_dir / "summary.pdf",
            )
            per_benchmark_pdfs.append(pdf_path)

        if per_benchmark_pdfs:
            logger.info("Merging benchmark PDFs")
            merge_pdfs(input_pdfs=per_benchmark_pdfs, output_pdf=dir / SUITE_SUMMARY_PDF_FILENAME)

    def redraw_summary(self, summary_file: Path, output_dir: Path) -> str:
        logger.info(f"Redrawing summary from {summary_file}")

        with open(summary_file) as file:
            summary = Benchmark.load_from_file(file)
        self.plot_generator.schedule_graphs_for_summary(
            summary.get_info().id, summary.get_stats(), output_dir, type=summary.get_info().type
        )
        self.plot_generator.schedule_graphs_for_summary(
            summary.get_info().id,
            summary.get_stats(),
            output_dir,
            type=summary.get_info().type,
            image_format="png",
        )

        for run in summary.get_runs():
            run_id = run.id
            run_output_dir = output_dir / f"run_{run_id}"
            run_output_dir.mkdir(exist_ok=True, parents=True)
            self.plot_generator.schedule_graphs_for_run(
                summary.get_info().id, run.results, run_output_dir, type=summary.get_info().type
            )

        return summary.get_info().id


def run_redraw_suite_args(args: argparse.Namespace, metadata_holder: BenchmarkMetadataHolder) -> None:
    runner = RedrawSuiteRunner(metadata_holder)
    runner.run_redraw_suite(Path(args.dir))


def configure_redraw_suite_parser(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--dir", help="directory to save the output to", required=True)
    parser.set_defaults(func=run_redraw_suite_args)
