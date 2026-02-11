import argparse

from benchmarks import configure_run_benchmark_suite_parser
from log import get_logger, set_level
from metadata import Metadata
from redraw import configure_redraw_parser
from redraw_suite import configure_redraw_suite_parser

logger = get_logger()


def load_metadata(path: str | None) -> Metadata:
    if path is None:
        logger.info("No metadata file provided, using default metadata")
        return Metadata()
    logger.info(f"Loading metadata from {path}")
    with open(path) as f:
        return Metadata.load_from_file(f)


def main(argv=None):
    parser = argparse.ArgumentParser(description="Utility for running seastar performance tests")

    subparsers = parser.add_subparsers(dest="subprogram", required=True, description="subprogram to execute")

    configure_redraw_parser(subparsers.add_parser(name="redraw", help="generate graphs from existing inputs"))
    configure_redraw_suite_parser(
        subparsers.add_parser(name="redraw_suite", help="generate graphs for an existing run")
    )
    configure_run_benchmark_suite_parser(subparsers.add_parser(name="suite", help="run a benchmark suite"))

    parser.add_argument(
        "--log-level",
        help="logger output level",
        choices=["debug", "info", "warning", "error", "critical"],
        default="info",
    )
    parser.add_argument("--metadata", help="path to a file containing metadata", default=None)

    args = parser.parse_args(argv)
    set_level(args.log_level)
    metadata = load_metadata(args.metadata)
    args.func(args, metadata)


if __name__ == "__main__":
    main()
