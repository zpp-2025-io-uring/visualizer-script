import argparse
import pathlib

from benchmarks import SUPPORTED_BENCHMARK_TYPES, configure_run_benchmark_suite_parser
from log import get_logger, set_level
from metadata import BenchmarkMetadata, BenchmarkMetadataHolder
from redraw import configure_redraw_parser
from redraw_suite import configure_redraw_suite_parser

logger = get_logger()


def _cli_key_for_benchmark_type(type: str) -> str:
    return f"{type}-metadata"


def _configure_metadata_parser(parser: argparse.ArgumentParser):
    default_metadata_dir = str(pathlib.Path(__file__).resolve().parent / "configuration" / "plots")
    for type in SUPPORTED_BENCHMARK_TYPES:
        parser.add_argument(
            f"--{_cli_key_for_benchmark_type(type)}",
            help=f"path to {type} benchmark metadata file",
            default=f"{default_metadata_dir}/{type}.yaml",
        )


def _load_metadata_from_args(args: argparse.Namespace) -> BenchmarkMetadataHolder:
    args_dict = vars(args)
    print(args_dict)

    metadata_holder = BenchmarkMetadataHolder()
    for type in SUPPORTED_BENCHMARK_TYPES:
        # argparse converts flag names with '-' into attribute names with '_'
        cli_key = _cli_key_for_benchmark_type(type)  # e.g. "io-metadata"
        arg_key = cli_key.replace("-", "_")  # e.g. "io_metadata"

        if arg_key not in args_dict or args_dict[arg_key] is None:
            logger.warning(f"No metadata file provided for benchmark type {type}, using empty metadata")
            continue

        metadata_path = args_dict[arg_key]
        with open(metadata_path) as f:
            metadata = BenchmarkMetadata.load_from_yaml(f)
        logger.info(f"Loaded metadata for benchmark type {type} from {metadata_path}")
        metadata_holder.set_metadata(type, metadata)

    return metadata_holder


def main(argv=None):
    parser = argparse.ArgumentParser(description="Utility for running seastar performance tests")

    subparsers = parser.add_subparsers(dest="subprogram", required=True, description="subprogram to execute")

    configure_redraw_parser(subparsers.add_parser(name="redraw", help="generate graphs from existing inputs"))
    configure_redraw_suite_parser(
        subparsers.add_parser(name="redraw_suite", help="generate graphs for an existing run")
    )
    configure_run_benchmark_suite_parser(subparsers.add_parser(name="suite", help="run a benchmark suite"))

    _configure_metadata_parser(parser)
    parser.add_argument(
        "--log-level",
        help="logger output level",
        choices=["debug", "info", "warning", "error", "critical"],
        default="info",
    )

    args = parser.parse_args(argv)
    set_level(args.log_level)
    metadata_holder = _load_metadata_from_args(args)
    args.func(args, metadata_holder)


if __name__ == "__main__":
    main()
