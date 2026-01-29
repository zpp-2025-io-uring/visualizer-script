import argparse

from benchmarks import configure_run_benchmark_suite_parser
from log import set_level
from redraw import configure_redraw_parser
from redraw_suite import configure_redraw_suite_parser


def test_func(args):
    print("Test function executed with args:", args)
    xd = [1, 2, 3]
    return xd


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

    args = parser.parse_args(argv)
    set_level(args.log_level)
    _ = test_func(args)

    args.func(args)


if __name__ == "__main__":
    main()
