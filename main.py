import argparse
from run_io import configure_run_io_parser
from run_rpc import configure_run_rpc_parser
from redraw import configure_redraw_parser
from benchmarks import configure_run_benchmark_suite_parser

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Utility for running seastar performance tests")

    subparsers = parser.add_subparsers(dest="subprogram", required=True, description="subprogram to execute")

    configure_run_io_parser(subparsers.add_parser(name="io", help="run io_tester"))
    configure_run_rpc_parser(subparsers.add_parser(name="rpc", help="run rpc_tester"))
    configure_redraw_parser(subparsers.add_parser(name="redraw", help="generate graphs from existing inputs"))
    configure_run_benchmark_suite_parser(subparsers.add_parser(name="suite", help="run a benchmark suite"))

    args = parser.parse_args()

    args.func(args)