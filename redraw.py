import argparse
from pathlib import Path
from generate import generate_graphs

def run_redraw(asymmetric_path, symmetric_path, output_dir):
    symmetric_path = Path(symmetric_path)
    asymmetric_path = Path(asymmetric_path)
    output_dir = Path(output_dir)

    with open(symmetric_path, "r") as f:
        symmetric_data = f.read()

    with open(asymmetric_path, "r") as f:
        asymmetric_data = f.read()

    generate_graphs(asymmetric_data, symmetric_data, output_dir)

def run_redraw_args(args):
    run_redraw(args.asymmetric, args.symmetric, args.output_dir)

def configure_redraw_parser(parser: argparse.ArgumentParser):
    parser.add_argument("--symmetric", help="path to symmetric results", required=True)
    parser.add_argument("--asymmetric", help="path to asymmetric results", required=True)
    parser.add_argument("--output-dir", help="directory to save the output to", required=True)
    parser.set_defaults(func=run_redraw_args)
