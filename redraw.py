import argparse
from pathlib import Path
from generate import generate_graphs

def run_io_test(asymmetric_path, symmetric_path, output_dir):
    symmetric_path = Path(symmetric_path)
    asymmetric_path = Path(asymmetric_path)
    output_dir = Path(output_dir)

    with open(symmetric_path, "r") as f:
        symmetric_data = f.read()

    with open(symmetric_path, "r") as f:
        asymmetric_data = f.read()

    generate_graphs(asymmetric_data, symmetric_data, output_dir)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="io_tester runner and visualizer")

    parser.add_argument("--symmetric", help="Path to symmetric results")
    parser.add_argument("--asymmetric", help="Path to configuration .yaml file")
    parser.add_argument("--output-dir", help="Directory to save the output to", required=True)

    args = parser.parse_args()

    output_dir = Path(args.output_dir)

    if args.symmetric is None:
        symmetric_path = output_dir / "symmetric.out"
    else:
        symmetric_path = args.symmetric

    if args.asymmetric is None:
        asymmetric_path = output_dir / "symmetric.out"
    else:
        asymmetric_path = args.asymmetric

    run_io_test(asymmetric_path, symmetric_path, output_dir)
