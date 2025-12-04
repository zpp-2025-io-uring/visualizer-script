import argparse
import subprocess
from pathlib import Path
from generate import generate_graphs

class io_test_runner:
    def __init__(self, tester_path: Path, config_path: Path, output_dir: Path, storage_dir: Path):
        self.tester_path: Path = tester_path.resolve()
        self.config_path: Path = config_path.resolve()
        self.output_dir: Path = output_dir.resolve()
        self.storage_dir: Path = storage_dir.resolve()

    def __run_test(self, backend: str, output_filename: str):
        print(f"Running io_tester with backend {backend}")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.storage_dir.mkdir(parents=True, exist_ok=True)

        result = subprocess.run(
            [self.tester_path, "--conf", self.config_path, "--storage", self.storage_dir, "--reactor-backend", backend],
            capture_output=True,
            text=True,
        )

        stdout_output_path: Path = self.output_dir / (output_filename + ".out")

        with open(stdout_output_path, "w") as f:
            print(result.stdout, file=f)

        stderr_output_path: Path = self.output_dir / (output_filename + ".err")

        with open(stderr_output_path, "w") as f:
            print(result.stderr, file=f)

        self.storage_dir.rmdir()

        if (err := result.returncode != 0):
            raise RuntimeError(f"Tester failed with exit code {err}")


        return result.stdout

    def run(self):
        asymmetric_data = self.__run_test("asymmetric_io_uring", "asymmetric")
        symmetric_data = self.__run_test("io_uring", "symmetric")
        print("Generating graphs")
        generate_graphs(asymmetric_data, symmetric_data, self.output_dir)

def run_io_test(tester_path, config_path, output_dir, storage_dir):
    io_test_runner(Path(tester_path), Path(config_path), Path(output_dir), Path(storage_dir)).run()

def configure_run_io_parser(parser: argparse.ArgumentParser):
    parser.add_argument("--tester", help="path to io_tester", required=True)
    parser.add_argument("--config", help="path to configuration .yaml file", required=True)
    parser.add_argument("--output-dir", help="directory to save the output to", required=True)
    parser.add_argument("--storage", help="directory for temporary files", default="./temp")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="io_tester runner and visualizer")
    configure_run_io_parser(parser)
    args = parser.parse_args()
    run_io_test(args.tester, args.config, args.output_dir, args.storage)
