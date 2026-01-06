import argparse
from os import cpu_count
import subprocess
from pathlib import Path

class io_test_runner:
    def __init__(self, tester_path: Path, config_path: Path, output_dir: Path, storage_dir: Path, asymmetric_app_cpuset: str, asymmetric_async_worker_cpuset: str, symmetric_cpuset: str, backends):
        self.tester_path: Path = tester_path.resolve()
        self.config_path: Path = config_path.resolve()
        self.output_dir: Path = output_dir.resolve()
        self.storage_dir: Path = storage_dir.resolve()
        self.asymmetric_app_cpuset = asymmetric_app_cpuset
        self.asymmetric_async_worker_cpuset = asymmetric_async_worker_cpuset
        self.symmetric_cpuset = symmetric_cpuset
        self.backends = backends

    def __run_test(self, backend: str, output_filename: str, cpuset: str, async_worker_cpuset: str | None):
        print(f"Running io_tester with backend {backend}")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.storage_dir.mkdir(parents=True, exist_ok=True)

        argv = [self.tester_path, "--conf", self.config_path, "--storage", self.storage_dir, "--reactor-backend", backend, "--cpuset", cpuset]
        if async_worker_cpuset is not None:
            argv.extend(['--async-workers-cpuset', async_worker_cpuset])

        result = subprocess.run(
            argv,
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

    def run(self) -> dict:
        backends_data_raw = dict()

        for backend in self.backends:
            if backend == 'asymmetric_io_uring':
                backends_data_raw[backend] = self.__run_test(backend, backend, self.asymmetric_app_cpuset, self.asymmetric_async_worker_cpuset)
            else:
                backends_data_raw[backend] = self.__run_test(backend, backend, self.asymmetric_app_cpuset, None)

        return backends_data_raw

def run_io_test(tester_path, config_path, output_dir, storage_dir, asymmetric_app_cpuset, asymmetric_async_worker_cpuset, symmetric_cpuset, backends) -> dict:
    return io_test_runner(Path(tester_path), Path(config_path), Path(output_dir), Path(storage_dir), asymmetric_app_cpuset, asymmetric_async_worker_cpuset, symmetric_cpuset, backends).run()

def run_io_test_args(args):
    run_io_test(args.tester, args.config, args.output_dir, args.storage, args.asymmetric_app_cpuset, args.asymmetric_async_worker_cpuset, args.symmetric_cpuset, args.backends)

def configure_run_io_parser(parser: argparse.ArgumentParser):
    cpus = cpu_count()

    parser.add_argument("--tester", help="path to io_tester", required=True)
    parser.add_argument("--config", help="path to configuration .yaml file", required=True)
    parser.add_argument("--output-dir", help="directory to save the output to", required=True)
    parser.add_argument("--storage", help="directory for temporary files", default="./temp")
    parser.add_argument("--asymmetric-app-cpuset", help="cpuset for the asymmetric seastar app", default=f"0-{cpus-1}")
    parser.add_argument("--asymmetric-async-worker-cpuset", help="cpuset for the async workers", default=f"0-{cpus-1}")
    parser.add_argument("--symmetric-cpuset", help="cpuset for the symmetric seastar app", default=f"0-{cpus-1}")
    parser.add_argument("--backends", help="list of backends to compare", nargs='+', default=['asymmetric_io_uring', 'io_uring'])
    parser.set_defaults(func=run_io_test_args)
    parser.formatter_class = argparse.ArgumentDefaultsHelpFormatter
