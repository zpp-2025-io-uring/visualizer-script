import subprocess
from pathlib import Path

from log import get_logger, warn_if_not_release

logger = get_logger()


class IOTestRunner:
    def __init__(
        self,
        io_runner_config: dict,
        config_path: Path,
        run_output_dir: Path,
        backends: list[str],
        skip_async_workers_cpuset: bool,
    ):
        self.tester_path: Path = Path(io_runner_config["tester_path"]).expanduser().resolve()
        self.config_path: Path = config_path.resolve()
        self.run_output_dir = run_output_dir.resolve()
        self.storage_dir: Path = Path(io_runner_config["storage_dir"]).resolve()
        self.asymmetric_app_cpuset = io_runner_config["asymmetric_app_cpuset"]
        self.asymmetric_async_worker_cpuset = io_runner_config["asymmetric_async_worker_cpuset"]
        self.symmetric_cpuset = io_runner_config["symmetric_cpuset"]
        self.backends = backends
        self.skip_async_workers_cpuset = skip_async_workers_cpuset

        warn_if_not_release(self.tester_path)

    def __run_test(self, backend: str, output_filename: str, cpuset: str, async_worker_cpuset: str | None):
        logger.info(
            f"Running io_tester with backend {backend}, cpuset: {cpuset}, async worker cpuset: {async_worker_cpuset}"
        )
        self.run_output_dir.mkdir(parents=True, exist_ok=True)
        self.storage_dir.mkdir(parents=True, exist_ok=True)

        argv = [
            self.tester_path,
            "--conf",
            self.config_path,
            "--storage",
            self.storage_dir,
            "--reactor-backend",
            backend,
            "--cpuset",
            cpuset,
        ]
        if async_worker_cpuset is not None:
            argv.extend(["--async-workers-cpuset", async_worker_cpuset])

        result = subprocess.run(
            argv,
            capture_output=True,
            text=True,
            check=True,
        )

        stdout_output_path: Path = self.run_output_dir / (output_filename + ".out")

        with open(stdout_output_path, "w") as f:
            print(result.stdout, file=f)

        stderr_output_path: Path = self.run_output_dir / (output_filename + ".err")

        with open(stderr_output_path, "w") as f:
            print(result.stderr, file=f)

        self.storage_dir.rmdir()

        if err := result.returncode != 0:
            raise RuntimeError(f"Tester failed with exit code {err}")

        return result.stdout

    def run(self) -> dict:
        backends_data_raw = {}

        for backend in self.backends:
            if backend == "asymmetric_io_uring":
                if self.skip_async_workers_cpuset:
                    backends_data_raw[backend] = self.__run_test(backend, backend, self.asymmetric_app_cpuset, None)
                else:
                    backends_data_raw[backend] = self.__run_test(
                        backend, backend, self.asymmetric_app_cpuset, self.asymmetric_async_worker_cpuset
                    )
            else:
                backends_data_raw[backend] = self.__run_test(backend, backend, self.symmetric_cpuset, None)

        return backends_data_raw


def run_io_test(io_runner_config: dict, config_path, run_output_dir, backends, skip_async_workers_cpuset: bool) -> dict:
    return IOTestRunner(
        io_runner_config, Path(config_path), Path(run_output_dir), backends, skip_async_workers_cpuset
    ).run()
