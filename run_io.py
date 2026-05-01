import subprocess
from os import PathLike
from pathlib import Path

from log import get_logger, warn_if_not_release
from parse import RawBackendData, load_data
from remote import CmdOutput, IoTesterParams, Remote

logger = get_logger()


class IOTestRunner:
    def __init__(
        self,
        io_runner_config: dict,
        config_path: Path,
        run_output_dir: Path,
        skip_async_workers_cpuset: bool,
    ) -> None:
        self.tester_path: Path = Path(io_runner_config["tester_path"]).expanduser().resolve()
        self.config_path: Path = config_path.resolve()
        self.run_output_dir = run_output_dir.resolve()
        self.storage_dir: Path = Path(io_runner_config["storage_dir"]).resolve()
        self.asymmetric_app_cpuset = io_runner_config["asymmetric_app_cpuset"]
        self.asymmetric_async_worker_cpuset = io_runner_config["asymmetric_async_worker_cpuset"]
        self.symmetric_cpuset = io_runner_config["symmetric_cpuset"]
        self.skip_async_workers_cpuset = skip_async_workers_cpuset
        if (remote := io_runner_config.get("remote", None)) is not None:
            remote = Remote(remote)
        self.remote: Remote | None = remote
        self.extra_options: list[str] = io_runner_config.get("extra_options", [])

        warn_if_not_release(self.tester_path)

    def __run_test_process(self, backend: str, cpuset: str, async_worker_cpuset: str | None) -> CmdOutput:
        opts_argv = [
            "--reactor-backend",
            backend,
            "--cpuset",
            cpuset,
        ] + self.extra_options

        if async_worker_cpuset is not None:
            opts_argv.extend(["--async-workers-cpuset", async_worker_cpuset])

        if self.remote is None:
            argv: list[str | bytes | PathLike[str] | PathLike[bytes]] = [
                self.tester_path,
                "--conf",
                self.config_path,
                "--storage",
                self.storage_dir,
            ] + opts_argv

            result: subprocess.CompletedProcess | CmdOutput = subprocess.run(
                argv,
                check=False,
                capture_output=True,
                text=True,
            )

            return CmdOutput(stdout=result.stdout, stderr=result.stderr, returncode=result.returncode)
        else:
            try:
                with open(self.config_path) as f:
                    process = self.remote.run_io_tester(IoTesterParams(config=f.read(), argv=opts_argv))
                return process.wait()
            except KeyboardInterrupt:
                logger.warning("remote io_tester interrupted")
                process.kill()
                process.wait()  # Clear zombie
                raise

    def __run_test(
        self, backend: str, output_filename: str, cpuset: str, async_worker_cpuset: str | None
    ) -> RawBackendData:
        logger.info(
            f"Running io_tester with backend {backend}, cpuset: {cpuset}, async worker cpuset: {async_worker_cpuset}"
        )
        self.run_output_dir.mkdir(parents=True, exist_ok=True)
        self.storage_dir.mkdir(parents=True, exist_ok=True)

        result = self.__run_test_process(backend, cpuset, async_worker_cpuset)

        stdout_output_path: Path = self.run_output_dir / (output_filename + ".out")

        with open(stdout_output_path, "w") as f:
            print(result.stdout, file=f)

        stderr_output_path: Path = self.run_output_dir / (output_filename + ".err")

        with open(stderr_output_path, "w") as f:
            print(result.stderr, file=f)

        self.storage_dir.rmdir()

        if (err := result.returncode) != 0:
            raise RuntimeError(f"Tester failed with exit code {err}")

        return load_data(result.stdout)

    def run(self, backend: str) -> RawBackendData:
        if backend == "asymmetric_io_uring":
            if self.skip_async_workers_cpuset:
                return self.__run_test(backend, backend, self.asymmetric_app_cpuset, None)
            else:
                return self.__run_test(
                    backend, backend, self.asymmetric_app_cpuset, self.asymmetric_async_worker_cpuset
                )
        else:
            return self.__run_test(backend, backend, self.symmetric_cpuset, None)


def run_io_test(
    io_runner_config: dict, config_path: Path, run_output_dir: Path, backend: str, skip_async_workers_cpuset: bool
) -> RawBackendData:
    return IOTestRunner(io_runner_config, config_path, run_output_dir, skip_async_workers_cpuset).run(backend)
