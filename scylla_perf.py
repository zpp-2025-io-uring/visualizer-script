import json
import subprocess
from abc import ABC, abstractmethod
from pathlib import Path
from subprocess import CompletedProcess
from typing import override

from yaml import safe_load

from log import get_logger

logger = get_logger(__name__)


class OneExecutableTestRunner(ABC):
    def __init__(self, test_config: dict, config_path: Path, run_output_dir: Path, backends, skip_async_workers_cpuset):
        super().__init__()

        self.tester_path: Path = Path(test_config["path"]).expanduser().resolve()
        self.config_path: Path = config_path.resolve()
        self.run_output_dir: Path = run_output_dir.resolve()
        self.asymmetric_app_cpuset = test_config["asymmetric_app_cpuset"]
        self.asymmetric_async_worker_cpuset = test_config["asymmetric_async_worker_cpuset"]
        self.symmetric_cpuset = test_config["symmetric_cpuset"]
        self.backends = backends
        self.skip_async_workers_cpuset = skip_async_workers_cpuset

        logger.debug(f"Initialized single executable test runner with tester_path={self.tester_path}, config_path={self.config_path}, run_output_dir={self.run_output_dir}, asymmetric_app_cpuset={self.asymmetric_app_cpuset}, asymmetric_async_worker_cpuset={self.asymmetric_async_worker_cpuset}, symmetric_cpuset={self.symmetric_cpuset}, backends={self.backends}, skip_async_workers_cpuset={self.skip_async_workers_cpuset}")

    def run_tester_with_additional_args(
        self, backend: str, cpuset: str, async_worker_cpuset: str | None, args: list[str]
    ) -> CompletedProcess:
        logger.info(
            f"Running {self.__class__.__name__} with backend {backend}, cpuset: {cpuset}, async worker cpuset: {async_worker_cpuset}"
        )
        self.run_output_dir.mkdir(parents=True, exist_ok=True)

        argv = (
            [self.tester_path]
            + args
            + [
                "--reactor-backend",
                backend,
                "--cpuset",
                cpuset,
            ]
        )

        if async_worker_cpuset is not None:
            argv.extend(["--async-workers-cpuset", async_worker_cpuset])

        logger.debug(f"Running {argv=}")
        result = subprocess.run(
            argv,
            check=False,
            capture_output=True,
            text=True,
        )

        stdout_output_path: Path = self.run_output_dir / (backend + ".out")

        with open(stdout_output_path, "w") as f:
            print(result.stdout, file=f)

        stderr_output_path: Path = self.run_output_dir / (backend + ".err")

        with open(stderr_output_path, "w") as f:
            print(result.stderr, file=f)

        return result

    @abstractmethod
    def _run_test(self, backend: str, cpuset: str, async_worker_cpuset: str | None):
        pass

    def run(self) -> dict[str]:
        backends_data_parsed = {}

        for backend in self.backends:
            if backend == "asymmetric_io_uring":
                if self.skip_async_workers_cpuset:
                    backends_data_parsed[backend] = self._run_test(backend, self.asymmetric_app_cpuset, None)
                else:
                    backends_data_parsed[backend] = self._run_test(
                        backend, self.asymmetric_app_cpuset, self.asymmetric_async_worker_cpuset
                    )
            else:
                backends_data_parsed[backend] = self._run_test(backend, self.asymmetric_app_cpuset, None)

        return backends_data_parsed


class PerfSimpleQueryTestRunner(OneExecutableTestRunner):
    @override
    def _run_test(self, backend, cpuset, async_worker_cpuset) -> list[dict]:
        json_output_path = self.run_output_dir / "result.json"

        with open(self.config_path) as f:
            config = safe_load(f.read())

        args = ["perf-simple-query", "--json-result", str(json_output_path)]

        if "flags" in config:
            flags = config.pop("flags")

            for flag in flags:
                args.append(f"--{flag}")

        for key, val in config.items():
            args.extend([f"--{key}", str(val)])

        result = self.run_tester_with_additional_args(backend, cpuset, async_worker_cpuset, args)

        if result.returncode != 0:
            raise RuntimeError(f"Simple query test failed with error code {result.returncode}")
        with open(json_output_path) as f:
            metrics = json.loads(f.read())

        metrics["parameters"].pop("concurrency,partitions,cpus,duration")
        metrics.pop("test_properties")
        metrics.pop("versions")
        metrics.pop("parameters")

        return [metrics]
