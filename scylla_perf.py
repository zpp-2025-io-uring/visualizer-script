from abc import ABC, abstractmethod
from pathlib import Path

class ScyllaPerformanceTestRunner(ABC):
    def __init__(self, scylla_test_config: dict, config_path: Path, run_output_dir: Path, backends, skip_async_workers_cpuset):
        super().__init__()

        self.tester_path: Path = Path(scylla_test_config["tester_path"]).expanduser().resolve()
        self.config_path: Path = config_path.resolve()
        self.run_output_dir: Path = run_output_dir.resolve()
        self.asymmetric_app_cpuset = scylla_test_config["asymmetric_app_cpuset"]
        self.asymmetric_async_worker_cpuset = scylla_test_config["asymmetric_async_worker_cpuset"]
        self.symmetric_cpuset = scylla_test_config["symmetric_cpuset"]
        self.backends = backends
        self.skip_async_workers_cpuset = skip_async_workers_cpuset

    @abstractmethod
    def __run_test(self, backend: str, cpuset: str, async_worker_cpuset: str | None):
        pass

    @abstractmethod
    def run(self) -> dict[str, str]:
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
                backends_data_raw[backend] = self.__run_test(backend, backend, self.asymmetric_app_cpuset, None)

        return backends_data_raw