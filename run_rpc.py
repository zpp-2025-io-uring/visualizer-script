import subprocess
from pathlib import Path
from time import sleep

from log import get_logger

logger = get_logger(__name__)


class RpcTestRunner:
    def __init__(
        self, rpc_runner_config: dict, config_path: Path, run_output_dir: Path, backends, skip_async_workers_cpuset
    ):
        self.tester_path: Path = Path(rpc_runner_config["tester_path"]).expanduser().resolve()
        self.config_path: Path = config_path.resolve()
        self.run_output_dir: Path = run_output_dir.resolve()
        self.ip_address = rpc_runner_config["ip_address"]
        self.asymmetric_server_app_cpuset = rpc_runner_config["asymmetric_server_app_cpuset"]
        self.asymmetric_server_async_worker_cpuset = rpc_runner_config["asymmetric_server_async_worker_cpuset"]
        self.symmetric_server_cpuset = rpc_runner_config["symmetric_server_cpuset"]
        self.asymmetric_client_app_cpuset = rpc_runner_config["asymmetric_client_app_cpuset"]
        self.asymmetric_client_async_worker_cpuset = rpc_runner_config["asymmetric_client_async_worker_cpuset"]
        self.symmetric_client_cpuset = rpc_runner_config["symmetric_client_cpuset"]
        self.backends = backends
        self.skip_async_workers_cpuset = skip_async_workers_cpuset

    def __run_test(
        self,
        backend: str,
        output_filename: str,
        server_cpuset: str,
        server_async_worker_cpuset: str | None,
        client_cpuset: str,
        client_async_worker_cpuset: str | None,
    ):
        print(
            f"Running rpc_tester with backend {backend}, server cpuset: {server_cpuset}, server async worker cpuset: {server_async_worker_cpuset}, client cpuset: {client_cpuset}, client async worker cpuset: {client_async_worker_cpuset}"
        )
        self.run_output_dir.mkdir(parents=True, exist_ok=True)

        argv = [
            self.tester_path,
            "--conf",
            self.config_path,
            "--listen",
            self.ip_address,
            "--reactor-backend",
            backend,
            "--cpuset",
            server_cpuset,
        ]
        if server_async_worker_cpuset is not None:
            argv.extend(["--async-workers-cpuset", server_async_worker_cpuset])

        server_process = subprocess.Popen(
            argv,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        sleep(1)

        try:
            argv = [
                self.tester_path,
                "--conf",
                self.config_path,
                "--connect",
                self.ip_address,
                "--reactor-backend",
                backend,
                "--cpuset",
                client_cpuset,
            ]
            if client_async_worker_cpuset is not None:
                argv.extend(["--async-workers-cpuset", client_async_worker_cpuset])

            client = subprocess.run(
                argv,
                capture_output=True,
                text=True,
            )
        except KeyboardInterrupt:
            server_process.terminate()

            if server_process.poll() is None:
                sleep(1)

            if server_process.poll() is None:
                logger.warning("Force killing server")
                server_process.kill()

            raise

        server_process.terminate()

        sleep(1)

        if server_process.poll() is None:
            server_process.kill()

        server_process.wait()

        server_stdout, server_stderr = server_process.communicate()

        server_stdout_output_path: Path = self.run_output_dir / (output_filename + ".server.out")

        with open(server_stdout_output_path, "w") as f:
            print(server_stdout, file=f)

        server_stderr_output_path: Path = self.run_output_dir / (output_filename + ".server.err")

        with open(server_stderr_output_path, "w") as f:
            print(server_stderr, file=f)

        client_stdout_output_path: Path = self.run_output_dir / (output_filename + ".client.out")

        with open(client_stdout_output_path, "w") as f:
            print(client.stdout, file=f)

        client_stderr_output_path: Path = self.run_output_dir / (output_filename + ".client.err")

        with open(client_stderr_output_path, "w") as f:
            print(client.stderr, file=f)

        if (err := server_process.returncode) != 0:
            raise RuntimeError(f"Server failed with exit code {err}")

        if err := client.returncode != 0:
            raise RuntimeError(f"Client failed with exit code {err}")

        return client.stdout

    def run(self) -> dict:
        backends_data_raw = {}

        for backend in self.backends:
            if backend == "asymmetric_io_uring":
                if self.skip_async_workers_cpuset:
                    backends_data_raw[backend] = self.__run_test(
                        backend,
                        backend,
                        self.asymmetric_server_app_cpuset,
                        None,
                        self.asymmetric_client_app_cpuset,
                        None,
                    )
                else:
                    backends_data_raw[backend] = self.__run_test(
                        backend,
                        backend,
                        self.asymmetric_server_app_cpuset,
                        self.asymmetric_server_async_worker_cpuset,
                        self.asymmetric_client_app_cpuset,
                        self.asymmetric_client_async_worker_cpuset,
                    )
            else:
                backends_data_raw[backend] = self.__run_test(
                    backend, backend, self.symmetric_server_cpuset, None, self.symmetric_client_cpuset, None
                )

        return backends_data_raw


def run_rpc_test(rpc_runner_config: dict, config_path, run_output_dir, backends, skip_async_workers_cpuset) -> dict:
    return RpcTestRunner(
        rpc_runner_config, Path(config_path), Path(run_output_dir), backends, skip_async_workers_cpuset
    ).run()
