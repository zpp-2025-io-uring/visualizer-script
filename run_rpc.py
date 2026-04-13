import subprocess
from pathlib import Path
from time import sleep

from log import get_logger, warn_if_not_release
from remote import CmdOutput, Remote, RpcTesterParams

logger = get_logger()


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
        if (server_remote := rpc_runner_config.get("server_remote", None)) is not None:
            self.server_remote: Remote | None = Remote(server_remote)
        else:
            self.server_remote: Remote | None = None
        if (client_remote := rpc_runner_config.get("client_remote", None)) is not None:
            self.client_remote: Remote | None = Remote(client_remote)
        else:
            self.client_remote: Remote | None = None
        self.remote_listen_address: str | None = rpc_runner_config.get("remote_listen_address", None)
        self.remote_listen_port: str | None = rpc_runner_config.get("remote_listen_port", None)
        self.remote_connect_address: str | None = rpc_runner_config.get("remote_connect_address", None)
        self.remote_connect_port: str | None = rpc_runner_config.get("remote_connect_port", None)

        warn_if_not_release(self.tester_path)

    def __run_server(self, backend: str,server_cpuset: str,server_async_worker_cpuset: str | None): # Creates a process
        if self.server_remote is None:
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
        else:
            with open(self.config_path, "r") as f:
                server_process = self.server_remote.run_rpc_tester(RpcTesterParams(f.read(), backend, self.remote_listen_address, self.remote_listen_port, is_server=True, app_cpuset=server_cpuset, async_worker_cpuset=server_async_worker_cpuset))
            
            return server_process
        
    def __run_client(self, backend: str,client_cpuset: str,client_async_worker_cpuset: str | None) -> CmdOutput:
        if self.client_remote is None:
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

            output = subprocess.run(
                argv,
                capture_output=True,
                text=True,
            )

            return CmdOutput(stdout=output.stdout, stderr=output.stderr, returncode=output.returncode)
        else:
            with open(self.config_path, "r") as f:
                return self.client_remote.run_rpc_tester(RpcTesterParams(f.read(), backend, self.remote_connect_address, self.remote_connect_port, is_server=False, app_cpuset=client_cpuset, async_worker_cpuset=client_async_worker_cpuset)).wait()
            

    def ___run_test(
        self,
        backend: str,
        server_cpuset: str,
        server_async_worker_cpuset: str | None,
        client_cpuset: str,
        client_async_worker_cpuset: str | None,
    ) -> tuple[CmdOutput, CmdOutput]:
        server_process = self.__run_server(backend, server_cpuset, server_async_worker_cpuset)

        sleep(1)

        try:
            client_output = self.__run_client(backend, client_cpuset, client_async_worker_cpuset)
        except KeyboardInterrupt:
            server_process.terminate()

            if server_process.poll() is None:
                sleep(1)

            if server_process.poll() is None:
                logger.warning("Force killing server")
                server_process.kill()

            raise

        sleep(1)

        server_process.terminate()

        sleep(1)

        if self.server_remote is None and server_process.poll() is None:
            server_process.kill()

        if self.server_remote is None:
            server_stdout, server_stderr = server_process.communicate()
            return client_output, CmdOutput(stdout=server_stdout, stderr=server_stderr, returncode=server_process.poll())
        else:
            return client_output, server_process.wait()

    def __run_test(
        self,
        backend: str,
        output_filename: str,
        server_cpuset: str,
        server_async_worker_cpuset: str | None,
        client_cpuset: str,
        client_async_worker_cpuset: str | None,
    ):
        logger.info(
            f"Running rpc_tester with backend {backend}, server cpuset: {server_cpuset}, server async worker cpuset: {server_async_worker_cpuset}, client cpuset: {client_cpuset}, client async worker cpuset: {client_async_worker_cpuset}"
        )
        self.run_output_dir.mkdir(parents=True, exist_ok=True)

        client, server = self.___run_test(backend, server_cpuset, server_async_worker_cpuset, client_cpuset, client_async_worker_cpuset)

        server_stdout_output_path: Path = self.run_output_dir / (output_filename + ".server.out")

        with open(server_stdout_output_path, "w") as f:
            print(server.stdout, file=f)

        server_stderr_output_path: Path = self.run_output_dir / (output_filename + ".server.err")

        with open(server_stderr_output_path, "w") as f:
            print(server.stderr, file=f)

        client_stdout_output_path: Path = self.run_output_dir / (output_filename + ".client.out")

        with open(client_stdout_output_path, "w") as f:
            print(client.stdout, file=f)

        client_stderr_output_path: Path = self.run_output_dir / (output_filename + ".client.err")

        with open(client_stderr_output_path, "w") as f:
            print(client.stderr, file=f)

        if (err := server.returncode) != 0:
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
