import subprocess
from pathlib import Path
from time import sleep

from log import get_logger, warn_if_not_release
from parse import RawBackendData, load_data
from remote import CmdOutput, Remote, RemoteProcess, RpcTesterParams

logger = get_logger()


class RpcTestRunner:
    def __init__(
        self,
        rpc_runner_config: dict,
        config_path: Path,
        run_output_dir: Path,
        skip_async_workers_cpuset: bool,
    ) -> None:
        self.tester_path: Path = Path(rpc_runner_config["tester_path"]).expanduser().resolve()
        self.config_path: Path = config_path.resolve()
        self.run_output_dir: Path = run_output_dir.resolve()
        self.asymmetric_server_app_cpuset = rpc_runner_config["asymmetric_server_app_cpuset"]
        self.asymmetric_server_async_worker_cpuset = rpc_runner_config["asymmetric_server_async_worker_cpuset"]
        self.symmetric_server_cpuset = rpc_runner_config["symmetric_server_cpuset"]
        self.asymmetric_client_app_cpuset = rpc_runner_config["asymmetric_client_app_cpuset"]
        self.asymmetric_client_async_worker_cpuset = rpc_runner_config["asymmetric_client_async_worker_cpuset"]
        self.symmetric_client_cpuset = rpc_runner_config["symmetric_client_cpuset"]
        self.skip_async_workers_cpuset = skip_async_workers_cpuset
        if (server_remote := rpc_runner_config.get("server_remote", None)) is not None:
            server_remote = Remote(server_remote)
        self.server_remote: Remote | None = server_remote
        if (client_remote := rpc_runner_config.get("client_remote", None)) is not None:
            client_remote = Remote(client_remote)
        self.client_remote: Remote | None = client_remote
        self.remote_listen_address: str = rpc_runner_config["remote_listen_address"]
        self.remote_listen_port: str = rpc_runner_config["remote_listen_port"]
        self.remote_connect_address: str = rpc_runner_config["remote_connect_address"]
        self.remote_connect_port: str = rpc_runner_config["remote_connect_port"]
        self.extra_server_options: list[str] = rpc_runner_config.get("extra_server_options", [])
        self.extra_client_options: list[str] = rpc_runner_config.get("extra_client_options", [])

        warn_if_not_release(self.tester_path)

    def __run_server(
        self, backend: str, server_cpuset: str, server_async_worker_cpuset: str | None
    ) -> subprocess.Popen[str] | RemoteProcess:  # Creates a process
        opts_argv = [
            "--listen", self.remote_listen_address,
            "--port", self.remote_listen_port,
            "--reactor-backend", backend,
            "--cpuset", server_cpuset,
        ] + self.extra_server_options

        if server_async_worker_cpuset is not None:
            opts_argv.extend(["--async-workers-cpuset", server_async_worker_cpuset])

        if self.server_remote is None:
            argv = [
                self.tester_path,
                "--conf",
                self.config_path,
            ] + opts_argv


            return subprocess.Popen(
                argv,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
        else:
            with open(self.config_path) as f:
                if self.remote_listen_address is None:
                    raise RuntimeError("Remote listen address not specified")
                if self.remote_listen_port is None:
                    raise RuntimeError("Remote listen port not specified")
                assert isinstance(self.remote_listen_address, str)
                assert isinstance(self.remote_listen_port, str)
                return self.server_remote.run_rpc_tester(
                    RpcTesterParams(
                        f.read(),
                        opts_argv
                    )
                )

    def __run_client(self, backend: str, client_cpuset: str, client_async_worker_cpuset: str | None) -> CmdOutput:
        opts_argv = [
            "--connect", self.remote_connect_address,
            "--port", self.remote_connect_port,
            "--reactor-backend", backend,
            "--cpuset", client_cpuset,
        ] + self.extra_client_options

        if client_async_worker_cpuset is not None:
            opts_argv.extend(["--async-workers-cpuset", client_async_worker_cpuset])

        if self.client_remote is None:
            argv = [
                self.tester_path,
                "--conf",
                self.config_path,
            ] + opts_argv

            output = subprocess.run(
                argv,
                check=False,
                capture_output=True,
                text=True,
            )

            return CmdOutput(stdout=output.stdout, stderr=output.stderr, returncode=output.returncode)
        else:
            with open(self.config_path) as f:
                if self.remote_connect_address is None:
                    raise RuntimeError("Remote connect address not specified")
                if self.remote_connect_port is None:
                    raise RuntimeError("Remote connect port not specified")
                assert isinstance(self.remote_connect_address, str)
                assert isinstance(self.remote_connect_port, str)
                return self.client_remote.run_rpc_tester(
                    RpcTesterParams(
                        f.read(),
                        opts_argv
                    )
                ).wait()

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
            assert isinstance(server_process, subprocess.Popen)
            server_stdout, server_stderr = server_process.communicate()
            return client_output, CmdOutput(
                stdout=server_stdout, stderr=server_stderr, returncode=server_process.poll()
            )
        else:
            assert isinstance(server_process, RemoteProcess)
            return client_output, server_process.wait()

    def __run_test(
        self,
        backend: str,
        output_filename: str,
        server_cpuset: str,
        server_async_worker_cpuset: str | None,
        client_cpuset: str,
        client_async_worker_cpuset: str | None,
    ) -> RawBackendData:
        logger.info(
            f"Running rpc_tester with backend {backend}, server cpuset: {server_cpuset}, server async worker cpuset: {server_async_worker_cpuset}, client cpuset: {client_cpuset}, client async worker cpuset: {client_async_worker_cpuset}"
        )
        self.run_output_dir.mkdir(parents=True, exist_ok=True)

        client, server = self.___run_test(
            backend, server_cpuset, server_async_worker_cpuset, client_cpuset, client_async_worker_cpuset
        )

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

        if server.returncode is not None and server.returncode != 0:
            raise RuntimeError(f"Server failed with exit code {server.returncode}")

        if client.returncode is not None and client.returncode != 0:
            raise RuntimeError(f"Client failed with exit code {client.returncode}")

        return load_data(client.stdout)

    def run(self, backend: str) -> RawBackendData:
        if backend == "asymmetric_io_uring":
            if self.skip_async_workers_cpuset:
                return self.__run_test(
                    backend,
                    backend,
                    self.asymmetric_server_app_cpuset,
                    None,
                    self.asymmetric_client_app_cpuset,
                    None,
                )
            else:
                return self.__run_test(
                    backend,
                    backend,
                    self.asymmetric_server_app_cpuset,
                    self.asymmetric_server_async_worker_cpuset,
                    self.asymmetric_client_app_cpuset,
                    self.asymmetric_client_async_worker_cpuset,
                )
        else:
            return self.__run_test(
                backend, backend, self.symmetric_server_cpuset, None, self.symmetric_client_cpuset, None
            )


def run_rpc_test(
    rpc_runner_config: dict, config_path: Path, run_output_dir: Path, backend: str, skip_async_workers_cpuset: bool
) -> RawBackendData:
    return RpcTestRunner(rpc_runner_config, config_path, run_output_dir, skip_async_workers_cpuset).run(backend)
