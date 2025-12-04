import argparse
import subprocess
from pathlib import Path
from generate import generate_graphs
from os import cpu_count
from time import sleep

class rpc_test_runner:
    def __init__(self, tester_path: Path, config_path: Path, output_dir: Path, ip_address: str, server_cpuset: str, client_cpuset: str):
        self.tester_path: Path = tester_path.resolve()
        self.config_path: Path = config_path.resolve()
        self.output_dir: Path = output_dir.resolve()
        self.ip_address = ip_address
        self.server_cpuset = server_cpuset
        self.client_cpuset = client_cpuset

    def __run_test(self, backend: str, output_filename: str):
        print(f"Running rpc_tester with backend {backend}")
        self.output_dir.mkdir(parents=True, exist_ok=True)

        server_process = subprocess.Popen(
            [self.tester_path, "--conf", self.config_path, "--listen", self.ip_address, "--reactor-backend", backend, "--cpuset", self.server_cpuset],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        sleep(1)

        client = subprocess.run(
            [self.tester_path, "--conf", self.config_path, "--connect", self.ip_address, "--reactor-backend", backend, "--cpuset", self.client_cpuset],
            capture_output=True,
            text=True,
        )

        server_process.terminate()

        sleep(1)

        if server_process.poll() is None:
            server_process.kill()

        server_process.wait()

        server_stdout, server_stderr = server_process.communicate()

        server_stdout_output_path: Path = self.output_dir / (output_filename + ".server.out")

        with open(server_stdout_output_path, "w") as f:
            print(server_stdout, file=f)

        server_stderr_output_path: Path = self.output_dir / (output_filename + ".server.err")

        with open(server_stderr_output_path, "w") as f:
            print(server_stderr, file=f)

        client_stdout_output_path: Path = self.output_dir / (output_filename + ".client.out")

        with open(client_stdout_output_path, "w") as f:
            print(client.stdout, file=f)

        client_stderr_output_path: Path = self.output_dir / (output_filename + ".client.err")

        with open(client_stderr_output_path, "w") as f:
            print(client.stderr, file=f)

        if (err := server_process.returncode) != 0:
            raise RuntimeError(f"Server failed with exit code {err}")
        
        if (err := client.returncode != 0):
            raise RuntimeError(f"Client failed with exit code {err}")

        return client.stdout

    def run(self):
        asymmetric_data = self.__run_test("asymmetric_io_uring", "asymmetric")
        symmetric_data = self.__run_test("io_uring", "symmetric")
        print("Generating graphs")
        generate_graphs(asymmetric_data, symmetric_data, self.output_dir)

def run_rpc_test(tester_path, config_path, output_dir, ip_address, server_cpuset, client_cpuset):
    rpc_test_runner(Path(tester_path), Path(config_path), Path(output_dir), ip_address, server_cpuset, client_cpuset).run()

def run_rpc_test_args(args):
    run_rpc_test(args.tester, args.config, args.output_dir, args.ip, args.server_cpuset, args.client_cpuset)

def configure_run_rpc_parser(parser: argparse.ArgumentParser):
    cpus = cpu_count()
    server_cpus = f"0-{int(cpus/2)-1}"
    client_cpus = f"{int(cpus/2)}-{cpus-1}"

    parser.add_argument("--tester", help="path to rpc_tester", required=True)
    parser.add_argument("--config", help="path to configuration .yaml file", required=True)
    parser.add_argument("--output-dir", help="directory to save the output to", required=True)
    parser.add_argument("--ip", help="ip address to connect on", default="127.0.0.5")
    parser.add_argument("--server-cpuset", help="cpuset for the server", default=server_cpus)
    parser.add_argument("--client-cpuset", help="cpuset for the client", default=client_cpus)
    parser.set_defaults(func=run_rpc_test_args)
    parser.formatter_class = argparse.ArgumentDefaultsHelpFormatter
