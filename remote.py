from dataclasses import dataclass

import requests

from log import get_logger

logger = get_logger()


@dataclass
class CmdOutput:
    stdout: str
    stderr: str
    returncode: int | None

    @staticmethod
    def from_json(data: dict) -> "CmdOutput":
        return CmdOutput(stdout=data["stdout"], stderr=data["stderr"], returncode=data["return_code"])


class RemoteProcess:
    def __init__(self, remote: "Remote", pid: int):
        self.remote = remote
        self.pid = pid

    def wait(self) -> CmdOutput:
        response = requests.post(f"http://{self.remote.address}/wait_and_output", json=self.pid)
        print(response.text)
        if response.ok:
            return CmdOutput.from_json(response.json())
        else:
            raise RuntimeError(f"Remote failed with response {response.status_code}")

    def kill(self) -> None:
        response = requests.post(f"http://{self.remote.address}/kill", json=self.pid)
        if not response.ok:
            raise RuntimeError(f"Remote failed with response {response.status_code}")

    def terminate(self) -> None:
        response = requests.post(f"http://{self.remote.address}/terminate", json=self.pid)
        if not response.ok:
            raise RuntimeError(f"Remote failed with response {response.status_code}")

    def poll(self) -> int | None:
        response = requests.post(f"http://{self.remote.address}/poll", json=self.pid)
        if response.ok:
            return response.json()
        else:
            raise RuntimeError(f"Remote failed with response {response.status_code}")


@dataclass
class IoTesterParams:
    config: str
    backend: str
    app_cpuset: str
    async_worker_cpuset: str | None

    def to_dict(self) -> dict:
        return {
            "config": self.config,
            "backend": self.backend,
            "app_cpuset": self.app_cpuset,
            "async_worker_cpuset": self.async_worker_cpuset,
        }


@dataclass
class RpcTesterParams:
    config: str
    backend: str
    ip_address: str
    port: str
    is_server: bool
    app_cpuset: str
    async_worker_cpuset: str | None

    def to_dict(self) -> dict:
        return {
            "config": self.config,
            "backend": self.backend,
            "ip_address": self.ip_address,
            "port": self.port,
            "is_server": self.is_server,
            "app_cpuset": self.app_cpuset,
            "async_worker_cpuset": self.async_worker_cpuset,
        }


class Remote:
    def __init__(self, address: str):
        self.address = address

    def run_io_tester(self, params: IoTesterParams) -> RemoteProcess:
        response = requests.post(f"http://{self.address}/io_tester", json=params.to_dict())
        if response.ok:
            return RemoteProcess(remote=self, pid=response.json())
        else:
            raise RuntimeError(f"Remote failed with response {response.status_code}")

    def run_rpc_tester(self, params: RpcTesterParams) -> RemoteProcess:
        response = requests.post(f"http://{self.address}/rpc_tester", json=params.to_dict())
        if response.ok:
            return RemoteProcess(remote=self, pid=response.json())
        else:
            print(response.text)
            raise RuntimeError(f"Remote failed with response {response.status_code}")
