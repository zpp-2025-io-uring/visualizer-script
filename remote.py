from dataclasses import dataclass

import requests

from log import get_logger

logger = get_logger()

@dataclass
class CmdOutput:
    stdout: str
    stderr: str
    returncode: int

    def from_json(data: dict) -> "CmdOutput":
        return CmdOutput(stdout=data["stdout"], stderr=data["stderr"], returncode=data["return_code"])

class RemoteProcess:
    def __init__(self, remote: "Remote", pid: int):
        self.remote = remote
        self.pid = pid
    
    def wait(self) -> CmdOutput:
        response =  requests.post(f"http://{self.remote.address}/wait_and_output", json=self.pid)
        print(response.json())
        if response.ok:
            return CmdOutput.from_json(response.json())
        else:
            raise RuntimeError(f"Remote failed with response {response.status_code}")

    def kill(self):
        response =  requests.post(f"http://{self.remote.address}/kill", json=self.pid)
        if not response.ok:
            raise RuntimeError(f"Remote failed with response {response.status_code}")
        
    def terminate(self):
        response =  requests.post(f"http://{self.remote.address}/terminate", json=self.pid)
        if not response.ok:
            raise RuntimeError(f"Remote failed with response {response.status_code}")
        
    def poll(self) -> int:
        response =  requests.post(f"http://{self.remote.address}/poll", json=self.pid)
        if response.ok:
            return response.json()
        else:
            raise RuntimeError(f"Remote failed with response {response.status_code}")

class Remote:
    def __init__(self, address: str):
        self.address = address

    def run_io_tester(self, config: str, backend: str, app_cpuset: str, async_worker_cpuset: str | None) -> RemoteProcess:
        params = {
            "config":config,
            "backend":backend,
            "app_cpuset":app_cpuset,
            "async_worker_cpuset":async_worker_cpuset
        }

        response =  requests.post(f"http://{self.address}/io_tester", json=params)
        if response.ok:
            return RemoteProcess(remote=self, pid=response.json())
        else:
            raise RuntimeError(f"Remote failed with response {response.status_code}")

