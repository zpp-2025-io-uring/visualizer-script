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

class Remote:
    def __init__(self, address: str):
        self.address = address

    def run_io_tester(self, config: str, backend: str, app_cpuset: str, async_worker_cpuset: str | None):
        params = {
            "config":config,
            "backend":backend,
            "app_cpuset":app_cpuset,
            "async_worker_cpuset":async_worker_cpuset
        }

        response =  requests.post(f"http://{self.address}/io_tester", json=params)

        if response.ok:
            return CmdOutput.from_json(response.json())
        else:
            logger.error(f"Remote failed with response {response.status_code}")
            return CmdOutput(None, None, response.status_code)
