import os
import stat
import subprocess
import textwrap
from pathlib import Path

import pytest
from yaml import safe_dump

from test.output import generate_fake_output
from test.smoketests.benchmark_should import (
    BenchmarkShould,
)


def _write_executable(path: Path, content: str):
    path.write_text(content)
    st = os.stat(path)
    os.chmod(path, st.st_mode | stat.S_IEXEC)


def generate_dummy_script(output: str, where_to_print_args: Path) -> str:
    return textwrap.dedent(f"""\
#!/usr/bin/env python3
import sys
print("client: running")
with open(r"{where_to_print_args}", "w") as f:
    f.write(' '.join(sys.argv[1:]))
    f.write('\\n')
    f.flush()
print("---")
print('''{output}''')
print("...")""")


sharded_metrics = [["latency", "p50"], ["latency", "p99"], ["throughput"]]
shardless_metrics = [["errors", "total"]]
shards_count = 3


def generate_simple_config(name: str, output_dir: Path, rpc_tester_path: Path, io_tester_path: Path) -> dict:
    cfg = {
        "config_version": 2,
        "output_dir": str(output_dir / name.replace(".yaml", "")),
        "params": {"skip_async_workers_cpuset": True},
        "backends": ["asymmetric_io_uring", "io_uring"],
        "io": {
            "tester_path": str(io_tester_path),
            "storage_dir": str(output_dir / "storage"),
            "asymmetric_app_cpuset": "0",
            "asymmetric_async_worker_cpuset": "1",
            "symmetric_cpuset": "0",
        },
        "rpc": {
            "tester_path": str(rpc_tester_path),
            "ip_address": "127.0.0.1",
            "asymmetric_server_app_cpuset": "0",
            "asymmetric_server_async_worker_cpuset": "1",
            "symmetric_server_cpuset": "0",
            "asymmetric_client_app_cpuset": "0",
            "asymmetric_client_async_worker_cpuset": "1",
            "symmetric_client_cpuset": "0",
        },
        "scylla": {},
    }
    return cfg


@pytest.mark.parametrize(
    "number_of_configs",
    [1, 2],
)
@pytest.mark.parametrize(
    "generate_options",
    [(g, s, p) for g in [True, False] for s in [True, False] for p in [True, False]],
    ids=[
        f"{'with' if g else 'without'}_graphs_{'with' if s else 'without'}_summary_{'with' if p else 'without'}_pdf"
        for g in [True, False]
        for s in [True, False]
        for p in [True, False]
    ],
)
def test_suite_runs_io_and_rpc(
    invoke_main, tmp_path, number_of_configs: int, generate_options: tuple[bool, bool, bool]
):
    # Arrange: create a temp base dir to hold tester executable and output
    base = tmp_path / "suite_test"
    base.mkdir()

    # create dummy executable
    tester = base / "dummy_tester.py"
    fake_output = generate_fake_output(
        shards_count=shards_count,
        sharded_metrics=sharded_metrics,
        shardless_metrics=shardless_metrics,
    )
    _write_executable(tester, generate_dummy_script(safe_dump(fake_output), base / "args.txt"))

    __prepare_env_dump_dir(base)

    # build config dict(s) and write to disk based on provided_configs
    configs_on_disk = []
    for i in range(number_of_configs):
        config_name = f"config_{i}.yaml"
        cfg = generate_simple_config(
            name=config_name,
            output_dir=base / "output",
            rpc_tester_path=tester,
            io_tester_path=tester,
        )

        cfg_path = base / config_name
        with open(cfg_path, "w") as f:
            f.write(safe_dump(cfg))
        configs_on_disk.append(
            {
                "path": cfg_path,
                "stem": cfg_path.stem,
                "content": cfg,
                "expected_output_dir": Path(cfg["output_dir"]),
            }
        )

    # create benchmark yaml
    suite = [
        {"type": "io", "name": "test_io", "iterations": 2, "config": {}},
        {"type": "rpc", "name": "test_rpc", "iterations": 1, "config": {}},
    ]

    benchmark_path = base / "suite.yaml"
    with open(benchmark_path, "w") as f:
        f.write(safe_dump(suite))

    config_args = [str(p["path"]) for p in configs_on_disk]
    invoke_args = ["suite", "--benchmark", str(benchmark_path), "--config"] + config_args

    generate_graphs, generate_summary_graphs, generate_pdf = generate_options
    if generate_graphs:
        invoke_args += ["--generate-graphs"]
    if generate_summary_graphs:
        invoke_args += ["--generate-summary-graphs"]
    if generate_pdf:
        invoke_args += ["--pdf"]

    # Act
    _, _ = invoke_main(invoke_args)

    # Assert
    timestamp_dir = None
    for config in configs_on_disk:
        timestamp_dir, out_dir = __assert_for_config(
            name=config["stem"],
            expected_out_dir=str(config["expected_output_dir"]),
            timestamp=timestamp_dir,
        )

        benchmark_should = BenchmarkShould(
            output_dir=out_dir,
            backends=config["content"]["backends"],
            sharded_metrics=sharded_metrics,
            shardless_metrics=shardless_metrics,
        )
        benchmark_should.verify_config_file(
            expected_config_name=config["path"].name, expected_content=config["content"]
        )
        benchmark_should.verify_suite_file(expected_content=suite)
        benchmark_should.verify_summary_files_exists_for_benchmarks(benchmarks=suite)
        benchmark_should.assert_dump_environment()
        benchmark_should.verify_outputs_for_benchmarks(
            benchmarks=suite,
        )
        benchmark_should.verify_media_for_benchmarks(
            benchmarks=suite,
            generate_graphs=generate_graphs,
            generate_summary_graphs=generate_summary_graphs,
            generate_pdf=generate_pdf,
        )


def __prepare_env_dump_dir(base: Path) -> Path:
    # initialize a git repo in base so dump_environment's git log succeeds
    subprocess.run(["git", "init"], cwd=base, check=True)
    subprocess.run(["git", "commit", "--allow-empty", "-m", "init"], cwd=base, check=True)
    return base


def __assert_for_config(name: str, expected_out_dir: str, timestamp: str | None) -> tuple[str, str]:
    base_path = Path(expected_out_dir)

    timestamps_dirs = [d for d in base_path.iterdir() if d.is_dir()]
    print(f"Found timestamp dirs: {[d.name for d in timestamps_dirs]} in {base_path}")
    assert len(timestamps_dirs) == 1, f"Expected one timestamp directory in {base_path}, found {len(timestamps_dirs)}"

    # Validate that there is only one timestamp dir per suite and get its name
    if timestamp:
        assert timestamps_dirs[0].name == timestamp, (
            f"Expected timestamp dir {timestamp}, found {timestamps_dirs[0].name}"
        )
    timestamp_dir = timestamps_dirs[0]

    config_dir = timestamp_dir / name
    assert config_dir.exists() and config_dir.is_dir(), (
        f"Config directory {config_dir} does not exist or is not a directory"
    )

    return timestamp_dir.name, str(config_dir)
