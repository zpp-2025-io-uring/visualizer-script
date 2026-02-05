import pytest

from metadata import BACKENDS_NAMES
from test.output import dump_fake_output_to_file, generate_fake_benchmark_results, generate_fake_output
from test.smoketests.benchmark_should import (
    BenchmarkShould,
    assert_files,
    get_expected_files_for_metrics_per_run_sharded,
    get_expected_files_for_metrics_per_run_shardless,
)

SHARDED_METRICS_PATHS = [
    ["messages", "per second"],
    ["messages", "count"],
    ["throughput"],
]

SHARDLESS_METRICS_PATHS = [["shardless", "nested", "metric"], ["another", "shardless"], ["final"]]


def test_redraw(invoke_main, tmp_path_factory):
    # Arrange
    dir_with_files = tmp_path_factory.mktemp("redraw_test_files")

    shards_count = 4
    seed_start = 1000

    backend_results_paths = {}
    for backend in BACKENDS_NAMES:
        results = generate_fake_output(
            shards_count=shards_count,
            sharded_metrics=SHARDED_METRICS_PATHS,
            shardless_metrics=SHARDLESS_METRICS_PATHS,
            seed=seed_start,
        )
        seed_start += 1
        file_path = dir_with_files / f"{backend}.client.out"
        backend_results_paths[backend] = file_path
        dump_fake_output_to_file(results, file_path)

    output_dir = tmp_path_factory.mktemp("redraw_test_output")

    args_paths = []
    for backend in BACKENDS_NAMES:
        args_paths.extend([f"--{backend}", str(backend_results_paths[backend])])

    # Act
    _, _ = invoke_main(
        [
            "redraw",
            *args_paths,
            "--output-dir",
            str(output_dir),
        ]
    )

    # Assert
    print("Checking generated files in:", output_dir)
    expected_files_for_sharded = get_expected_files_for_metrics_per_run_sharded(SHARDED_METRICS_PATHS)
    assert_files(output_dir, expected_files_for_sharded)

    expected_files_for_shardless = get_expected_files_for_metrics_per_run_shardless(SHARDLESS_METRICS_PATHS)
    assert_files(output_dir, expected_files_for_shardless)

    assert "--linux-aio" in args_paths  # just to use the variable and avoid linter warning


@pytest.mark.parametrize("suite_name, runs_count", [("rpc_vecho", 3), ("rpc_64kB_stream_unidirectional", 2)])
def test_redraw_suite(invoke_main, tmp_path, suite_name: str, runs_count: int):
    dir_with_files = tmp_path

    backends = generate_fake_benchmark_results(
        dir_with_files, suite_name, runs_count, SHARDED_METRICS_PATHS, SHARDLESS_METRICS_PATHS
    )

    print("Generated test suite files in:", dir_with_files)

    # Act
    _, _ = invoke_main(["redraw_suite", "--dir", str(dir_with_files)])

    # Assert
    benchmark_should = BenchmarkShould(
        output_dir=dir_with_files,
        backends=backends,
        sharded_metrics=SHARDED_METRICS_PATHS,
        shardless_metrics=SHARDLESS_METRICS_PATHS,
    )
    benchmark_should.verify_media_for_benchmarks(
        benchmarks=[{"name": suite_name, "iterations": runs_count}],
        generate_graphs=True,
        generate_summary_graphs=True,
        generate_pdf=False,  # PDF generation is not part of redraw_suite
    )
