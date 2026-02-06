import pytest

from metadata import BACKENDS_NAMES
from test.output import generate_fake_benchmark_results, generate_fake_run_results
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
    _, backend_paths = generate_fake_run_results(
        dir_with_files,
        sharded_metrics=SHARDED_METRICS_PATHS,
        shardless_metrics=SHARDLESS_METRICS_PATHS,
        backends=BACKENDS_NAMES,
        shards_count=shards_count,
        seed=123,
    )

    output_dir = tmp_path_factory.mktemp("redraw_test_output")

    file_args = []
    for backend_name, backend_path in backend_paths.items():
        file_args.extend([f"--{backend_name}", str(backend_path)])

    # Act
    _, _ = invoke_main(
        [
            "redraw",
            *file_args,
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


@pytest.mark.parametrize("suite_name, runs_count", [("rpc_vecho", 3), ("rpc_64kB_stream_unidirectional", 2)])
def test_redraw_suite(invoke_main, tmp_path, suite_name: str, runs_count: int):
    dir_with_files = tmp_path

    generate_fake_benchmark_results(
        dir_with_files, suite_name, runs_count, SHARDED_METRICS_PATHS, SHARDLESS_METRICS_PATHS, BACKENDS_NAMES
    )

    print("Generated test suite files in:", dir_with_files)

    # Act
    _, _ = invoke_main(["redraw_suite", "--dir", str(dir_with_files)])

    # Assert
    benchmark_should = BenchmarkShould(
        output_dir=dir_with_files,
        backends=["io_uring"],
        sharded_metrics=SHARDED_METRICS_PATHS,
        shardless_metrics=SHARDLESS_METRICS_PATHS,
    )
    benchmark_should.verify_media_for_benchmarks(
        benchmarks=[{"name": suite_name, "iterations": runs_count}],
        generate_graphs=True,
        generate_summary_graphs=True,
        generate_pdf=False,  # PDF generation is not part of redraw_suite
    )
