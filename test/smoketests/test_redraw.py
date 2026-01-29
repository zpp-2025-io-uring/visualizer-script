import pytest

from test.output import dump_fake_output_to_file, generate_fake_benchmark_results, generate_fake_output
from benchmarks import benchmark_summary_pdf_filename
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

    io_uring_results = generate_fake_output(
        shards_count=shards_count,
        sharded_metrics=SHARDED_METRICS_PATHS,
        shardless_metrics=SHARDLESS_METRICS_PATHS,
        seed=1234,
    )
    io_uring_path = dir_with_files / "io_uring.client.out"
    dump_fake_output_to_file(io_uring_results, io_uring_path)

    epoll_results = generate_fake_output(
        shards_count=shards_count,
        sharded_metrics=SHARDED_METRICS_PATHS,
        shardless_metrics=SHARDLESS_METRICS_PATHS,
        seed=5678,
    )
    epoll_path = dir_with_files / "epoll.client.out"
    dump_fake_output_to_file(epoll_results, epoll_path)

    aio_results = generate_fake_output(
        shards_count=shards_count,
        sharded_metrics=SHARDED_METRICS_PATHS,
        shardless_metrics=SHARDLESS_METRICS_PATHS,
        seed=91011,
    )
    aio_path = dir_with_files / "linux-aio.client.out"
    dump_fake_output_to_file(aio_results, aio_path)

    asymmetric_results = generate_fake_output(
        shards_count=shards_count,
        sharded_metrics=SHARDED_METRICS_PATHS,
        shardless_metrics=SHARDLESS_METRICS_PATHS,
        seed=121314,
    )
    asymmetric_path = dir_with_files / "asymmetric.client.out"
    dump_fake_output_to_file(asymmetric_results, asymmetric_path)

    output_dir = tmp_path_factory.mktemp("redraw_test_output")

    # Act
    _, _ = invoke_main(
        [
            "redraw",
            "--epoll",
            str(epoll_path),
            "--io_uring",
            str(io_uring_path),
            "--linux-aio",
            str(aio_path),
            "--asymmetric_io_uring",
            str(asymmetric_path),
            "--output-dir",
            str(output_dir),
            "--pdf",
        ]
    )

    # Assert
    print("Checking generated files in:", output_dir)
    expected_files_for_sharded = get_expected_files_for_metrics_per_run_sharded(SHARDED_METRICS_PATHS)
    assert_files(output_dir, expected_files_for_sharded)

    expected_files_for_shardless = get_expected_files_for_metrics_per_run_shardless(SHARDLESS_METRICS_PATHS)
    assert_files(output_dir, expected_files_for_shardless)

    pdf_file = output_dir / benchmark_summary_pdf_filename(output_dir.name)
    assert pdf_file.exists(), f"Expected PDF file {pdf_file} missing"


@pytest.mark.parametrize("suite_name, runs_count", [("rpc_vecho", 3), ("rpc_64kB_stream_unidirectional", 2)])
def test_redraw_suite(invoke_main, tmp_path, suite_name: str, runs_count: int):
    dir_with_files = tmp_path

    generate_fake_benchmark_results(
        dir_with_files, suite_name, runs_count, SHARDED_METRICS_PATHS, SHARDLESS_METRICS_PATHS
    )

    print("Generated test suite files in:", dir_with_files)

    # Act
    _, _ = invoke_main(["redraw_suite", "--dir", str(dir_with_files), "--pdf"])

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
        generate_pdf=True,
    )
