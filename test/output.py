import random
from pathlib import Path
from typing import Any

from yaml import safe_dump

from benchmark import compute_benchmark_summary
from benchmarks import BENCHMARK_SUMMARY_FILENAME
from parse import auto_generate_data_points, join_metrics
from stats import join_stats


def generate_fake_output(
    shards_count: int,
    sharded_metrics: list[list[str]],
    shardless_metrics: list[list[str]] = None,
    seed: int = 42,
    from_to: tuple[float, float] = (0.0, 1000.0),
) -> list[dict]:
    """Generate a fake output dict with the results of of some benchmark.

    Args:
        shards_count: Number of shards to generate data for.
        sharded_metrics: Tree of metric names to include per shard.
        shardless_metrics: Tree of metric names to include without shards.
        seed: Random seed for reproducibility (default: 42).
        from_to: Tuple containing minimum and maximum values for metric values (default: (0.0, 1000.0)).

    Returns:
        A list of dicts representing the generated data.
    """
    random.seed(seed)
    output = []

    from_, to = from_to

    # Generate sharded metrics
    for shard in range(shards_count):
        shard_data = {}
        for group in sharded_metrics:
            value = random.uniform(from_, to)
            _recursive_metric(shard_data, group, value)
        shard_data["shard"] = shard
        output.append(shard_data)

    # Generate shardless metrics
    if shardless_metrics:
        shardless_data = {}
        for group in shardless_metrics:
            value = random.uniform(from_, to)
            _recursive_metric(shardless_data, group, value)
        output.append(shardless_data)

    return output


def _recursive_metric(target: dict, metric_path: list[str], value: Any) -> None:
    for key in metric_path[:-1]:
        target = target.setdefault(key, {})
    target[metric_path[-1]] = value


def dump_fake_output_to_file(output: list[dict], path: Path) -> None:
    """Dump the generated fake output to a YAML file.

    Args:
        output: The output list of dicts to dump.
        path: The file path to write the YAML data to.
    """
    output_str = safe_dump(output)
    output_wrapped_with_markers = f"---\n{output_str}...\n"
    with open(path, "w") as f:
        f.write(output_wrapped_with_markers)


def generate_fake_benchmark_results(
    output_dir: Path,
    suite_name: str,
    runs_count: int,
    sharded_metrics: list[list[str]],
    shardless_metrics: list[list[str]],
    backends: list[str],
) -> None:
    suite_path = output_dir / suite_name
    suite_path.mkdir(parents=True, exist_ok=True)
    metrics_runs = []

    for run_idx in range(runs_count):
        run_path = suite_path / f"run_{run_idx}"
        run_path.mkdir(parents=True, exist_ok=True)

        backends_results, _ = generate_fake_run_results(
            run_path, sharded_metrics, shardless_metrics, backends, shards_count=4, seed=run_idx * 1000 + 123
        )

        [shardless, sharded] = join_metrics(backends_results)
        metrics_runs.append({"run_id": run_idx, "sharded": sharded, "shardless": shardless})

    (combined_sharded, combined_shardless) = join_stats(metrics_runs)
    benchmark_info = {"id": suite_name, "properties": {"iterations": runs_count}}
    summary = compute_benchmark_summary(combined_sharded, combined_shardless, benchmark_info)
    print("Writing benchmark summary to:", suite_path / BENCHMARK_SUMMARY_FILENAME)
    print(summary)
    with open(suite_path / BENCHMARK_SUMMARY_FILENAME, "w") as f:
        f.write(safe_dump(summary))


def generate_fake_run_results(
    output_dir: Path,
    sharded_metrics: list[list[str]],
    shardless_metrics: list[list[str]],
    backends: list[str],
    shards_count: int,
    seed: int,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Generate fake results for a single run of a benchmark.

    Returns: A dict containing the generated results for each backend.
    """
    backends_results = {}
    backend_paths = {}
    for backend in backends:
        backend_results = generate_fake_output(
            shards_count=shards_count,
            sharded_metrics=sharded_metrics,
            shardless_metrics=shardless_metrics,
            seed=seed + hash(backend),
        )
        backend_file_path = output_dir / f"{backend}.client.out"
        dump_fake_output_to_file(backend_results, backend_file_path)
        backends_results[backend] = auto_generate_data_points(backend_results)
        backend_paths[backend] = backend_file_path

    return backends_results, backend_paths
