import statistics
from collections.abc import Iterable
from typing import Any

from yamlable import YamlAble, yaml_info

from tree import TreeDict


class ShardlessMetricRunMeasurement:
    def __init__(self, run_id: int, value: Any):
        self.run_id = run_id
        self.value = value

    def __repr__(self) -> str:
        return f"ShardlessMetricRunMeasurement(run_id={self.run_id}, value={self.value})"


class ShardedMetricRunMeasurement:
    def __init__(self, run_id: int, shard: int, value: Any):
        self.run_id = run_id
        self.value = value
        self.shard = shard

    def __repr__(self) -> str:
        return f"ShardedMetricRunMeasurement(run_id={self.run_id}, shard={self.shard}, value={self.value})"


def join_stats(
    metrics_runs: list[dict],
) -> tuple[
    TreeDict[dict[str, list[ShardedMetricRunMeasurement]]], TreeDict[dict[str, list[ShardlessMetricRunMeasurement]]]
]:
    """Aggregate per-run metrics into a run-oriented structure.

    Expected input: list of dicts with keys:
        - 'run_id': arbitrary run identifier (int or str)
        - 'sharded': mapping metric -> backend -> { shard: value }
        - 'shardless': mapping metric -> backend -> value
    """

    sharded_out: TreeDict[dict[str, list[ShardedMetricRunMeasurement]]] = TreeDict()
    shardless_out: TreeDict[dict[str, list[ShardlessMetricRunMeasurement]]] = TreeDict()

    for run in metrics_runs:
        run_id = run["run_id"]
        if run_id is None:
            raise ValueError(f"Missing run_id in run entry: {run}")

        # sharded metrics: iterate over metrics and backends and record each shard as a run entry
        for metric_name, backend_map in run["sharded"].items():
            if metric_name not in sharded_out:
                sharded_out.setdefault(metric_name, {})

            for backend, shard_map in backend_map.items():
                sharded_out[metric_name].setdefault(backend, [])
                for shard, value in shard_map.items():
                    sharded_out[metric_name][backend].append(ShardedMetricRunMeasurement(run_id, shard, value))

        # shardless metrics: record single value per run per backend
        for metric_name, backend_map in run["shardless"].items():
            shardless_out.setdefault(metric_name, {})

            for backend, value in backend_map.items():
                shardless_out[metric_name].setdefault(backend, [])
                shardless_out[metric_name][backend].append(ShardlessMetricRunMeasurement(run_id, value))

    return (sharded_out, shardless_out)


_SAMPLES_FOR_STDEV_AND_VARIANCE = 2


def compute_stats(samples: Iterable[Any]):
    nums = []
    for s in samples:
        try:
            nums.append(float(s))
        except Exception:
            # skip non-numeric values
            continue

    if not nums:
        return None

    stats = {}
    stats["min"] = min(nums)
    stats["max"] = max(nums)
    stats["mean"] = statistics.mean(nums)
    stats["median"] = statistics.median(nums)
    stats["range"] = stats["max"] - stats["min"]
    if len(nums) >= _SAMPLES_FOR_STDEV_AND_VARIANCE:
        stats["stdev"] = statistics.stdev(nums)
        stats["variance"] = statistics.variance(nums)
    else:
        stats["stdev"] = 0.0
        stats["variance"] = 0.0

    return stats


@yaml_info("stats")
class Stats(YamlAble):
    def __init__(
        self, sharded_metrics: TreeDict[dict[str, dict[int, Any]]], shardless_metrics: TreeDict[dict[str, Any]]
    ):
        self.sharded_metrics = sharded_metrics
        self.shardless_metrics = shardless_metrics

    def get_sharded_metrics(self) -> TreeDict[dict[str, dict[int, Any]]]:
        return self.sharded_metrics

    def get_shardless_metrics(self) -> TreeDict[dict[str, Any]]:
        return self.shardless_metrics

    def __repr__(self) -> str:
        return f"Stats(sharded_metrics={self.sharded_metrics}, shardless_metrics={self.shardless_metrics})"


def summarize_stats(
    sharded_metrics: TreeDict[dict[str, list[ShardedMetricRunMeasurement]]],
    shardless_metrics: TreeDict[dict[str, list[ShardlessMetricRunMeasurement]]],
) -> Stats:
    sharded_stats: TreeDict[dict[str, dict[int, Any]]] = __summarize_sharded_stats(sharded_metrics)
    shardless_stats: TreeDict[dict[str, Any]] = __summarize_shardless_stats(shardless_metrics)
    return Stats(sharded_stats, shardless_stats)


def __summarize_sharded_stats(
    sharded_metrics: TreeDict[dict[str, list[ShardedMetricRunMeasurement]]],
) -> TreeDict[dict[str, dict[int, Any]]]:
    summarized: TreeDict[dict[str, dict[int, Any]]] = TreeDict()

    for metric_name, backends in sharded_metrics.items():
        summarized.setdefault(metric_name, {})
        for backend_name, items in backends.items():
            summarized[metric_name].setdefault(backend_name, {})
            shard_map: dict[int, list[Any]] = summarized[metric_name][backend_name]
            for item in items:
                shard_map.setdefault(item.shard, [])
                shard_map[item.shard].append(item.value)

    for metric_name, backends in summarized.items():
        for backend_name, shard_map in backends.items():
            for shard, samples in shard_map.items():
                shard_map[shard] = compute_stats(samples)

    return summarized


def __summarize_shardless_stats(
    shardless_metrics: TreeDict[dict[str, list[ShardlessMetricRunMeasurement]]],
) -> TreeDict[dict[str, Any]]:
    summarized: TreeDict[dict[str, Any]] = TreeDict()

    for metric_name, backends in shardless_metrics.items():
        summarized.setdefault(metric_name, {})
        for backend_name, items in backends.items():
            summarized[metric_name].setdefault(backend_name, [])
            for item in items:
                summarized[metric_name][backend_name].append(item.value)

    for metric_name, backends in summarized.items():
        for backend_name, samples in backends.items():
            backends[backend_name] = compute_stats(samples)

    return summarized
