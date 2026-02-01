import statistics
from collections.abc import Iterable
from typing import Any

from yamlable import YamlAble, yaml_info

from tree import TreeDict


def join_stats(metrics_runs: list[dict]) -> tuple[TreeDict[dict[str, list[dict]]], TreeDict[dict[str, list[dict]]]]:
    """Aggregate per-run metrics into a run-oriented structure.

    Expected input: list of dicts with keys:
        - 'run_id': arbitrary run identifier (int or str)
        - 'sharded': mapping metric -> backend -> { shard: value }
        - 'shardless': mapping metric -> backend -> value

    Returns a tuple (sharded, shardless) where:
        sharded: metric -> backend -> list of { 'run_id', 'shard', 'value' }
        shardless: metric -> backend -> list of { 'run_id', 'value' }

    This shape avoids duplicating aggregated lists per metric and keeps per-run
    information available for downstream consumers.
    """

    sharded_out: TreeDict[dict[str, list[dict]]] = TreeDict()
    shardless_out: TreeDict[dict[str, list[dict]]] = TreeDict()

    for run in metrics_runs:
        run_id = run.get("run_id")

        # sharded metrics: iterate over metrics and backends and record each shard as a run entry
        for metric_name, backend_map in run["sharded"].items():
            if metric_name not in sharded_out:
                sharded_out.setdefault(metric_name, {})

            for backend, shard_map in backend_map.items():
                if backend not in sharded_out[metric_name]:
                    sharded_out[metric_name][backend] = []

                for shard, value in shard_map.items():
                    sharded_out[metric_name][backend].append({"run_id": run_id, "shard": shard, "value": value})

        # shardless metrics: record single value per run per backend
        for metric_name, backend_map in run["shardless"].items():
            shardless_out.setdefault(metric_name, {})

            for backend, value in backend_map.items():
                if backend not in shardless_out[metric_name]:
                    shardless_out[metric_name][backend] = []

                shardless_out[metric_name][backend].append({"run_id": run_id, "value": value})

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
    sharded_metrics: TreeDict[dict[str, list[dict]]], shardless_metrics: TreeDict[dict[str, list[dict]]]
) -> Stats:
    # gather sharded values: metric -> backend -> shard -> [values]
    sharded_stats: TreeDict[dict[str, dict[int, Any]]] = TreeDict()
    for metric_name, backends in (sharded_metrics or {}).items():
        sharded_stats.setdefault(metric_name, {})
        for backend_name, items in backends.items():
            sharded_stats[metric_name].setdefault(backend_name, {})
            for item in items:
                shard = item.get("shard")
                value = item.get("value")
                b = sharded_stats[metric_name][backend_name]
                b.setdefault(shard, [])
                b[shard].append(value)

    # compute stats for sharded
    for metric_name, backends in sharded_stats.items():
        for backend_name, shards in backends.items():
            for shard, samples in shards.items():
                shards[shard] = compute_stats(samples)

    # gather shardless values: metric -> backend -> [values]
    shardless_stats: TreeDict[dict[str, Any]] = TreeDict()
    for metric_name, backends in (shardless_metrics or {}).items():
        shardless_stats.setdefault(metric_name, {})
        for backend_name, items in backends.items():
            shardless_stats[metric_name].setdefault(backend_name, [])
            for item in items:
                value = item.get("value")
                shardless_stats[metric_name][backend_name].append(value)

    # compute stats for shardless
    for metric_name, backends in shardless_stats.items():
        for backend_name, samples in backends.items():
            backends[backend_name] = compute_stats(samples)

    return Stats(sharded_metrics=sharded_stats, shardless_metrics=shardless_stats)
