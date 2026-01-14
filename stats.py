import statistics
from collections.abc import Iterable

from yamlable import YamlAble, yaml_info


def join_sharded_metrics(sharded_data_points: dict):
    """Aggregate sharded data points into metric -> backend -> {shard: value}.

    The expected input shape is a mapping: backend -> { (shard, *path): value }
    where keys are tuples whose first element is the shard index. This helper
    converts the tuple path into a string metric name (using
    `generate_metric_name_from_path`) and groups values by backend and shard.

    Returns:
        dict mapping metric_name -> backend -> { shard_index: value, ... }
    """

    metrics = {}

    for backend, data in sharded_data_points.items():
        for key, val in data.items():
            shard = key[0]
            path = generate_metric_name_from_path(key[1:])
            if path not in metrics:
                metrics[path] = {}
            if backend not in metrics[path]:
                metrics[path][backend] = {}
            metrics[path][backend][shard] = val

    return metrics


def join_shardless_metrics(shardless_data_points: dict):
    """Aggregate shardless data points into metric -> backend -> value.

    The expected input shape is a mapping: backend -> { (path,): value } (note
    the path is a tuple of path components). This helper converts the tuple
    path into a string metric name (using `generate_metric_name_from_path`) and
    groups values by backend.

    Returns:
        dict mapping metric_name -> backend -> value
    """

    metrics = {}

    for backend, data in shardless_data_points.items():
        for key, val in data.items():
            path = generate_metric_name_from_path(key)
            if path not in metrics:
                metrics[path] = {}
            if backend not in metrics[path]:
                metrics[path][backend] = {}
            # store raw (non-sharded) value directly for the backend
            metrics[path][backend] = val

    return metrics


def generate_metric_name_from_path(path: tuple) -> str:
    return "_".join(str(p) for p in path)


def join_metrics(backends_parsed: dict) -> tuple[dict, dict]:
    """Merge sharded and shardless metrics produced per-backend.

    Expects `backends_parsed` to be a mapping: backend -> (shardless_dict, sharded_dict)
    where each of those dicts maps path-tuples (or path tuples with shard as first
    element for sharded) to values.

    Returns: (tuple) of (shardless_metrics, sharded_metrics)
    """

    shardless_all = {backend: parsed[0] for backend, parsed in backends_parsed.items()}
    sharded_all = {backend: parsed[1] for backend, parsed in backends_parsed.items()}

    shardless_metrics = join_shardless_metrics(shardless_all)
    sharded_metrics = join_sharded_metrics(sharded_all)

    return (shardless_metrics, sharded_metrics)


def join_stats(metrics_runs: list[dict]) -> tuple[dict, dict]:
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

    sharded_out: dict = {}
    shardless_out: dict = {}

    for run in metrics_runs:
        run_id = run.get("run_id")

        # sharded metrics: iterate over metrics and backends and record each shard as a run entry
        for metric_name, backend_map in (run.get("sharded") or {}).items():
            if metric_name not in sharded_out:
                sharded_out[metric_name] = {}

            for backend, shard_map in backend_map.items():
                if backend not in sharded_out[metric_name]:
                    sharded_out[metric_name][backend] = []

                for shard, value in shard_map.items():
                    sharded_out[metric_name][backend].append({"run_id": run_id, "shard": shard, "value": value})

        # shardless metrics: record single value per run per backend
        for metric_name, backend_map in (run.get("shardless") or {}).items():
            if metric_name not in shardless_out:
                shardless_out[metric_name] = {}

            for backend, value in backend_map.items():
                if backend not in shardless_out[metric_name]:
                    shardless_out[metric_name][backend] = []

                shardless_out[metric_name][backend].append({"run_id": run_id, "value": value})

    return (sharded_out, shardless_out)


_SAMPLES_FOR_STDEV_AND_VARIANCE = 2


def compute_stats(samples: Iterable[object]):
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
    def __init__(self, sharded_metrics: dict = None, shardless_metrics: dict = None):
        self.sharded_metrics = sharded_metrics or {}
        self.shardless_metrics = shardless_metrics or {}

    def get_sharded_metrics(self) -> dict:
        return self.sharded_metrics

    def get_shardless_metrics(self) -> dict:
        return self.shardless_metrics


def summarize_stats(sharded_metrics: dict, shardless_metrics: dict) -> Stats:
    # gather sharded values: metric -> backend -> shard -> [values]
    sharded_stats = {}
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
    shardless_stats = {}
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
