import statistics
from typing import Iterable

def join_sharded_metrics(sharded_data_points: dict):
    """Aggregate sharded data points into metric -> backend -> {shard: value}.

    The expected input shape is a mapping: backend -> { (shard, *path): value }
    where keys are tuples whose first element is the shard index. This helper
    converts the tuple path into a string metric name (using
    `generate_metric_name_from_path`) and groups values by backend and shard.

    Returns:
        dict mapping metric_name -> backend -> { shard_index: value, ... }
    """

    metrics = dict()

    for backend in sharded_data_points:
        for key, val in sharded_data_points[backend].items():
            shard = key[0]
            path = generate_metric_name_from_path(key[1:])
            if path not in metrics:
                metrics[path] = dict()
            if backend not in metrics[path]:
                metrics[path][backend] = dict()
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

    metrics = dict()

    for backend in shardless_data_points:
        for key, val in shardless_data_points[backend].items():
            path = generate_metric_name_from_path(key)
            if path not in metrics:
                metrics[path] = dict()
            if backend not in metrics[path]:
                metrics[path][backend] = dict()
            # store raw (non-sharded) value directly for the backend
            metrics[path][backend] = val
    
    return metrics

def generate_metric_name_from_path(path: tuple) -> str:
    return '_'.join(str(p) for p in path)

def join_metrics(backends_parsed: dict):
    """Merge sharded and shardless metrics produced per-backend.

    Expects `backends_parsed` to be a mapping: backend -> (shardless_dict, sharded_dict)
    where each of those dicts maps path-tuples (or path tuples with shard as first
    element for sharded) to values.

    Returns: A tuple (shardless_metrics, sharded_metrics) where each value is a mapping:
        metric_name -> backend -> value-or-dict
    """

    shardless_all = {backend: parsed[0] for backend, parsed in backends_parsed.items()}
    sharded_all = {backend: parsed[1] for backend, parsed in backends_parsed.items()}

    shardless_metrics = join_shardless_metrics(shardless_all)
    sharded_metrics = join_sharded_metrics(sharded_all)

    return (shardless_metrics, sharded_metrics)

def join_stats(metrics_runs: list[dict]):
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
        run_id = run.get('run_id')

        # sharded metrics: iterate over metrics and backends and record each shard as a run entry
        for metric_name, backend_map in (run.get('sharded') or {}).items():
            if metric_name not in sharded_out:
                sharded_out[metric_name] = {}

            for backend, shard_map in backend_map.items():
                if backend not in sharded_out[metric_name]:
                    sharded_out[metric_name][backend] = []

                for shard, value in shard_map.items():
                    sharded_out[metric_name][backend].append({'run_id': run_id, 'shard': shard, 'value': value})

        # shardless metrics: record single value per run per backend
        for metric_name, backend_map in (run.get('shardless') or {}).items():
            if metric_name not in shardless_out:
                shardless_out[metric_name] = {}

            for backend, value in backend_map.items():
                if backend not in shardless_out[metric_name]:
                    shardless_out[metric_name][backend] = []

                shardless_out[metric_name][backend].append({'run_id': run_id, 'value': value})

    return (sharded_out, shardless_out)


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
    stats['min'] = min(nums)
    stats['max'] = max(nums)
    stats['mean'] = statistics.mean(nums)
    stats['median'] = statistics.median(nums)
    stats['range'] = stats['max'] - stats['min']
    if len(nums) >= 2:
        stats['stdev'] = statistics.stdev(nums)
        stats['variance'] = statistics.variance(nums)
    else:
        stats['stdev'] = 0.0
        stats['variance'] = 0.0

    return stats

def summarize_stats(sharded_metrics: dict, shardless_metrics: dict):
    summary_stats = {'sharded_metrics': {}, 'shardless_metrics': {}}

    # gather sharded values: metric -> backend -> shard -> [values]
    for metric_name, backends in (sharded_metrics or {}).items():
        summary_stats['sharded_metrics'].setdefault(metric_name, {})
        for backend_name, items in backends.items():
            summary_stats['sharded_metrics'][metric_name].setdefault(backend_name, {})
            for item in items:
                shard = item.get('shard')
                value = item.get('value')
                b = summary_stats['sharded_metrics'][metric_name][backend_name]
                b.setdefault(shard, [])
                b[shard].append(value)

    # compute stats for sharded
    for metric_name, backends in summary_stats['sharded_metrics'].items():
        for backend_name, shards in backends.items():
            for shard, samples in shards.items():
                shards[shard] = compute_stats(samples)

    # gather shardless values: metric -> backend -> [values]
    for metric_name, backends in (shardless_metrics or {}).items():
        summary_stats['shardless_metrics'].setdefault(metric_name, {})
        for backend_name, items in backends.items():
            summary_stats['shardless_metrics'][metric_name].setdefault(backend_name, [])
            for item in items:
                value = item.get('value')
                summary_stats['shardless_metrics'][metric_name][backend_name].append(value)

    # compute stats for shardless
    for metric_name, backends in summary_stats['shardless_metrics'].items():
        for backend_name, samples in backends.items():
            backends[backend_name] = compute_stats(samples)

    return summary_stats