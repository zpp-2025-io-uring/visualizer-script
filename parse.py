"""Helpers to parse raw io-tester client output and produce metric maps.

This module provides two layers of helpers:
- low-level helpers that transform raw backend data into two shapes:
    - shardless data points: mapping from a path-tuple -> value
    - sharded data points: mapping from a path-tuple prefixed with shard -> value
- a single, small public API `join_metrics(backends_parsed)` that merges
    per-backend parsed data into a unified mapping suitable for plotting or
    aggregation.

Typical usage:
    backends_parsed = {
            'epoll': (shardless_dict, sharded_dict),
            'io_uring': (shardless_dict, sharded_dict),
    }

    metrics = join_metrics(backends_parsed)

The returned `metrics` mapping has the shape:
    metric_name -> backend -> value
where `value` is either a raw value (for shardless metrics) or a dict of
shard -> value for sharded metrics. If both a sharded and a shardless value
exist for the same backend/metric, the shardless value is stored under the
special key '_total' alongside shard keys.
"""

import yaml


def load_data(raw_output: str):
        """Extract embedded YAML from a client output text and parse it.

        The client output is expected to contain a YAML document separated by the
        standard YAML document markers. This function extracts the second section
        (the YAML payload), strips the trailing document terminator `...\n` and
        returns the parsed Python object.

        Args:
                raw_output: full stdout/stderr capture produced by a client run.

        Returns:
                A Python object loaded from the YAML payload (typically a list of
                dicts describing shards and metrics).
        """

        yaml_part = raw_output.split('---\n')[1]
        yaml_part = yaml_part.removesuffix("...\n")
        return yaml.safe_load(yaml_part)

def auto_generate_data_points(backend_data: dict) -> tuple[dict, dict]:
    """Generates all data points available in the data for automatic plotting.

    Walks the nested dicts and collects all data point paths and their values.
    For entries that include a top-level 'shard' key the returned mapping will
    include the shard as the first element of the key tuple: (shard, *path).
    For non-sharded entries keys are simply the path tuples: (path,).

    Returns a tuple: (shardless_data_points, sharded_data_points), where each
    value is a dict mapping tuple keys -> value.
    """

    shardless_data_points: dict[tuple, object] = dict()
    sharded_data_points: dict[tuple, object] = dict()

    def walk_tree(prefix, data):
        """Return list of (path_tuple, value) pairs for leaves under data.

        The returned path_tuple does NOT include any 'shard' key — caller
        handles including the shard value in the final key when needed.
        """
        if not isinstance(data, dict):
            return [(tuple(prefix), data)]

        result = []
        for key, val in data.items():
            # skip traversing the 'shard' key so it won't appear in the path
            if key == 'shard':
                continue
            result += walk_tree(prefix + [key], val)

        return result

    for el in backend_data:
        # handle non-dict or dict without shard
        if isinstance(el, dict) and 'shard' in el:
            shard_val = el['shard']
            for path, val in walk_tree([], el):
                # key includes shard as first element
                key = (shard_val,) + path
                sharded_data_points[key] = val
        else:
            for path, val in walk_tree([], el):
                shardless_data_points[path] = val

    return (shardless_data_points, sharded_data_points)

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

def generate_metric_name_from_path(path: tuple) -> str:
    return '_'.join(str(p) for p in path)