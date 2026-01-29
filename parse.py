from collections.abc import Callable
from typing import Any

from yaml import safe_load

from tree import TreeDict


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

    yaml_part = raw_output.split("---\n")[1]
    yaml_part = yaml_part.removesuffix("...\n")
    return safe_load(yaml_part)


SHARD_KEY = "shard"


def auto_generate_data_points(
    backend_data: list[dict],
) -> tuple[TreeDict[Any], TreeDict[dict[int, Any]]]:
    """
    Parse backend data and separate sharded and shardless metrics.
    Expects a list of dicts, where each dict represents either a sharded
    metric (with a 'shard' key) or a shardless metric (without a 'shard' key).

    Returns:
        A tuple containing two TreeDict objects:
            - The first TreeDict contains shardless metrics - values directly.
            - The second TreeDict contains sharded metrics - values per shard.
    """

    sharded_data_points: TreeDict[dict[int, Any]] = TreeDict()
    shardless_data_points: TreeDict[Any] = TreeDict()
    for el in backend_data:
        if SHARD_KEY in el:
            shard_val = el[SHARD_KEY]

            def put_sharded_value(path: tuple[str, ...], value: Any) -> None:
                sharded_data_points.setdefault(path, {})[shard_val] = value

            _walk_tree(el, put_sharded_value)
        else:

            def put_shardless_value(path: tuple[str, ...], value: Any) -> None:
                shardless_data_points[path] = value

            _walk_tree(el, put_shardless_value)

    return (shardless_data_points, sharded_data_points)


def _walk_tree(data: dict, put_value: Callable[[tuple[str, ...], Any], None], path: tuple[str, ...] = ()) -> None:
    """
    Recursively walk the data dict and populate the metric_tree with values.
    Uses the put_value callback to insert values into the metric_tree.

    Basically walk the tree, populate the same branch in the metric_tree,
    and when a leaf is reached, use put_value to insert the value.

    We skip the 'shard' key when traversing, so it doesn't appear in the path.
    """
    for child_key, val in data.items():
        # skip traversing the 'shard' key so it won't appear in the path
        if child_key == SHARD_KEY:
            continue
        if isinstance(val, dict):
            _walk_tree(val, put_value, path + (child_key,))
        else:
            put_value(path + (child_key,), val)


def swap_backend_and_metric_path(data_points: dict[str, TreeDict[Any]]) -> TreeDict[dict[str, Any]]:
    """Aggregate data points into metric -> backend -> value.

    The expected input shape is a mapping: backend -> { metric..path -> value }.

    Note that it both works for sharded and shardless data points, as the key
    values are either `metric_value` or dict[int, metric_value].

    Returns:
        dict mapping metric..path -> backend -> value
    """

    metrics: TreeDict[dict[str, Any]] = TreeDict()

    for backend, data in data_points.items():
        for metric_path, val in data.items():
            metrics.setdefault(metric_path, {})[backend] = val
    return metrics


def join_metrics(
    backends_parsed: dict[str, tuple[TreeDict[Any], TreeDict[dict[int, Any]]]],
) -> tuple[TreeDict[dict[str, Any]], TreeDict[dict[str, dict[int, Any]]]]:
    """Merge sharded and shardless metrics produced per-backend."""

    shardless_all = {backend: parsed[0] for backend, parsed in backends_parsed.items()}
    sharded_all = {backend: parsed[1] for backend, parsed in backends_parsed.items()}

    shardless_metrics = swap_backend_and_metric_path(shardless_all)
    sharded_metrics = swap_backend_and_metric_path(sharded_all)

    return (shardless_metrics, sharded_metrics)
