from yaml import safe_load, safe_dump
from pathlib import Path

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
        return safe_load(yaml_part)

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
