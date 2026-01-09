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

def compute_benchmark_summary(sharded_metrics: dict, shardless_metrics: dict, benchmark_info: dict):
    """Write a single, non-duplicated metrics summary organized by runs.

    Output structure:
      benchmark: { id, path, properties }
      run_count: N
      runs:
        - id: <run id>
          properties: {}
          results:
            sharded_metrics:
              <metric>:
                properties: {}
                backends:
                  <backend>:
                    properties: {}
                    shards:
                      - shard: <shard>
                        value: <value>
            shardless_metrics:
              <metric>:
                properties: {}
                backends:
                  <backend>:
                    properties: {}
                    value: <value>

    This function consumes the shape produced by `join_stats`:
      sharded_metrics: metric -> backend -> [ {run_id, shard, value}, ... ]
      shardless_metrics: metric -> backend -> [ {run_id, value}, ... ]

    The resulting YAML avoids duplicated metric-centric lists and instead
    groups values under each run, which makes it natural to extend run
    properties in the future.
    """
    # build map run_id -> run entry
    runs_map: dict = {}

    # process sharded metrics
    for metric_name, backends in (sharded_metrics or {}).items():
        for backend_name, items in backends.items():
            for item in items:
                run_id = item.get('run_id')
                shard = item.get('shard')
                value = item.get('value')

                if run_id not in runs_map:
                    runs_map[run_id] = {'id': run_id, 'properties': {}, 'results': {'sharded_metrics': {}, 'shardless_metrics': {}}}

                run_entry = runs_map[run_id]
                sharded_metrics_for_run = run_entry['results']['sharded_metrics']
                if metric_name not in sharded_metrics_for_run:
                    sharded_metrics_for_run[metric_name] = {'properties': {}, 'backends': {}}

                backends_for_metric = sharded_metrics_for_run[metric_name]['backends']
                if backend_name not in backends_for_metric:
                    backends_for_metric[backend_name] = {'properties': {}, 'shards': []}

                backends_for_metric[backend_name]['shards'].append({'shard': shard, 'value': value})

    # process shardless metrics
    for metric_name, backends in (shardless_metrics or {}).items():
        for backend_name, items in backends.items():
            for item in items:
                run_id = item.get('run_id')
                value = item.get('value')

                if run_id not in runs_map:
                    runs_map[run_id] = {'id': run_id, 'properties': {}, 'results': {'sharded_metrics': {}, 'shardless_metrics': {}}}

                run_entry = runs_map[run_id]
                shardless_metrics_for_run = run_entry['results']['shardless_metrics']
                if metric_name not in shardless_metrics_for_run:
                    shardless_metrics_for_run[metric_name] = {'properties': {}, 'backends': {}}

                backends_for_metric = shardless_metrics_for_run[metric_name]['backends']
                # for shardless, we store a single value per backend per run
                backends_for_metric[backend_name] = {'properties': {}, 'value': value}

    # prepare final summary
    runs_list = [runs_map[k] for k in sorted(runs_map.keys(), key=lambda x: (int(x) if isinstance(x, (int, str)) and str(x).isdigit() else str(x)))]
    summary = {
        'benchmark': benchmark_info,
        'run_count': len(runs_list),
        'runs': runs_list,
    }

    summary_stats = summarize_stats(sharded_metrics, shardless_metrics)
    summary['summary'] = summary_stats
    return summary
