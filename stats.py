
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