
def join_sharded_metrics(sharded_data_points: list[dict[dict[dict]]]):
    """Join sharded metrics from multiple runs."""

    summed_metrics = dict()

    for iteration in sharded_data_points:
        for metric_name, backend_map in iteration.items():
            if metric_name not in summed_metrics:
                summed_metrics[metric_name] = dict()

            for backend, shard_map in backend_map.items():
                if backend not in summed_metrics[metric_name]:
                    summed_metrics[metric_name][backend] = dict()

                for shard, value in shard_map.items():
                    if shard not in summed_metrics[metric_name][backend]:
                        summed_metrics[metric_name][backend][shard] = 0

                    summed_metrics[metric_name][backend][shard].append(value)

def join_shardless_metrics(metrics_list: list[dict[dict]]):
    """Join shardless metrics from multiple runs."""

    summed_metrics = dict()
    for iteration in metrics_list:
        for metric_name, backend_map in iteration.items():
            if metric_name not in summed_metrics:
                summed_metrics[metric_name] = dict()

            for backend, value in backend_map.items():
                if backend not in summed_metrics[metric_name]:
                    summed_metrics[metric_name][backend] = []

                summed_metrics[metric_name][backend].append(value)

    return summed_metrics

def join_stats(metrics_list: list[tuple[dict[dict[dict]], dict[dict]]]):
    """Join both sharded and shardless metrics from multiple runs."""

    sharded_list = [m[0] for m in metrics_list]
    shardless_list = [m[1] for m in metrics_list]

    joined_sharded = join_sharded_metrics(sharded_list)
    joined_shardless = join_shardless_metrics(shardless_list)

    return (joined_sharded, joined_shardless)