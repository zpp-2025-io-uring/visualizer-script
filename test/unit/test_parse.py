from parse import auto_generate_data_points
from test.output import generate_fake_output


def test_auto_generate_data_points():
    sharded_metrics = [["sharded", "metric", "one"], ["metric", "sharded", "two"]]
    shardless_metrics = [["shardless", "metric", "one"], ["metric", "shardless", "two"]]

    shards_count = 3
    fake_output = generate_fake_output(
        shards_count=shards_count,
        sharded_metrics=sharded_metrics,
        shardless_metrics=shardless_metrics,
        seed=123,
        from_to=(10.0, 20.0),
    )

    (shardless_data_points, sharded_data_points) = auto_generate_data_points(fake_output)

    print("Shardless data points:", shardless_data_points)
    print("Sharded data points:", sharded_data_points)

    # Check sharded metrics
    for metric in sharded_metrics:
        value_per_shard = sharded_data_points[tuple(metric)]
        assert isinstance(value_per_shard, dict)
        for shard in range(shards_count):
            actual_value = value_per_shard.get(shard)
            assert actual_value is not None, f"Missing shard {shard} for sharded metric {metric}"
            expected_value = walk_tree(fake_output[shard], metric)
            assert actual_value == expected_value, (
                f"Value mismatch for sharded metric {metric}: expected {expected_value}, got {actual_value}"
            )

    # Check shardless metrics
    shardless_fake_output = fake_output[-1]
    for metric in shardless_metrics:
        actual_value = shardless_data_points[tuple(metric)]
        expected_value = walk_tree(shardless_fake_output, metric)
        assert actual_value == expected_value, (
            f"Value mismatch for shardless metric {metric}: expected {expected_value}, got {actual_value}"
        )


def walk_tree(data, path):
    for key in path:
        data = data[key]
    return data
