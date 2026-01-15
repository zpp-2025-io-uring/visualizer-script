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

    # Check sharded metrics
    for metric in sharded_metrics:
        for shard in range(shards_count):
            key = (shard,) + tuple(metric)
            assert key in sharded_data_points
            value = sharded_data_points[key]

            expected_value = walk_tree(fake_output[shard], metric)
            assert value == expected_value, (
                f"Value mismatch for sharded metric {key}: expected {expected_value}, got {value}"
            )

    # Check shardless metrics
    for metric in shardless_metrics:
        key = tuple(metric)
        assert key in shardless_data_points
        value = shardless_data_points[key]

        expected_value = walk_tree(fake_output[-1], metric)
        assert value == expected_value, (
            f"Value mismatch for shardless metric {key}: expected {expected_value}, got {value}"
        )


def walk_tree(data, path):
    for key in path:
        data = data[key]
    return data
