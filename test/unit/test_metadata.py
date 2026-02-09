from yaml import safe_dump, safe_load

from metadata import Metadata, MetricPlotMetadata


def generate_fake_metadata() -> Metadata:
    metadata = Metadata()

    first_metric_metadata = MetricPlotMetadata()
    first_metric_metadata.title = "First Metric"
    first_metric_metadata.value_axis_title = "Value"
    first_metric_metadata.file_name = "first_metric.png"
    first_metric_metadata.unit = "ms"
    metadata.sharded_metrics[("group1", "metric1")] = first_metric_metadata

    second_metric_metadata = MetricPlotMetadata()
    second_metric_metadata.title = "Second Metric"
    second_metric_metadata.value_axis_title = "Value"
    second_metric_metadata.file_name = "second_metric.png"
    second_metric_metadata.unit = "MB/s"
    metadata.sharded_metrics[("group1", "metric2")] = second_metric_metadata

    third_metric_metadata = MetricPlotMetadata()
    third_metric_metadata.title = "Third Metric"
    third_metric_metadata.value_axis_title = "Value"
    third_metric_metadata.file_name = "third_metric.png"
    third_metric_metadata.unit = "ms"
    metadata.shardless_metrics[("group2", "metric3")] = third_metric_metadata

    return metadata


def test_serialization():
    metadata = generate_fake_metadata()

    # Act
    yaml_str = safe_dump(metadata)
    print("Serialized Metadata YAML:")
    print(yaml_str)

    loaded_metadata = safe_load(yaml_str)

    # Assert
    assert isinstance(loaded_metadata, Metadata)

    for metric_name, expected_metadata in metadata.sharded_metrics.items():
        loaded_metric_metadata = loaded_metadata.get_sharded_metric_metadata(metric_name)
        assert loaded_metric_metadata is not None, f"Missing sharded metric metadata for {metric_name}"
        assert loaded_metric_metadata == expected_metadata, "Loaded metadata does not match original metadata"

    for metric_name, expected_metadata in metadata.shardless_metrics.items():
        loaded_metric_metadata = loaded_metadata.get_shardless_metric_metadata(metric_name)
        assert loaded_metric_metadata is not None, f"Missing shardless metric metadata for {metric_name}"
        assert loaded_metric_metadata == expected_metadata, "Loaded metadata does not match original metadata"

    for metric_name in metadata.sharded_metrics.keys():
        assert metric_name in loaded_metadata.sharded_metrics, (
            f"Missing sharded metric {metric_name} in loaded metadata"
        )

    for metric_name in metadata.shardless_metrics.keys():
        assert metric_name in loaded_metadata.shardless_metrics, (
            f"Missing shardless metric {metric_name} in loaded metadata"
        )
