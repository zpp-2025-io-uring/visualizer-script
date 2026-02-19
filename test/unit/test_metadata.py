import pytest

from metadata import BenchmarkMetadata, MetricMetadata, MetricPlotMetadata, _asterix_compare
from tree import TreeDict


class TestMetricPlotMetadata:
    def test_init(self) -> None:
        obj = MetricPlotMetadata(title="Test Title", value_axis_title="Test Value")
        assert obj.title == "Test Title"
        assert obj.value_axis_title == "Test Value"
        assert obj.unit is None

    def test_init_with_unit(self) -> None:
        obj = MetricPlotMetadata(title="Test", value_axis_title="Value", unit="ms")
        assert obj.unit == "ms"

    def test_repr(self) -> None:
        obj = MetricPlotMetadata(title="Title", value_axis_title="Value", unit="ms")
        assert repr(obj) == "MetricPlotMetadata(title=Title, value_axis_title=Value, unit=ms)"

    def test_eq_true(self) -> None:
        obj1 = MetricPlotMetadata("Title", "Value", "ms")
        obj2 = MetricPlotMetadata("Title", "Value", "ms")
        assert obj1 == obj2

    def test_eq_false_different_unit(self) -> None:
        obj1 = MetricPlotMetadata("Title", "Value", "ms")
        obj2 = MetricPlotMetadata("Title", "Value", None)
        assert obj1 != obj2

    def test_get_title(self) -> None:
        obj = MetricPlotMetadata("Title", "Value")
        assert obj.get_title() == "Title"

    def test_get_title_with_unit(self) -> None:
        obj = MetricPlotMetadata("Title", "Value", "ms")
        assert obj.get_title_with_unit() == "Title [ms]"

    def test_get_title_without_unit(self) -> None:
        obj = MetricPlotMetadata("Title", "Value")
        assert obj.get_title_with_unit() == "Title"

    def test_get_value_axis_title(self) -> None:
        obj = MetricPlotMetadata("Title", "Value")
        assert obj.get_value_axis_title() == "Value"

    @pytest.mark.parametrize(
        "name, expected",
        [
            (("metric",), "metric"),
            (("path", "to", "metric"), "path_to_metric"),
        ],
    )
    def test_make_file_name_for_plot(self, name: tuple[str, ...], expected: str) -> None:
        assert MetricPlotMetadata.make_file_name_for_plot(name) == expected

    def test_default(self) -> None:
        obj = MetricPlotMetadata.default(("path", "to", "metric"))
        assert obj.title == "path_to_metric"
        assert obj.value_axis_title == "Value"
        assert obj.unit is None


class TestMetricMetadata:
    def test_init(self) -> None:
        plot = MetricPlotMetadata("Title", "Value")
        obj = MetricMetadata(plot)
        assert obj.plotting == plot

    def test_get_plot_metadata(self) -> None:
        plot = MetricPlotMetadata("Title", "Value")
        obj = MetricMetadata(plot)
        assert obj.get_plot_metadata() == plot

    def test_repr(self) -> None:
        plot = MetricPlotMetadata("Title", "Value")
        obj = MetricMetadata(plot)
        expected = f"MetricMetadata(plot_metadata={plot})"
        assert repr(obj) == expected


class TestMetadata:
    @pytest.fixture
    def sample_tree_dict(self) -> TreeDict[MetricMetadata]:
        plot1 = MetricPlotMetadata("metric1", "Value1")
        plot2 = MetricPlotMetadata("metric2", "Value2")
        tree_dict = TreeDict[MetricMetadata]()
        tree_dict[("path", "metric1")] = MetricMetadata(plot1)
        tree_dict[("path", "metric2")] = MetricMetadata(plot2)
        return tree_dict

    def test_init_defaults(self) -> None:
        obj = BenchmarkMetadata()
        assert isinstance(obj.sharded_metrics, TreeDict)
        assert len(obj.sharded_metrics) == 0
        assert isinstance(obj.shardless_metrics, TreeDict)
        assert len(obj.shardless_metrics) == 0

    def test_repr(self) -> None:
        sharded = TreeDict[MetricMetadata]()
        obj = BenchmarkMetadata(sharded_metrics=sharded)
        expected = f"BenchmarkMetadata(sharded_metrics={sharded}, shardless_metrics={TreeDict[MetricMetadata]()})"
        assert repr(obj) == expected

    def test_get_sharded_metric_metadata_or_default_present(self, sample_tree_dict: TreeDict[MetricMetadata]) -> None:
        obj = BenchmarkMetadata(sharded_metrics=sample_tree_dict)
        plot_meta = obj.get_sharded_metric_metadata_or_default(("path", "metric1"))
        assert plot_meta.title == "metric1"

    def test_get_sharded_metric_metadata_or_default_missing(self) -> None:
        obj = BenchmarkMetadata()
        plot_meta = obj.get_sharded_metric_metadata_or_default(("missing", "metric"))
        assert plot_meta.title == "missing_metric"
        assert plot_meta.value_axis_title == "Value"
        assert plot_meta.unit is None

    def test_get_shardless_metric_metadata_or_default_present(self, sample_tree_dict: TreeDict[MetricMetadata]) -> None:
        obj = BenchmarkMetadata(shardless_metrics=sample_tree_dict)
        plot_meta = obj.get_shardless_metric_metadata_or_default(("path", "metric1"))
        assert plot_meta.title == "metric1"

    def test_get_shardless_metric_metadata_or_default_missing(self) -> None:
        obj = BenchmarkMetadata()
        plot_meta = obj.get_shardless_metric_metadata_or_default(("missing", "metric"))
        assert plot_meta.title == "missing_metric"


def test_asterix_compare() -> None:
    assert _asterix_compare("a", "a") is True
    assert _asterix_compare("a", "*") is True
    assert _asterix_compare("*", "a") is True
    assert _asterix_compare("*", "*") is True
    assert _asterix_compare("a", "b") is False
