from yamlable import YamlAble, yaml_info

from tree import TreeDict

BACKENDS_NAMES = ["epoll", "linux-aio", "io_uring", "asymmetric_io_uring"]

# https://plotly.com/python/discrete-color/
BACKEND_COLORS = {
    "epoll": "#ef553b",
    "linux-aio": "#ab63fa",
    "io_uring": "#00cc96",
    "asymmetric_io_uring": "#636efa",
}

assert set(BACKENDS_NAMES).issubset(set(BACKEND_COLORS.keys())), "All backends must have a defined color"


@yaml_info("metric_plot_metadata")
class MetricPlotMetadata(YamlAble):
    title: str
    value_axis_title: str
    file_name: str
    unit: str | None

    def __init__(
        self,
        title: str,
        value_axis_title: str,
        file_name: str,
        unit: str | None = None,
    ) -> None:
        self.title = title
        self.value_axis_title = value_axis_title
        self.file_name = file_name
        self.unit = unit

    def __repr__(self) -> str:
        return f"MetricPlotMetadata(title={self.title}, value_axis_title={self.value_axis_title}, file_name={self.file_name}, unit={self.unit})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, MetricPlotMetadata):
            return NotImplemented
        return (
            self.title == other.title
            and self.value_axis_title == other.value_axis_title
            and self.file_name == other.file_name
            and self.unit == other.unit
        )

    def __hash__(self) -> int:
        return hash((self.title, self.value_axis_title, self.file_name, self.unit))


@yaml_info("metadata")
class Metadata(YamlAble):
    sharded_metrics: TreeDict[MetricPlotMetadata]
    shardless_metrics: TreeDict[MetricPlotMetadata]

    def __init__(
        self,
        sharded_metrics: TreeDict[MetricPlotMetadata] | None = None,
        shardless_metrics: TreeDict[MetricPlotMetadata] | None = None,
    ) -> None:
        self.sharded_metrics = sharded_metrics or TreeDict()
        self.shardless_metrics = shardless_metrics or TreeDict()

    def __repr__(self) -> str:
        return f"Metadata(sharded_metrics={self.sharded_metrics}, shardless_metrics={self.shardless_metrics})"

    def get_sharded_metric_metadata_or_default(self, metric_name: tuple[str, ...]) -> MetricPlotMetadata | None:
        value = self.sharded_metrics.get(metric_name, _asterix_compare)
        if value is None:
            return self.default(metric_name)
        return value

    def get_shardless_metric_metadata_or_default(self, metric_name: tuple[str, ...]) -> MetricPlotMetadata | None:
        value = self.shardless_metrics.get(metric_name, _asterix_compare)
        if value is None:
            return self.default(metric_name)
        return value

    @classmethod
    def default(cls, path: tuple[str, ...]) -> MetricPlotMetadata:
        name = _make_metric_name_for_plot(path)
        return MetricPlotMetadata(title=path[-1], value_axis_title="Value", file_name=name, unit=None)


def _asterix_compare(a: str, b: str) -> bool:
    """Compare two strings, treating '*' as a wildcard that matches any string."""
    if a == "*" or b == "*":
        return True
    return a == b


def _make_metric_name_for_plot(name: tuple[str, ...]) -> str:
    return "_".join(name)
