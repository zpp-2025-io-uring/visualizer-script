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
    title: str | None
    value_axis_title: str | None
    file_name: str | None
    unit: str | None

    def __init__(
        self,
        title: str | None = None,
        value_axis_title: str | None = None,
        file_name: str | None = None,
        unit: str | None = None,
    ) -> None:
        self.title = title
        self.value_axis_title = value_axis_title
        self.file_name = file_name
        self.unit = unit


@yaml_info("metadata")
class Metadata(YamlAble):
    def __init__(self) -> None:
        self.sharded_metrics: TreeDict[MetricPlotMetadata] = TreeDict()
        self.shardless_metrics: TreeDict[MetricPlotMetadata] = TreeDict()

    def get_sharded_metric_metadata(self, metric_name: tuple[str, ...]) -> MetricPlotMetadata | None:
        return self.sharded_metrics.get(metric_name, _asteriks_compare)

    def get_shardless_metric_metadata(self, metric_name: tuple[str, ...]) -> MetricPlotMetadata | None:
        return self.shardless_metrics.get(metric_name, _asteriks_compare)


def _asteriks_compare(a: str, b: str) -> bool:
    """Compare two strings, treating '*' as a wildcard that matches any string."""
    if a == "*" or b == "*":
        return True
    return a == b
