from yaml import safe_load
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
    unit: str | None

    def __init__(
        self,
        title: str,
        value_axis_title: str,
        unit: str | None = None,
    ) -> None:
        self.title = title
        self.value_axis_title = value_axis_title
        self.unit = unit

    def __repr__(self) -> str:
        return f"MetricPlotMetadata(title={self.title}, value_axis_title={self.value_axis_title}, unit={self.unit})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, MetricPlotMetadata):
            return NotImplemented
        return self.title == other.title and self.value_axis_title == other.value_axis_title and self.unit == other.unit

    def __hash__(self) -> int:
        return hash((self.title, self.value_axis_title, self.unit))

    def get_title(self) -> str:
        return self.title

    def get_title_with_unit(self) -> str:
        if self.unit:
            return f"{self.title} [{self.unit}]"
        return self.title

    def get_value_axis_title(self) -> str:
        return self.value_axis_title

    @classmethod
    def make_file_name_for_plot(cls, name: tuple[str, ...]) -> str:
        return "_".join(name)

    @classmethod
    def default(cls, path: tuple[str, ...]) -> "MetricPlotMetadata":
        return MetricPlotMetadata(title=_get_name_from_path(path), value_axis_title="Value", unit=None)


@yaml_info("metric_metadata")
class MetricMetadata(YamlAble):
    plotting: MetricPlotMetadata

    def __init__(self, plotting: MetricPlotMetadata) -> None:
        self.plotting = plotting

    def get_plot_metadata(self) -> MetricPlotMetadata:
        return self.plotting

    def __repr__(self) -> str:
        return f"MetricMetadata(plot_metadata={self.plotting})"


@yaml_info("metadata")
class BenchmarkMetadata(YamlAble):
    sharded_metrics: TreeDict[MetricMetadata]
    shardless_metrics: TreeDict[MetricMetadata]

    def __init__(
        self,
        sharded_metrics: TreeDict[MetricMetadata] | None = None,
        shardless_metrics: TreeDict[MetricMetadata] | None = None,
    ) -> None:
        self.sharded_metrics = sharded_metrics or TreeDict()
        self.shardless_metrics = shardless_metrics or TreeDict()

    def __repr__(self) -> str:
        return f"BenchmarkMetadata(sharded_metrics={self.sharded_metrics}, shardless_metrics={self.shardless_metrics})"

    def get_sharded_metric_metadata_or_default(self, metric_name: tuple[str, ...]) -> MetricPlotMetadata:
        return self._get_plot_metadata_or_default(self.sharded_metrics, metric_name)

    def get_shardless_metric_metadata_or_default(self, metric_name: tuple[str, ...]) -> MetricPlotMetadata:
        return self._get_plot_metadata_or_default(self.shardless_metrics, metric_name)

    @staticmethod
    def _get_plot_metadata_or_default(
        tree: TreeDict[MetricMetadata], metric_name: tuple[str, ...]
    ) -> MetricPlotMetadata:
        value = tree.get(metric_name, _asterix_compare)
        if value is None:
            return MetricPlotMetadata.default(metric_name)
        return value.get_plot_metadata()

    @staticmethod
    def load_from_yaml(yaml) -> "BenchmarkMetadata":
        obj = safe_load(yaml)
        if not isinstance(obj, BenchmarkMetadata):
            raise ValueError(f"Expected a Metadata object in the metadata file, got {type(obj)}")
        return obj


BenchmarkType = str


class BenchmarkMetadataHolder:
    def __init__(self):
        self._metadata = {}

    def set_metadata(self, benchmark_type: BenchmarkType, metadata: BenchmarkMetadata):
        self._metadata[benchmark_type] = metadata

    def get_metadata_or_default(self, benchmark_type: BenchmarkType | None) -> BenchmarkMetadata:
        if benchmark_type is None:
            return BenchmarkMetadata()
        return self._metadata.get(benchmark_type, BenchmarkMetadata())


def _asterix_compare(a: str, b: str) -> bool:
    """Compare two strings, treating '*' as a wildcard that matches any string."""
    if a == "*" or b == "*":
        return True
    return a == b


def _get_name_from_path(path: tuple[str, ...]) -> str:
    """Convert a metric path to a human-readable name for use in plot titles and file names."""
    return "_".join(path)
