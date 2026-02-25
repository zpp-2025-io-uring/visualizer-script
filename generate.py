"""Generates plots for sharded and shardless metrics."""

import pathlib
from enum import Enum
from glob import escape
from typing import Any

import pandas as pd
import plotly.express as px
import plotly.io as pio
from plotly.graph_objs import Figure

from benchmark import PerBenchmarkShardedResults, PerBenchmarkShardlessResults, Results
from log import get_logger
from metadata import BACKEND_COLORS, BACKENDS_NAMES
from stats import Stats

logger = get_logger()


class PlotGenerator:
    """Generates plots for sharded and shardless metrics."""

    def __init__(self):
        self.figs = []
        self.file_paths = []

    def schedule_graphs_for_run(
        self,
        results: Results,
        build_dir: pathlib.Path,
    ) -> None:
        """Schedule generating per run"""

        for metric_name, metric_by_backend in results.sharded_metrics.items():
            plot_metric_name = make_metric_name_for_plot(metric_name)
            (metric_file_path, plot) = plot_sharded_metric(plot_metric_name, metric_by_backend, build_dir)
            self.figs.append(plot)
            self.file_paths.append(metric_file_path)

            (total_file_path, total_plot) = plot_total_metric(plot_metric_name, metric_by_backend, build_dir)
            self.figs.append(total_plot)
            self.file_paths.append(total_file_path)

        for metric_name, shardless_metric_by_backend in results.shardless_metrics.items():
            plot_metric_name = make_metric_name_for_plot(metric_name)
            (metric_file_path, plot) = plot_shardless_metric(plot_metric_name, shardless_metric_by_backend, build_dir)
            self.figs.append(plot)
            self.file_paths.append(metric_file_path)

    def schedule_graphs_for_summary(self, stats: Stats, build_dir: pathlib.Path, image_format: str = "svg"):
        build_dir = pathlib.Path(build_dir)
        build_dir.mkdir(parents=True, exist_ok=True)

        image_format = image_format.removeprefix(".").lower()
        if image_format not in {"svg", "png", "jpg", "jpeg", "pdf"}:
            raise ValueError(f"Unsupported image format: {image_format}")

        stat_to_plot = "mean"
        stat_as_error = "stdev"

        for metric, per_backend_sharded_metrics in stats.get_sharded_metrics().items():
            rows = summarize_sharded_metrics_by_backend(per_backend_sharded_metrics, stat_to_plot, stat_as_error)
            df_long = pd.DataFrame(rows)

            plot_metric_name = make_metric_name_for_plot(metric)
            file_path = build_dir / pathlib.Path(f"{sanitize_filename(plot_metric_name)}.{image_format}")
            fig = make_plot_with_error(
                PlotDataWithError(
                    type=PlotType.Sharded,
                    display_name=plot_metric_name,
                    df=df_long,
                    value_axis_label=f"{stat_to_plot} value",
                )
            )

            self.figs.append(fig)
            self.file_paths.append(file_path)

        for metric, per_backend_shardless_metrics in stats.get_shardless_metrics().items():
            rows = summarize_shardless_metrics_by_backend(per_backend_shardless_metrics, stat_to_plot, stat_as_error)
            df = pd.DataFrame(rows)
            plot_metric_name = make_metric_name_for_plot(metric)
            file_path = build_dir / pathlib.Path(f"{sanitize_filename(plot_metric_name)}.{image_format}")
            fig = make_plot_with_error(
                PlotDataWithError(
                    type=PlotType.Shardless,
                    display_name=plot_metric_name,
                    df=df,
                    value_axis_label=f"{stat_to_plot} value",
                )
            )
            self.figs.append(fig)
            self.file_paths.append(file_path)

    def plot(self):
        pio.write_images(fig=self.figs, file=self.file_paths)
        self.figs = []
        self.file_paths = []

    def __del__(self):
        # If there are any pending plots, generate them
        # No need for giving possibility to skip it for now
        if self.figs or self.file_paths:
            self.plot()


def summarize_sharded_metrics_by_backend(
    per_backend_sharded_metrics: dict[str, dict[int, Any]], stat_to_plot: str, stat_as_error: str
) -> list[dict]:
    """Summarize sharded metrics into a list of rows for plotting."""

    # Create mapping backend -> shard -> (stat_to_plot, stat_as_error)
    metric_by_backend = {}
    for backend, shards in per_backend_sharded_metrics.items():
        shard_dict = {}
        for shard, stats in shards.items():
            shard_idx = int(shard)
            shard_dict[shard_idx] = (stats[stat_to_plot], stats[stat_as_error])
        metric_by_backend[backend] = shard_dict

    # Convert to list of rows for plotting
    rows = []
    for backend, shards in metric_by_backend.items():
        for shard in sorted(shards.keys()):
            value, error = shards[shard]
            rows.append(
                {
                    DF_SHARD_KEY: int(shard),
                    DF_BACKEND_KEY: backend,
                    DF_VALUE_KEY: value,
                    DF_ERROR_KEY: error,
                }
            )

    return rows


def summarize_shardless_metrics_by_backend(
    per_backend_shardless_metrics: dict[str, dict[Any, Any]], stat_to_plot: str, stat_as_error: str
) -> list[dict]:
    """Summarize shardless metrics into a list of rows for plotting."""

    # Create mapping backend -> (stat_to_plot, stat_as_error)
    metric_by_backend = {}
    for backend, stats in per_backend_shardless_metrics.items():
        metric_by_backend[backend] = (stats[stat_to_plot], stats[stat_as_error])

    # Convert to list of rows for plotting
    rows = []
    for backend, (value, error) in metric_by_backend.items():
        rows.append(
            {
                DF_BACKEND_KEY: backend,
                DF_VALUE_KEY: value,
                DF_ERROR_KEY: error,
                DF_SHARD_KEY: None,
            }
        )

    return rows


class PlotType(Enum):
    Sharded = 1
    Shardless = 2


class PlotData:
    type: PlotType
    display_name: str
    data: dict[str, Any]
    value_axis_label: str

    def __init__(self, type: PlotType, display_name: str, data: dict[str, Any], value_axis_label: str | None = None):
        self.type = type
        self.display_name = display_name
        self.data = data
        self.value_axis_label = value_axis_label if value_axis_label is not None else "Value"

        size = len(next(iter(data.values())))
        for val in data.values():
            if len(val) != size:
                raise ValueError("Plotted data must have the same length")


DF_SHARD_KEY = "Shard"
DF_VALUE_KEY = "Value"
DF_BACKEND_KEY = "Backend"
DF_ERROR_KEY = "Error"


def make_plot(
    data: PlotData,
) -> Figure:
    size = len(next(iter(data.data.values())))
    per_backend_data_with_shardnum = data.data.copy()
    per_backend_data_with_shardnum[DF_SHARD_KEY] = list(range(0, size))

    df = pd.DataFrame(per_backend_data_with_shardnum)
    df_long = df.melt(
        id_vars=DF_SHARD_KEY, value_vars=list(data.data.keys()), var_name=DF_BACKEND_KEY, value_name=DF_VALUE_KEY
    )

    labels = {
        DF_SHARD_KEY: "Shard" if data.type == PlotType.Sharded else "",
        DF_VALUE_KEY: data.value_axis_label,
        DF_BACKEND_KEY: "Backend",
    }
    fig = px.bar(
        df_long,
        x=DF_SHARD_KEY,
        y=DF_VALUE_KEY,
        color=DF_BACKEND_KEY,
        color_discrete_map=BACKEND_COLORS,
        category_orders={DF_BACKEND_KEY: BACKENDS_NAMES},
        labels=labels,
        barmode="group",
        title=data.display_name,
    )

    apply_bar_template(fig, data.type)
    fig.update_traces(texttemplate="%{y}", textposition="outside")

    return fig


class PlotDataWithError(PlotData):
    type: PlotType
    display_name: str
    df: pd.DataFrame
    value_axis_label: str

    def __init__(
        self,
        type: PlotType,
        display_name: str,
        df: pd.DataFrame,
        value_axis_label: str | None = None,
    ):
        self.type = type
        self.display_name = display_name
        self.df = df
        self.value_axis_label = value_axis_label if value_axis_label is not None else "Value"


def make_plot_with_error(
    data: PlotDataWithError,
) -> Figure:
    labels = {
        DF_SHARD_KEY: "Shard" if data.type == PlotType.Sharded else "",
        DF_VALUE_KEY: data.value_axis_label,
        DF_BACKEND_KEY: "Backend",
    }

    plot_kwargs = {
        "y": DF_VALUE_KEY,
        "error_y": DF_ERROR_KEY,
        "barmode": "group",
        "title": data.display_name,
        "labels": labels,
        "color": DF_BACKEND_KEY,
        "color_discrete_map": BACKEND_COLORS,
        "category_orders": {DF_BACKEND_KEY: BACKENDS_NAMES},
    }
    if data.type == PlotType.Sharded:
        plot_kwargs["x"] = DF_SHARD_KEY
    else:
        plot_kwargs["x"] = DF_BACKEND_KEY

    fig = px.bar(data.df, **plot_kwargs)

    fig.update_layout(
        width=find_width_for_min_bar(
            len(data.df[DF_SHARD_KEY].unique()),
            len(data.df[DF_BACKEND_KEY].unique()) if DF_BACKEND_KEY else 1,
        )
    )
    apply_bar_template(fig, data.type)

    return fig


def apply_bar_template(fig: Figure, type: PlotType) -> None:
    fig.update_layout(bargap=0.2, bargroupgap=0.1)
    fig.update_layout(margin_autoexpand=True)

    if type == PlotType.Sharded:
        fig.update_xaxes(tickmode="linear", dtick=1)
    else:
        fig.update_xaxes(showticklabels=False)


def find_width_for_min_bar(number_of_groups: int, number_of_bars_per_group: int) -> int:
    default_width = 400
    if number_of_groups * number_of_bars_per_group == 0:
        return default_width
    width_per_bar = 60
    calculated_width = number_of_groups * number_of_bars_per_group * width_per_bar
    return max(default_width, calculated_width)


def plot_sharded_metric(
    metric_name: str, sharded_metric_by_backend: PerBenchmarkShardedResults, build_dir: pathlib.Path
) -> tuple[pathlib.Path, Figure]:
    file_basename = sanitize_filename(metric_name)

    # determine max shard index
    max_shard = -1
    for backend, result_by_shard in sharded_metric_by_backend.backends.items():
        for measurement in result_by_shard.shards:
            max_shard = max(max_shard, measurement.shard)

    if max_shard == -1:
        raise ValueError(f"No sharded data found for metric {metric_name}")

    num_shards = max_shard + 1

    per_backend = {}
    for backend, result_by_shard in sharded_metric_by_backend.backends.items():
        values = [0] * num_shards
        for measurement in result_by_shard.shards:
            values[measurement.shard] = measurement.value
        per_backend[backend] = values

    file_path = build_dir / pathlib.Path(f"{file_basename}.svg")
    logger.debug(f"Plotting sharded {file_path}")
    return (
        file_path,
        make_plot(PlotData(type=PlotType.Sharded, display_name=metric_name, data=per_backend)),
    )


def plot_shardless_metric(
    metric_name: str, shardless_metric_by_backend: PerBenchmarkShardlessResults, build_dir: pathlib.Path
) -> tuple[pathlib.Path, Figure]:
    file_basename = sanitize_filename(metric_name)

    per_backend = {}
    for backend, measurement in shardless_metric_by_backend.backends.items():
        per_backend[backend] = [measurement.value]

    file_path = build_dir / pathlib.Path(f"{file_basename}.svg")
    logger.debug(f"Plotting shardless metric {file_path}")
    return (
        file_path,
        make_plot(PlotData(type=PlotType.Shardless, display_name=metric_name, data=per_backend)),
    )


def plot_total_metric(
    metric_name: str, sharded_metric_by_backend: PerBenchmarkShardedResults, build_dir: pathlib.Path
) -> tuple[pathlib.Path, Figure]:
    """Plot a sharded metric as total values per backend.

    Produces a single bar chart.
    """
    file_basename = sanitize_filename(metric_name)

    per_backend = {}
    for backend, result_by_shard in sharded_metric_by_backend.backends.items():
        total = 0
        for measurement in result_by_shard.shards:
            total += measurement.value
        per_backend[backend] = [total]

    file_path = build_dir / pathlib.Path(f"total_{file_basename}.svg")
    logger.debug(f"Plotting total metric {file_path}")
    return (
        file_path,
        make_plot(PlotData(type=PlotType.Shardless, display_name=f"Total {metric_name}", data=per_backend)),
    )


def make_metric_name_for_plot(name: tuple[str, ...]) -> str:
    return "_".join(name)


def sanitize_filename(name: str) -> str:
    return escape(name)
