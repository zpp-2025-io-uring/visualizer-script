"""Generates plots for sharded and shardless metrics."""

import pathlib

import pandas as pd
import plotly.express as px
import plotly.io as pio

from stats import Stats


class PlotGenerator:
    """Generates plots for sharded and shardless metrics."""

    def __init__(self):
        self.figs = []
        self.file_paths = []

    def schedule_generate_graphs(
        self, sharded_metrics: dict[dict[dict]], shardless_metrics: dict[dict], build_dir: pathlib.Path
    ):
        """Schedule generating plots from a metrics mapping (metric_name -> backend -> value-or-dict).

        This function expects the output of `stats.join_metrics` as input.
        """
        for metric_name, metric_by_backend in sharded_metrics.items():
            (metric_file_path, plot) = plot_sharded_metric(metric_name, metric_by_backend, build_dir)
            self.figs.append(plot)
            self.file_paths.append(metric_file_path)

            (total_file_path, total_plot) = plot_total_metric(metric_name, metric_by_backend, build_dir)
            self.figs.append(total_plot)
            self.file_paths.append(total_file_path)

        for metric_name, metric_by_backend in shardless_metrics.items():
            (metric_file_path, plot) = plot_shardless_metric(metric_name, metric_by_backend, build_dir)
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

        for metric in stats.get_sharded_metrics():
            rows = summarize_sharded_metrics_by_backend(
                metric, stats.get_sharded_metrics()[metric], stat_to_plot, stat_as_error
            )
            df_long = pd.DataFrame(rows)

            file_path = build_dir / pathlib.Path(f"auto_{sanitize_filename(metric)}_with_error_bars.{image_format}")
            fig = make_plot_from_df(
                metric,
                df_long,
                x="shard",
                y=stat_to_plot,
                color="backend",
                error_y=stat_as_error,
                xlabel="Shard",
                ylabel=f"{stat_to_plot} value",
                xticks=True,
            )

            self.figs.append(fig)
            self.file_paths.append(file_path)

        for metric in stats.get_shardless_metrics():
            rows = summarize_shardless_metrics_by_backend(
                metric, stats.get_shardless_metrics()[metric], stat_to_plot, stat_as_error
            )
            df = pd.DataFrame(rows)
            file_path = build_dir / pathlib.Path(f"{sanitize_filename(metric)}.{image_format}")
            fig = make_plot_from_df(
                metric,
                df,
                x="backend",
                y=stat_to_plot,
                color="backend",
                error_y=stat_as_error,
                ylabel=f"{stat_to_plot} value",
                xticks=False,
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
    metric: str, per_backend_sharded_metrics: dict, stat_to_plot: str, stat_as_error: str
) -> dict:
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
            rows.append({"shard": int(shard), "backend": backend, stat_to_plot: value, stat_as_error: error})

    return rows


def summarize_shardless_metrics_by_backend(
    metric: str, per_backend_shardless_metrics: dict, stat_to_plot: str, stat_as_error: str
) -> dict:
    """Summarize shardless metrics into a list of rows for plotting."""

    # Create mapping backend -> (stat_to_plot, stat_as_error)
    metric_by_backend = {}
    for backend, stats in per_backend_shardless_metrics.items():
        metric_by_backend[backend] = (stats[stat_to_plot], stats[stat_as_error])

    # Convert to list of rows for plotting
    rows = []
    for backend, (value, error) in metric_by_backend.items():
        rows.append({"backend": backend, stat_to_plot: value, stat_as_error: error})

    return rows


def make_plot(title: str, xlabel: str, ylabel: str, per_backend_data_vec: dict, xticks: bool):
    """Draw a grouped bar chart from a mapping backend -> list-of-values.

    Expects all value lists to have identical length.
    """
    size = len(next(iter(per_backend_data_vec.values())))
    for val in per_backend_data_vec.values():
        if len(val) != size:
            raise ValueError("Plotted data must have the same length")

    per_backend_data_with_shardnum = per_backend_data_vec.copy()
    per_backend_data_with_shardnum["Shard"] = list(range(0, size))

    df = pd.DataFrame(per_backend_data_with_shardnum)

    # Convert to long form
    df_long = df.melt(id_vars="Shard", value_vars=per_backend_data_vec.keys(), var_name="Backend", value_name="Value")

    labels = {
        "Shard": xlabel if xlabel is not None else "",
        "Value": ylabel if ylabel is not None else "",
        "Backend": "Backend",
    }

    # Plot grouped bar chart
    fig = px.bar(
        df_long,
        x="Shard",
        y="Value",
        color="Backend",
        labels=labels,
        barmode="group",
        title=title,
    )

    fig.update_layout(bargap=0.5, bargroupgap=0.1)

    if xticks:
        fig.update_xaxes(tickmode="linear", dtick=1)
    else:
        fig.update_xaxes(showticklabels=False)

    # Optional: show values on top of bars
    fig.update_traces(texttemplate="%{y}", textposition="outside")

    return fig


def make_plot_from_df(
    title: str,
    df: pd.DataFrame,
    x: str,
    y: str,
    color: str | None = None,
    error_y: str | None = None,
    xlabel: str | None = None,
    ylabel: str | None = None,
    xticks: bool = False,
):
    """Draw a grouped bar chart from a DataFrame.

    Parameters mirror the usage in this module: `x`, `y` are column names in `df`, `color`
    is an optional column name for series, and `error_y` is an optional column name
    providing error bar values.
    """
    labels = {}
    if xlabel is not None:
        labels[x] = xlabel
    if ylabel is not None:
        labels[y] = ylabel

    plot_kwargs = {"x": y, "y": x, "orientation": "h", "barmode": "group", "title": title, "labels": labels}
    if color is not None:
        plot_kwargs["color"] = color
    if error_y is not None:
        plot_kwargs["error_x"] = error_y

    fig = px.bar(df, **plot_kwargs)

    fig.update_layout(height=find_height_for_min_bar(len(df[x].unique()), len(df[color].unique()) if color else 1))
    fig.update_layout(bargap=0.2, bargroupgap=0.1)
    fig.update_layout(margin_autoexpand=True)

    if xticks:
        fig.update_yaxes(tickmode="linear", dtick=1)
    else:
        fig.update_yaxes(showticklabels=False)

    return fig


def find_height_for_min_bar(number_of_groups: int, number_of_bars_per_group: int) -> int:
    default_height = 400
    if number_of_groups * number_of_bars_per_group == 0:
        return default_height
    height_per_bar = 20
    calculated_height = number_of_groups * number_of_bars_per_group * height_per_bar
    return max(default_height, calculated_height)


def plot_sharded_metric(metric_name: str, sharded_metric_by_backend: dict, build_dir: pathlib.Path):
    """Plot a single metric described by `metric_map` (backend -> shard -> value).

    Produces a per-shard grouped plot and a separate totals plot.
    """
    file_basename = sanitize_filename(metric_name)

    # determine max shard index
    max_shard = -1
    for backend, result_by_shard in sharded_metric_by_backend.items():
        for shard, result in result_by_shard.items():
            try:
                shard_idx = int(shard)
                max_shard = max(max_shard, shard_idx)
            except Exception:
                raise ValueError(f"Shard identifiers must be integers, got {shard} for backend {backend}")

    if max_shard == -1:
        raise ValueError(f"No sharded data found for metric {metric_name}")

    num_shards = max_shard + 1

    per_backend = {}
    for backend, result_by_shard in sharded_metric_by_backend.items():
        values = []
        for shard_idx in range(num_shards):
            if shard_idx in result_by_shard:
                values.append(result_by_shard[shard_idx])
            else:
                values.append(0)
        per_backend[backend] = values

    file_path = build_dir / pathlib.Path(f"{file_basename}.svg")
    return (file_path, make_plot(metric_name, "shard", None, per_backend, True))


def plot_shardless_metric(metric_name: str, shardless_metric_by_backend: dict, build_dir: pathlib.Path):
    """Plot a single shardless metric described by `metric_map` (backend -> value).

    Produces a single bar chart.
    """
    file_basename = sanitize_filename(metric_name)

    per_backend = {}
    for backend, value in shardless_metric_by_backend.items():
        per_backend[backend] = [value]

    file_path = build_dir / pathlib.Path(f"{file_basename}.svg")
    return (file_path, make_plot(metric_name, None, None, per_backend, False))


def plot_total_metric(metric_name: str, sharded_metric_by_backend: dict, build_dir: pathlib.Path):
    """Plot a sharded metric as total values per backend.

    Produces a single bar chart.
    """
    file_basename = sanitize_filename(metric_name)

    per_backend = {}
    for backend, result_by_shard in sharded_metric_by_backend.items():
        total = 0
        for _, value in result_by_shard.items():
            total += value
        per_backend[backend] = [total]

    file_path = build_dir / pathlib.Path(f"total_{file_basename}.svg")
    return (file_path, make_plot("Total " + metric_name, None, None, per_backend, False))


def sanitize_filename(name: str) -> str:
    return name.replace("/", "_")
