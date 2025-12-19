"""Generates plots for sharded and shardless metrics."""

import pathlib
import pandas as pd
import plotly.express as px

def make_plot(title: str, filename: str, xlabel: str, ylabel: str, per_backend_data_vec: dict, xticks: bool):
    """Draw a grouped bar chart from a mapping backend -> list-of-values.

    Expects all value lists to have identical length.
    """
    size = len(next(iter(per_backend_data_vec.values())))
    for val in per_backend_data_vec.values():
        if len(val) != size:
            raise ValueError(f"Plotted data must have the same length")

    per_backend_data_with_shardnum = per_backend_data_vec.copy()
    per_backend_data_with_shardnum['Shard'] = list(range(0,size))

    df = pd.DataFrame(per_backend_data_with_shardnum)

    # Convert to long form
    df_long = df.melt(id_vars="Shard", value_vars=per_backend_data_vec.keys(),
                    var_name="Backend", value_name="Value")

    labels = {"Shard": xlabel if xlabel is not None else "", "Value": ylabel if ylabel is not None else "", "Backend": "Backend"}

    # Plot grouped bar chart
    fig = px.bar(df_long,
                x="Shard",
                y="Value",
                color="Backend",
                labels=labels,
                barmode="group",
                title=title,
    )

    fig.update_layout(bargap=0.5, bargroupgap=0.1, autosize=True)

    if xticks:
        fig.update_xaxes(tickmode="linear", dtick=1)
    else:
        fig.update_xaxes(showticklabels=False)

    # Optional: show values on top of bars
    fig.update_traces(texttemplate="%{y}", textposition="outside")

    fig.write_image(filename)


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
                if shard_idx > max_shard:
                    max_shard = shard_idx
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
    
    file_path = build_dir / pathlib.Path(f"auto_{file_basename}.svg")
    make_plot(metric_name, file_path, "shard", None, per_backend, True)

def plot_shardless_metric(metric_name: str, shardless_metric_by_backend: dict, build_dir: pathlib.Path):
    """Plot a single shardless metric described by `metric_map` (backend -> value).

    Produces a single bar chart.
    """
    file_basename = sanitize_filename(metric_name)

    per_backend = {}
    for backend, value in shardless_metric_by_backend.items():
        per_backend[backend] = [value]

    file_path = build_dir / pathlib.Path(f"auto_{file_basename}_total.svg")
    make_plot(metric_name + " (total)", file_path, None, None, per_backend, False)

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

    file_path = build_dir / pathlib.Path(f"auto_total_{file_basename}.svg")
    make_plot("Total " + metric_name, file_path, None, None, per_backend, False)

def sanitize_filename(name: str) -> str:
    return name.replace('/', '_')

def generate_graphs(sharded_metrics: dict[dict[dict]], shardless_metrics: dict[dict], build_dir: pathlib.Path):
    """Generate plots from a metrics mapping (metric_name -> backend -> value-or-dict).

    This function expects the output of `stats.join_metrics` as input.
    """

    for metric_name, metric_by_backend in sharded_metrics.items():
        plot_sharded_metric(metric_name, metric_by_backend, build_dir)
        plot_total_metric(metric_name, metric_by_backend, build_dir)

    for metric_name, metric_by_backend in shardless_metrics.items():
        plot_shardless_metric(metric_name, metric_by_backend, build_dir)