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

    fig.update_layout(bargap=0.5, bargroupgap=0.1)

    if xticks:
        fig.update_xaxes(tickmode="linear", dtick=1)
    else:
        fig.update_xaxes(showticklabels=False)

    # Optional: show values on top of bars
    fig.update_traces(texttemplate="%{y}", textposition="outside")

    fig.write_image(filename)


def _is_sharded_metric(metric_map: dict) -> bool:
    # metric_map: backend -> either scalar or dict-of-shard
    for v in metric_map.values():
        if isinstance(v, dict):
            return True
    return False


def plot_metric(metric_name: str, metric_map: dict, build_dir: pathlib.Path):
    """Plot a single metric described by `metric_map` (backend -> value-or-dict).

    Produces a per-shard grouped plot when sharded data is present and a
    separate totals plot. Non-sharded scalar values are shown as single-bar
    grouped charts.
    """
    # sanitize filename
    file_basename = metric_name.replace('/', '_')

    if _is_sharded_metric(metric_map):
        # determine number of shards
        max_shard = -1
        for v in metric_map.values():
            if isinstance(v, dict):
                # keys may be ints or strings like '_total'
                for k in v.keys():
                    if isinstance(k, int):
                        max_shard = max(max_shard, k)
                    else:
                        try:
                            ik = int(k)
                            max_shard = max(max_shard, ik)
                        except Exception:
                            pass

        num_shards = max_shard + 1 if max_shard >= 0 else 0

        if num_shards > 0:
            per_backend = {}
            for backend, v in metric_map.items():
                vec = [0 for _ in range(num_shards)]
                if isinstance(v, dict):
                    for k, val in v.items():
                        # treat keys that are ints or numeric strings as shard indices
                        if k == '_total':
                            continue
                        try:
                            idx = int(k)
                        except Exception:
                            continue
                        if idx < num_shards:
                            vec[idx] = val
                # scalars remain zero-filled for per-shard plot
                per_backend[backend] = vec

            filename = build_dir / pathlib.Path(f"auto_{file_basename}.svg")
            make_plot(metric_name, filename, "shard", None, per_backend, True)

        # totals plot
        totals = {}
        for backend, v in metric_map.items():
            if isinstance(v, dict):
                # sum numeric shards, ignore '_total' if present (prefer explicit)
                total_val = 0
                for k, val in v.items():
                    if k == '_total':
                        total_val = val
                        break
                    try:
                        float(val)
                        total_val += val
                    except Exception:
                        pass
                totals[backend] = [total_val]
            else:
                totals[backend] = [v]

        filename = build_dir / pathlib.Path(f"auto_total_{file_basename}.svg")
        make_plot(f"Total {metric_name}", filename, None, None, totals, False)
        print(f"{metric_name}: ", ', '.join((f"{key}: {val[0]}" for key, val in totals.items())))
    else:
        # all scalars -> single grouped bar chart
        per_backend = {backend: [v] for backend, v in metric_map.items()}
        filename = build_dir / pathlib.Path(f"auto_{file_basename}.svg")
        make_plot(metric_name, filename, None, None, per_backend, False)
        print(f"{metric_name}: ", ', '.join((f"{key}: {val[0]}" for key, val in per_backend.items())))


def generate_graphs(metrics: dict, build_dir: pathlib.Path):
    """Generate plots from a metrics mapping (metric_name -> backend -> value-or-dict).

    This function expects the output of `parse.join_metrics` as input.
    """
    for metric_name, metric_map in metrics.items():
        try:
            plot_metric(metric_name, metric_map, build_dir)
        except Exception as e:
            print(f"Failed to plot {metric_name}: {e}")


def join_stats(metrics_list: list[dict]):
    """Join metrics from multiple runs.

    Input: list where each element is a metrics mapping (metric_name -> backend -> value-or-dict)
    Output: mapping metric_name -> backend -> shard_index_or__total -> list_of_values
    """
    results = {}

    for metrics in metrics_list:
        for metric_name, backend_map in metrics.items():
            if metric_name not in results:
                results[metric_name] = {}
            for backend, val in backend_map.items():
                if backend not in results[metric_name]:
                    results[metric_name][backend] = {}
                if isinstance(val, dict):
                    for k, v in val.items():
                        key = k if k == '_total' else int(k) if isinstance(k, (int, str)) and str(k).isdigit() else k
                        results[metric_name][backend].setdefault(key, []).append(v)
                else:
                    # scalar value -> append under '_total'
                    results[metric_name][backend].setdefault('_total', []).append(val)

    return results