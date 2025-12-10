import yaml
import numpy as np
import argparse
import plotly.express as px
import pandas as pd
import pathlib

BAR_WIDTH = 0.25

def get_data(yaml_dict, getter, num_shards, default=0):
    result = [default for _ in range(num_shards)]

    for el in yaml_dict:
        result[el['shard']] = getter(el)

    return result

def total_data(yaml_dict, getter):  
    result = [getter(el) for el in yaml_dict]
    return np.sum(result)

def make_plot(title: str, filename: str, xlabel: str, ylabel: str, per_backend_data_vec: dict, xticks):
    size = len(next(iter(per_backend_data_vec.values())))
    for val in per_backend_data_vec.values():
        if len(val) != size:
            raise ValueError(f"Plotted data must have the same length")

    per_backend_data_with_shardnum = per_backend_data_vec.copy()
    per_backend_data_with_shardnum['Shard'] = list(range(0,size))

    df = pd.DataFrame(per_backend_data_with_shardnum)

    # Convert to long form
    df_long = df.melt(id_vars="Shard", value_vars=per_backend_data_vec.keys(),
                    var_name="Type", value_name="Value")

    labels = {"Shard": xlabel if xlabel is not None else "", "Value": ylabel if ylabel is not None else "", "Type": "Type"}

    # Plot grouped bar chart
    fig = px.bar(df_long,
                x="Shard",
                y="Value",
                color="Type",
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

def make_plot_getter(title: str, filename: str, ylabel: str, backends_data: dict, getter):
    num_shards = max((len(x) for x in backends_data.values()))

    per_backend_data_vec = dict()
    for backend, data in backends_data.items():
        per_backend_data_vec[backend] = get_data(data, getter, num_shards)

    make_plot(title, filename, "shard", ylabel, per_backend_data_vec, True)

def load_data(raw_output: str):
    yaml_part = raw_output.split('---\n')[1]
    yaml_part = yaml_part.removesuffix("...\n")
    return yaml.safe_load(yaml_part)

def auto_generate_data_points(backends_data: dict):
    data_points = set()

    def walk_tree(prefix, data):
        if not isinstance(data, dict):
            return [prefix]

        result = []
        for key, val in data.items():
            result += walk_tree(prefix + [key], val)

        return result
    
    for data in backends_data.values():
        for el in data:
            data_points |= set([tuple(x) for x in walk_tree([], el)])

    data_points.remove(('shard',))

    return data_points

def plot_data_point(data_point, backends_data: dict, build_dir: pathlib.Path):
    def getter(data):
        for point in data_point:
            data = data[point]
        return data
    
    plot_title: str = " ".join(data_point)
    file_basename: str = "_".join(data_point).replace('/', '_')
    filename = build_dir / pathlib.Path(f"auto_{file_basename}.svg")
    
    make_plot_getter(plot_title.capitalize(), filename, None, backends_data, getter)

    totals = dict()
    for backend, data in backends_data.items():
        totals[backend] = [total_data(data, getter)]

    filename = build_dir / pathlib.Path(f"auto_total_{file_basename}.svg")
    make_plot(f"Total {plot_title}", filename, None, None, totals, False)
    print(f"{plot_title}: ", ', '.join((f"{key}: {val[0]}" for key, val in totals.items())))

def auto_generate(asymmetric_data, symmetric_data, build_dir: pathlib.Path):
    backends_data = {'Asymmetric':asymmetric_data, 'Symmetric':symmetric_data}
    for data_point in auto_generate_data_points(backends_data):
        plot_data_point(data_point, backends_data, build_dir)

def generate_graphs(asymmetric_data, symmetric_data, build_dir: pathlib.Path):
    auto_generate(load_data(asymmetric_data), load_data(symmetric_data), build_dir)