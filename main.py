import sys
import yaml
import numpy as np
import argparse
import matplotlib.pyplot as plt
import plotly.express as px
import pandas as pd

parser = argparse.ArgumentParser(description="IO Tester Visualizer")
args = parser.parse_args()

BAR_WIDTH = 0.25

def get_data(yaml_dict, getter, num_shards, default=0):
    result = [default for _ in range(num_shards)]

    for el in yaml_dict:
        result[el['shard']] = getter(el)

    return result

def total_data(yaml_dict, getter):
    result = [getter(el) for el in yaml_dict]
    return np.sum(result)

def make_plot(title: str, filename: str, xlabel: str, ylabel: str, asymmetric_data, symmetric_data, xticks):
    # ensure lengths match
    if len(asymmetric_data) != len(symmetric_data):
        raise ValueError(f"Asymmetric length {len(asymmetric_data)} != Symmetric length {len(symmetric_data)}")
    size = len(symmetric_data)

    df = pd.DataFrame({
        "Shard": list(range(0, size)),
        "Asymmetric": asymmetric_data,
        "Symmetric": symmetric_data
    })

    # Convert to long form
    df_long = df.melt(id_vars="Shard", value_vars=["Asymmetric", "Symmetric"],
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

def make_plot_getter(title: str, filename: str, ylabel: str, asymmetric_data, symmetric_data, getter):
    num_shards = len(symmetric_data)
    make_plot(title, filename, "shard", ylabel, get_data(asymmetric_data, getter, num_shards), get_data(symmetric_data, getter, num_shards), True)

def load_data(raw_output: str):
    yaml_part = raw_output.split('---\n')[1]
    yaml_part = yaml_part.removesuffix("...\n")
    return yaml.safe_load(yaml_part)


def auto_generate_data_points(asymmetric_data, symmetric_data):
    data_points = set()

    def walk_tree(prefix, data):
        if not isinstance(data, dict):
            return [prefix]

        result = []
        for key, val in data.items():
            result += walk_tree(prefix + [key], val)

        return result
    
    for el in asymmetric_data:
        data_points |= set([tuple(x) for x in walk_tree([], el)])

    for el in symmetric_data:
        data_points |= set([tuple(x) for x in walk_tree([], el)])

    data_points.remove(('shard',))

    return data_points

def plot_data_point(data_point, asymmetric_data, symmetric_data):
    def getter(data):
        for point in data_point:
            data = data[point]
        return data
    
    plot_title: str = " ".join(data_point)
    file_basename: str = "_".join(data_point).replace('/', '_')
    
    make_plot_getter(plot_title.capitalize(), f"auto_{file_basename}.svg", None, asymmetric_data, symmetric_data, getter)

    asymmetric_total = total_data(asymmetric_data, getter)
    symmetric_total = total_data(symmetric_data, getter)
    make_plot(f"Total {plot_title}", f"auto_total_{file_basename}.svg", None, None, [asymmetric_total], [symmetric_total], False)
    print(f"{plot_title}: asymmetric: {asymmetric_total:.4f}, symmetric: {symmetric_total:.4f}" + (f", percentage: {asymmetric_total * 100 / symmetric_total:.4f}%" if symmetric_total != 0 else ""))

def auto_generate(asymmetric_data, symmetric_data):
    for data_point in auto_generate_data_points(asymmetric_data, symmetric_data):
        plot_data_point(data_point, asymmetric_data, symmetric_data)

with open("asymmetric.in", 'r') as f:
    asymmetric_data = load_data(f.read())

with open("symmetric.in", 'r') as f:
    symmetric_data = load_data(f.read())

auto_generate(asymmetric_data, symmetric_data)