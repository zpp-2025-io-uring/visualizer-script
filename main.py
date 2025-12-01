import sys
import yaml
import numpy as np
import argparse
import matplotlib.pyplot as plt

parser = argparse.ArgumentParser(description="IO Tester Visualizer")
parser.add_argument("num_shards", type=int, help="Number of shards")
args = parser.parse_args()

num_shards: int = args.num_shards
BAR_WIDTH = 0.25

def get_data(yaml_dict, getter, default=0):
    result = [default for _ in range(num_shards)]

    for el in yaml_dict:
        result[el['shard']] = getter(el)

    return result

def total_data(yaml_dict, getter):
    result = [getter(el) for el in yaml_dict]
    return np.sum(result)

def make_plot(title: str, filename: str, xlabel: str, ylabel: str, asymmetric_data, symmetric_data, xticks):
    size = max(len(asymmetric_data), len(symmetric_data))

    br1 = np.arange(size) 
    br2 = [x + BAR_WIDTH for x in br1] 

    plt.figure()
    plt.bar(br1, asymmetric_data, width=BAR_WIDTH, color='red', label="asymmetric")
    plt.bar(br2, symmetric_data, width=BAR_WIDTH, color='blue', label='symmetric')
    plt.xticks([r + BAR_WIDTH / 2 for r in range(size)], xticks)
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.legend()
    plt.savefig(filename)

def make_plot_getter(title: str, filename: str, ylabel: str, asymmetric_data, symmetric_data, getter):
    make_plot(title, filename, "shard", ylabel, get_data(asymmetric_data, getter), get_data(symmetric_data, getter), list(range(num_shards)))

def load_data(raw_output: str):
    yaml_part = raw_output.split('Starting evaluation...\n---\n')[1]
    yaml_part = yaml_part.removesuffix("...\n")
    return yaml.safe_load(yaml_part)

with open("asynchronous.in", 'r') as f:
    asymmetric_data = load_data(f.read())

with open("synchronous.in", 'r') as f:
    symmetric_data = load_data(f.read())

get_throughput = lambda x: x['big_writes']['throughput']
make_plot_getter("Throughput", "throughput.png", "kB/s", asymmetric_data, symmetric_data, get_throughput)

get_iops = lambda x: x['big_writes']['IOPS']
make_plot_getter("IOPS", "iops.png", "IO/s", asymmetric_data, symmetric_data, get_iops)

make_plot_getter("Average latency", "average_latency.png", "usec", asymmetric_data, symmetric_data, lambda x: x['big_writes']['latencies']['average'])
make_plot_getter("p0.5 latency", "p05_latency.png", "usec", asymmetric_data, symmetric_data, lambda x: x['big_writes']['latencies']['p0.5'])
make_plot_getter("p0.95 latency", "p095_latency.png", "usec", asymmetric_data, symmetric_data, lambda x: x['big_writes']['latencies']['p0.95'])
make_plot_getter("p0.99 latency", "p099_latency.png", "usec", asymmetric_data, symmetric_data, lambda x: x['big_writes']['latencies']['p0.99'])
make_plot_getter("p0.999 latency", "p0999_latency.png", "usec", asymmetric_data, symmetric_data, lambda x: x['big_writes']['latencies']['p0.999'])
make_plot_getter("Max latency", "max_latency.png", "usec", asymmetric_data, symmetric_data, lambda x: x['big_writes']['latencies']['max'])

get_total_requests = lambda x: x['big_writes']['stats']['total_requests']
make_plot_getter("Total requests", "total_requests.png", "count", asymmetric_data, symmetric_data, get_total_requests)

total_getters = {'throughput': lambda x: total_data(x, get_throughput), "IOPS": lambda x: total_data(x, get_iops), "requests": lambda x: total_data(x, get_total_requests)}

for key, getter in total_getters.items():
    make_plot(f"Total {key}", f"total_{key}.png", key, "", [getter(asymmetric_data)], [getter(symmetric_data)], [])
