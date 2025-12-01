import yaml
import numpy as np
import matplotlib.pyplot as plt

NUM_SHARDS: int = 16
BAR_WIDTH = 0.25

def get_data(yaml_dict, getter, default=0):
    result = [default for _ in range(NUM_SHARDS)]

    for el in yaml_dict:
        result[el['shard']] = getter(el)

    return result

def make_plot(title: str, filename: str, ylabel: str, asymmetric_data, symmetric_data):
    br1 = np.arange(NUM_SHARDS) 
    br2 = [x + BAR_WIDTH for x in br1] 

    plt.figure()
    plt.bar(br1, asymmetric_data, width=BAR_WIDTH, color='red', label="asymmetric")
    plt.bar(br2, symmetric_data, width=BAR_WIDTH, color='blue', label='symmetric')
    plt.xticks([r + BAR_WIDTH / 2 for r in range(NUM_SHARDS)], list(range(NUM_SHARDS)))
    plt.title(title)
    plt.xlabel('Shard')
    plt.ylabel(ylabel)
    plt.legend()
    plt.savefig(filename)

def make_plot_getter(title: str, filename: str, ylabel: str, asymmetric_data, symmetric_data, getter):
    make_plot(title, filename, ylabel, get_data(asymmetric_data, getter), get_data(symmetric_data, getter))

with open("asynchronous.in", 'r') as f:
    asymmetric_data = yaml.safe_load(f.read())

with open("synchronous.in", 'r') as f:
    symmetric_data = yaml.safe_load(f.read())

make_plot_getter("Throughput", "throughput.png", "kB/s", asymmetric_data, symmetric_data, lambda x: x['big_writes']['throughput'])
make_plot_getter("IOPS", "iops.png", "IO/s", asymmetric_data, symmetric_data, lambda x: x['big_writes']['IOPS'])
make_plot_getter("Average latency", "average_latency.png", "usec", asymmetric_data, symmetric_data, lambda x: x['big_writes']['latencies']['average'])
make_plot_getter("p0.5 latency", "p05_latency.png", "usec", asymmetric_data, symmetric_data, lambda x: x['big_writes']['latencies']['p0.5'])
make_plot_getter("p0.95 latency", "p095_latency.png", "usec", asymmetric_data, symmetric_data, lambda x: x['big_writes']['latencies']['p0.95'])
make_plot_getter("p0.99 latency", "p099_latency.png", "usec", asymmetric_data, symmetric_data, lambda x: x['big_writes']['latencies']['p0.99'])
make_plot_getter("p0.999 latency", "p0999_latency.png", "usec", asymmetric_data, symmetric_data, lambda x: x['big_writes']['latencies']['p0.999'])
make_plot_getter("Max latency", "max_latency.png", "usec", asymmetric_data, symmetric_data, lambda x: x['big_writes']['latencies']['max'])
make_plot_getter("Total requests", "total_requests.png", "count", asymmetric_data, symmetric_data, lambda x: x['big_writes']['stats']['total_requests'])