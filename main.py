import yaml
import numpy as np
import matplotlib.pyplot as plt

with open("asynchronous.in", 'r') as f:
    async_data = f.read()

with open("synchronous.in", 'r') as f:
    sync_data = f.read()

asym_parsed = yaml.safe_load(async_data)
sym_parsed = yaml.safe_load(sync_data)

NUM_SHARDS: int = 16
BAR_WIDTH = 0.25


async_throughputs = [0] * NUM_SHARDS
sync_throughputs = [0] * NUM_SHARDS

for el in asym_parsed:
    async_throughputs[el['shard']] = el['big_writes']['throughput']

for el in sym_parsed:
    sync_throughputs[el['shard']] = el['big_writes']['throughput']

def get_data(yaml_dict, getter, default=0):
    result = [default for _ in range(NUM_SHARDS)]

    for el in yaml_dict:
        result[el['shard']] = getter(el)

    return result

def make_plot(title: str, filename: str, ylabel: str, asymmetric_data, symmetric_data):
    br1 = np.arange(NUM_SHARDS) 
    br2 = [x + BAR_WIDTH for x in br1] 

    plt.bar(br1, asymmetric_data, width=BAR_WIDTH, color='red', label="asymmetric")
    plt.bar(br2, symmetric_data, width=BAR_WIDTH, color='blue', label='symmetric')
    plt.xticks([r + BAR_WIDTH / 2 for r in range(NUM_SHARDS)], list(range(NUM_SHARDS)))
    plt.title(title)
    plt.xlabel('Shard')
    plt.ylabel(ylabel)
    plt.legend()
    plt.savefig(filename)

get_throughput = lambda x: x['big_writes']['throughput']

make_plot("Throughput comparison", "test.png", "kB/s", get_data(asym_parsed, get_throughput), get_data(sym_parsed, get_throughput))