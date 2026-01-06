from typing import Callable

def get_config_version(config: dict) -> int:
    if "config_version" not in config:
        return 1
    else:
        return config["config_version"]
    
def parse_cpuset(cpuset: str) -> set[int]:
    result = set()
    for element in cpuset.split(','):
        if '-' in element:
            begin, end = element.split('-')
            result.update(range(int(begin), int(end)+1))
        else:
            result.add(int(element))
    return result

def cpuset_to_string(cpuset: set[int]) -> str:
    return ','.join((str(el) for el in cpuset))

def make_proportional_splitter(cores_per_worker: int) -> Callable[[set[int]], tuple[set[int], set[int]]]:
    return lambda cpuset: proportional_splitter(cpuset, cores_per_worker)

def proportional_splitter(cpuset: set[int], cores_per_worker: int) -> tuple[set[int], set[int]]:
    if cores_per_worker == 0:
        raise ValueError("Cores per worker must be more than 0")

    num_workers = len(cpuset) // (cores_per_worker + 1)

    if len(cpuset) <= num_workers:
        raise RuntimeError("Not enough cores in the cpuset for the reuested number of async workers")
    
    cpuset = sorted(list(cpuset))

    async_worker_cpuset = cpuset[:num_workers]
    app_cpuset = cpuset[num_workers:]

    return set(app_cpuset), set(async_worker_cpuset)


def upgrade_version1_to_version2(config: dict, splitter: Callable[[set[int]], tuple[set[int], set[int]]]) -> dict:
    if get_config_version(config) != 1:
        raise ValueError("Expected version 1 config")

    config = config.copy() # Don't modify the original config
    depreciated_keys = ['io_asymmetric_cpuset', 'rpc_asymmetric_server_cpuset', 'rpc_asymmetric_client_cpuset']

    for key in depreciated_keys:
        cpuset = parse_cpuset(config.pop(key))
        key_basename = key.removesuffix('_cpuset')

        app_cpuset, async_workers_cpuset = splitter(cpuset)

        config[key_basename + '_app_cpuset'] = cpuset_to_string(app_cpuset)
        config[key_basename + '_async_worker_cpuset'] = cpuset_to_string(async_workers_cpuset)

    config['config_version'] = 2

    return config