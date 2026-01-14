from yaml import safe_load
from yamlable import YamlAble, yaml_info

from stats import Stats, summarize_stats


@yaml_info("benchmark")
class Benchmark(YamlAble):
    def __init__(self, runs: list, benchmark: dict, summary: Stats, run_count: int = None):
        self.runs = runs
        self.benchmark = benchmark
        self.summary = summary
        self.run_count = run_count if run_count is not None else len(runs)

    def get_runs(self) -> dict:
        return self.runs

    def get_benchmark(self) -> dict:
        return self.benchmark

    def get_stats(self) -> Stats:
        return self.summary

    def get_run_count(self) -> int:
        return self.run_count

    @classmethod
    def __from_yaml_dict__(cls, dct, yaml_tag=None):
        # If a legacy (plain mapping) YAML was used and `summary` is a dict,
        # convert it into a `stats` instance so the `benchmark` object always
        # exposes a `stats` object for `.summary`.
        if isinstance(dct, dict) and "summary" in dct and isinstance(dct["summary"], dict):
            dct["summary"] = Stats(**dct["summary"])

        return cls(**dct)

    @classmethod
    def load_from_file(cls, file):
        """Load a benchmark summary from a YAML file and return a `benchmark` instance.

        This helper accepts both tagged yamlable documents and legacy plain mappings
        (no `!yamlable/...` tag). It prefers the yamlable loading path when possible,
        otherwise it normalizes the mapping and constructs the object.
        """
        data = safe_load(file)

        if isinstance(data, cls):
            return data

        if isinstance(data, dict):
            return cls.__from_yaml_dict__(data, yaml_tag=None)

        raise TypeError(f"Cannot load benchmark: unexpected YAML document type {type(data)}")


def compute_benchmark_summary(sharded_metrics: dict, shardless_metrics: dict, benchmark_info: dict) -> Benchmark:
    # build map run_id -> run entry
    runs_map: dict = {}

    # process sharded metrics
    for metric_name, backends in (sharded_metrics or {}).items():
        for backend_name, items in backends.items():
            for item in items:
                run_id = item.get("run_id")
                shard = item.get("shard")
                value = item.get("value")

                if run_id not in runs_map:
                    runs_map[run_id] = {
                        "id": run_id,
                        "properties": {},
                        "results": {"sharded_metrics": {}, "shardless_metrics": {}},
                    }

                run_entry = runs_map[run_id]
                sharded_metrics_for_run = run_entry["results"]["sharded_metrics"]
                if metric_name not in sharded_metrics_for_run:
                    sharded_metrics_for_run[metric_name] = {"properties": {}, "backends": {}}

                backends_for_metric = sharded_metrics_for_run[metric_name]["backends"]
                if backend_name not in backends_for_metric:
                    backends_for_metric[backend_name] = {"properties": {}, "shards": []}

                backends_for_metric[backend_name]["shards"].append({"shard": shard, "value": value})

    # process shardless metrics
    for metric_name, backends in (shardless_metrics or {}).items():
        for backend_name, items in backends.items():
            for item in items:
                run_id = item.get("run_id")
                value = item.get("value")

                if run_id not in runs_map:
                    runs_map[run_id] = {
                        "id": run_id,
                        "properties": {},
                        "results": {"sharded_metrics": {}, "shardless_metrics": {}},
                    }

                run_entry = runs_map[run_id]
                shardless_metrics_for_run = run_entry["results"]["shardless_metrics"]
                if metric_name not in shardless_metrics_for_run:
                    shardless_metrics_for_run[metric_name] = {"properties": {}, "backends": {}}

                backends_for_metric = shardless_metrics_for_run[metric_name]["backends"]
                # for shardless, we store a single value per backend per run
                backends_for_metric[backend_name] = {"properties": {}, "value": value}

    # prepare final summary
    runs_list = [
        runs_map[k]
        for k in sorted(
            runs_map.keys(), key=lambda x: (int(x) if isinstance(x, (int, str)) and str(x).isdigit() else str(x))
        )
    ]
    summary_stats = summarize_stats(sharded_metrics, shardless_metrics)
    return Benchmark(runs=runs_list, benchmark=benchmark_info, summary=summary_stats)
