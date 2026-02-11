from typing import Any, Generic, TypeVar

from yaml import safe_load
from yamlable import YamlAble, yaml_info

from log import get_logger
from stats import ShardedMetricRunMeasurement, ShardlessMetricRunMeasurement, Stats, summarize_stats
from tree import TreeDict

logger = get_logger()

T = TypeVar("T")


@yaml_info("benchmark_results")
class PerBenchmarkResults(Generic[T], YamlAble):
    def __init__(self, backends: dict[str, dict[str, T]], properties: dict[str, Any]) -> None:
        self.backends = backends
        self.properties = properties

    def __repr__(self) -> str:
        return f"PerBenchmarkResults(backends={self.backends}, properties={self.properties})"

    @classmethod
    def default(cls) -> "PerBenchmarkResults":
        return cls(backends={}, properties={})


@yaml_info("results")
class Results(YamlAble):
    def __init__(
        self,
        sharded_metrics: TreeDict[PerBenchmarkResults[Any]],
        shardless_metrics: TreeDict[PerBenchmarkResults[Any]],
    ) -> None:
        self.sharded_metrics = sharded_metrics
        self.shardless_metrics = shardless_metrics

    def __repr__(self) -> str:
        return f"Results(sharded_metrics={self.sharded_metrics}, shardless_metrics={self.shardless_metrics})"

    @classmethod
    def default(cls) -> "Results":
        return cls(sharded_metrics=TreeDict(), shardless_metrics=TreeDict())


@yaml_info("run_summary")
class RunSummary(YamlAble):
    def __init__(self, id: int, properties: dict, results: Results) -> None:
        self.id = id
        self.properties = properties
        self.results = results

    def __repr__(self) -> str:
        return f"RunSummary(id={self.id}, properties={self.properties}, results={self.results})"


@yaml_info("benchmark")
class Benchmark(YamlAble):
    def __init__(self, runs: list[RunSummary], benchmark: dict, summary: Stats, run_count: int = None) -> None:
        self.runs = runs
        self.benchmark = benchmark
        self.summary = summary
        self.run_count = run_count if run_count is not None else len(runs)
        logger.debug(
            f"Initialized benchmark with runs={self.runs}, benchmark={self.benchmark}, summary={self.summary}, run_count={self.run_count}"
        )

    def get_runs(self) -> list[RunSummary]:
        return self.runs

    def get_benchmark(self) -> dict:
        return self.benchmark

    def get_stats(self) -> Stats:
        return self.summary

    def get_run_count(self) -> int:
        return self.run_count

    @classmethod
    def load_from_file(cls, file):
        """Load a benchmark summary from a YAML file and return a `benchmark` instance.

        This helper accepts both tagged yamlable documents and legacy plain mappings
        (no `!yamlable/...` tag). It prefers the yamlable loading path when possible,
        otherwise it normalizes the mapping and constructs the object.
        """
        logger.debug(f"Loading benchmark from file {file}")
        data = safe_load(file)

        if isinstance(data, cls):
            return data

        if isinstance(data, dict):
            return cls.__from_yaml_dict__(data, yaml_tag=None)

        raise TypeError(f"Cannot load benchmark: unexpected YAML document type {type(data)}")

    def __repr__(self) -> str:
        return f"Benchmark(runs={self.runs}, benchmark={self.benchmark}, summary={self.summary})"


def compute_benchmark_summary(
    sharded_metrics: TreeDict[dict[str, list[ShardedMetricRunMeasurement]]],
    shardless_metrics: TreeDict[dict[str, list[ShardlessMetricRunMeasurement]]],
    benchmark_info: dict,
) -> Benchmark:
    # build map run_id -> run entry
    runs_map: dict[int, RunSummary] = {}

    logger.debug(f"Computing benchmark summary {sharded_metrics=}, {shardless_metrics=}, {benchmark_info=}")

    # process sharded metrics
    for metric_name, backends in (sharded_metrics or {}).items():
        for backend_name, items in backends.items():
            for item in items:
                run_id = item.run_id
                shard = item.shard
                value = item.value

                if run_id not in runs_map:
                    runs_map[run_id] = RunSummary(
                        id=run_id,
                        properties={},
                        results=Results(sharded_metrics=TreeDict(), shardless_metrics=TreeDict()),
                    )

                run_entry = runs_map[run_id]
                backends_for_metric = run_entry.results.sharded_metrics.setdefault(
                    metric_name, PerBenchmarkResults[Any].default()
                ).backends

                if backend_name not in backends_for_metric:
                    backends_for_metric[backend_name] = {"properties": {}, "shards": []}

                backends_for_metric[backend_name]["shards"].append({"shard": shard, "value": value})

    # process shardless metrics
    for metric_name, backends in (shardless_metrics or {}).items():
        for backend_name, items in backends.items():
            for item in items:
                run_id = item.run_id
                value = item.value

                if run_id not in runs_map:
                    runs_map[run_id] = RunSummary(
                        id=run_id,
                        properties={},
                        results=Results(sharded_metrics=TreeDict(), shardless_metrics=TreeDict()),
                    )

                run_entry = runs_map[run_id]
                backends_for_metric = run_entry.results.shardless_metrics.setdefault(
                    metric_name, PerBenchmarkResults[Any].default()
                ).backends
                backends_for_metric[backend_name] = {"properties": {}, "value": value}

    # prepare final summary
    runs_list = [
        runs_map[k]
        for k in sorted(
            runs_map.keys(), key=lambda x: int(x) if isinstance(x, (int, str)) and str(x).isdigit() else str(x)
        )
    ]
    summary_stats = summarize_stats(sharded_metrics, shardless_metrics)
    return Benchmark(runs=runs_list, benchmark=benchmark_info, summary=summary_stats)
