from pathlib import Path

from yaml import safe_load

from parse import load_data
from benchmarks import benchmark_summary_pdf_filename, suite_summary_pdf_filename


class BenchmarkShould:
    def __init__(
        self, output_dir, backends: list[str], sharded_metrics: list[list[str]], shardless_metrics: list[list[str]]
    ):
        self.output_dir = output_dir
        self.backends = backends
        self.sharded_metrics = sharded_metrics
        self.shardless_metrics = shardless_metrics

    def verify_config_file(self, expected_config_name: str, expected_content: dict):
        config_path = Path(self.output_dir) / f"config_{expected_config_name}"
        assert config_path.exists(), f"Config file {config_path} missing"
        parsed_config = safe_load(config_path.read_text())
        assert parsed_config == expected_content, "Parsed config content does not match expected content"

    def verify_suite_file(self, expected_content: dict):
        suite_path = Path(self.output_dir) / "suite.yaml"
        assert suite_path.exists(), f"Suite file {suite_path} missing"
        parsed_suite = safe_load(suite_path.read_text())
        assert parsed_suite == expected_content, "Parsed suite content does not match expected content"

    def assert_dump_environment(self):
        expected_files = ["git_log.txt", "hostname.txt", "lscpu_e.txt", "lscpu.txt"]
        for filename in expected_files:
            file_path = Path(self.output_dir) / filename
            assert file_path.exists(), f"Expected environment dump file {filename} missing in {self.output_dir}"

    def verify_outputs_for_benchmarks(self, benchmarks: list[dict]):
        for benchmark in benchmarks:
            name = benchmark["name"]
            iterations = benchmark["iterations"]
            type_ = benchmark["type"]
            self.__verify_backend_outputs_for_benchmark(name, iterations, type_)

    def verify_summary_files_exists_for_benchmarks(self, benchmarks: list[dict]):
        for benchmark in benchmarks:
            name = benchmark["name"]
            benchmark_dir = Path(self.output_dir) / name
            summary_path = benchmark_dir / "metrics_summary.yaml"
            assert summary_path.exists(), f"Summary file {summary_path} missing"

    def verify_media_for_benchmarks(
        self, benchmarks: list[dict], generate_graphs: bool, generate_summary_graphs: bool, generate_pdf: bool
    ):
        for benchmark in benchmarks:
            name = benchmark["name"]
            iterations = benchmark["iterations"]
            self.__verify_graphs_per_run_for_benchmark(name, iterations, generate_graphs)
            self.__verify_summary_graphs_for_benchmark(name, generate_summary_graphs)
            self.__verify_pdf_for_benchmark(name, generate_pdf)
        self.__verify_summary_pdf(generate_pdf)

    def __verify_backend_outputs_for_benchmark(self, name: str, iterations: int, benchmark_type: str):
        benchmark_dir = Path(self.output_dir) / name
        for i in range(iterations):
            run_dir = benchmark_dir / f"run_{i}"
            for backend in self.backends:
                self.__verify_backend_outputs(run_dir, backend, benchmark_type)

    def __verify_backend_outputs(self, run_dir: Path, backend: str, benchmark_type: str):
        if benchmark_type == "io":
            output_file = run_dir / f"{backend}.out"
            assert output_file.exists(), f"IO output file {output_file} missing"
            parsed = load_data(output_file.read_text())
            assert isinstance(parsed, list), f"Parsed IO data is not a list for backend {backend}"
        elif benchmark_type == "rpc":
            server_file = run_dir / f"{backend}.server.out"
            client_file = run_dir / f"{backend}.client.out"
            assert server_file.exists(), f"RPC server output file {server_file} missing"
            assert client_file.exists(), f"RPC client output file {client_file} missing"
            parsed_rpc = load_data(client_file.read_text())
            assert isinstance(parsed_rpc, list), f"Parsed RPC data is not a list for backend {backend}"

    def __verify_graphs_per_run_for_benchmark(self, name: str, iterations: int, generate_graphs: bool):
        benchmark_dir = Path(self.output_dir) / name
        run_dirs = [f"run_{i}" for i in range(iterations)]
        if generate_graphs:
            expected_files_for_sharded = get_expected_files_for_metrics_per_run_sharded(self.sharded_metrics)
            assert_files_redraw_for_each_run(benchmark_dir, run_dirs, expected_files_for_sharded)

            expected_files_for_shardless = get_expected_files_for_metrics_per_run_shardless(self.shardless_metrics)
            assert_files_redraw_for_each_run(benchmark_dir, run_dirs, expected_files_for_shardless)
        else:
            for run_dir in run_dirs:
                run_path = benchmark_dir / run_dir
                existing_files = list(run_path.glob("*.svg"))
                assert len(existing_files) == 0, f"Expected no graph files in {run_path}, but found some."

    def __verify_summary_graphs_for_benchmark(self, name: str, generate_summary_graphs: bool):
        benchmark_dir = Path(self.output_dir) / name
        expected_summary_files = get_expected_files_for_metrics_summary(self.sharded_metrics + self.shardless_metrics)
        if generate_summary_graphs:
            assert_files(benchmark_dir, expected_summary_files)
        else:
            existing_files = list(benchmark_dir.glob("*.svg"))
            assert len(existing_files) == 0, f"Expected no summary graph files in {benchmark_dir}, but found some."

    def __verify_pdf_for_benchmark(self, name: str, generate_pdf: bool):
        benchmark_dir = Path(self.output_dir) / name
        pdf_file = benchmark_dir / benchmark_summary_pdf_filename(name)
        if generate_pdf:
            assert pdf_file.exists(), f"Expected PDF file {pdf_file} missing"
        else:
            assert not pdf_file.exists(), f"Expected no PDF file {pdf_file}, but found one."

    def __verify_summary_pdf(self, generate_pdf: bool):
        output_dir = Path(self.output_dir)
        pdf_file = output_dir / suite_summary_pdf_filename(output_dir.name)
        if generate_pdf:
            assert pdf_file.exists(), f"Expected summary PDF file {pdf_file} missing"
        else:
            assert not pdf_file.exists(), f"Expected no summary PDF file {pdf_file}, but found one."


def assert_files_redraw_for_each_run(files_path: Path, run_dirs: list[str], expected_files: list[str]) -> None:
    for run_dir in run_dirs:
        run_path = files_path / run_dir
        assert_files(run_path, expected_files)


def assert_files(files_path: Path, expected_files: list[str]):
    missing_files = []
    for expected_file in expected_files:
        full_path = files_path / expected_file
        if not full_path.exists():
            missing_files.append(expected_file)

    if missing_files:
        missing_files_str = "\n".join(missing_files)
        raise AssertionError(f"The following expected files were not found in {files_path}:\n{missing_files_str}")


def get_expected_files_for_metrics_per_run_sharded(metrics: list[list[str]]) -> list[str]:
    per_run_prefixes = ["total_"]
    per_run_suffixes = [""]
    return get_expected_files_for_metrics(metrics, per_run_prefixes, per_run_suffixes)


def get_expected_files_for_metrics_per_run_shardless(metrics: list[list[str]]) -> list[str]:
    per_run_prefixes = [""]
    per_run_suffixes = [""]
    return get_expected_files_for_metrics(metrics, per_run_prefixes, per_run_suffixes)


def get_expected_files_for_metrics_summary(metrics: list[list[str]]) -> list[str]:
    summary_prefixes = [""]
    summary_suffixes = [""]
    return get_expected_files_for_metrics(metrics, summary_prefixes, summary_suffixes)


def get_expected_files_for_metrics(metrics: list[list[str]], prefixes: list[str], suffixes: list[str]) -> list[str]:
    expected_files = []
    for metric in metrics:
        for prefix in prefixes:
            for suffix in suffixes:
                joined_metric = metric_name_from_list(metric)
                expected_files.append(f"{prefix}{joined_metric}{suffix}.svg")
    return expected_files


def metric_name_from_list(metric_path: list[str]) -> str:
    return "_".join(metric_path)
