from pathlib import Path

from benchmark import Benchmark


def test_can_deserialize_before_strong_typying() -> None:
    """
    Before Benchmark was weakly typed, it had list of dicts as runs. Now all it's inner structures are strongly typed dataclasses.
    This test ensures that we can still deserialize old YAML files that have the old structure, without needing to convert them to the new structure first.
    """

    assests_dir = Path(__file__).parent / "assets"
    benchmark_yaml_path = assests_dir / "benchmark_old_structure.yaml"

    with open(benchmark_yaml_path) as f:
        _ = Benchmark.load_from_file(f)
