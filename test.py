from generate import generate_graphs_for_summary;
from parse import read_results_for_benchmark;

results = read_results_for_benchmark("/home/mszopa/uw/bachelors/io_tester_visualizer/results/better_graphs/08-01-2026_11:12:54/rpc_64kB_stream_unidirectional/metrics_summary.yaml")
print(results)

generate_graphs_for_summary(
    runs=results['runs'],
    stats=results['summary'],
    build_dir="./graphs_test")