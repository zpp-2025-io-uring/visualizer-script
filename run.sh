#!/bin/bash

backend="asymmetric_io_uring"

server_cpuset="5-7"
server_async_worker_cpuset="8"
client_cpuset="1-3"
client_async_worker_cpuset="4"
ip="127.0.0.67"

rpc_tester="/home/marcinsz/scylladb/seastar/build/release/apps/rpc_tester/rpc_tester"
suite="/home/marcinsz/visualizer-script/results/poll/2026-03-09_19:22:59/app_7_net_1_sym_7/rpc_throughput_uni_128kB/conf.yaml"
trap 'kill $(jobs -p) 2>/dev/null' EXIT

"$rpc_tester" \
    --conf "$suite" \
    --listen "$ip" \
    --reactor-backend "$backend" \
    --cpuset "$server_cpuset" \
    --async-workers-cpuset "$server_async_worker_cpuset" &

"$rpc_tester" \
    --conf "$suite" \
    --connect "$ip" \
    --reactor-backend "$backend" \
    --cpuset "$client_cpuset" \
    --async-workers-cpuset "$client_async_worker_cpuset" &

wait
