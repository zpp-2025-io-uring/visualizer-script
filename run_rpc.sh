#!/usr/bin/env bash

RPC_TESTER="$(realpath $1)"
CONFIG_PATH="$(realpath $2)"
IP_ADDRESS="127.0.0.2"
CPUS=$(nproc)
HALF_CPUS=$((CPUS/2))

mkdir -p build
cd build

mkdir -p temp

"$RPC_TESTER" --conf "$CONFIG_PATH" --listen "$IP_ADDRESS" --reactor-backend asymmetric_io_uring --cpuset "0-$((HALF_CPUS-1))"&
pid=$!

sleep 1

if "$RPC_TESTER" --conf "$CONFIG_PATH" --connect "$IP_ADDRESS" --reactor-backend asymmetric_io_uring --cpuset "$HALF_CPUS-$((CPUS-1))" > asymmetric.in; then
    echo "Successfully ran asymmetric benchmark"
else
    echo "Asymmetric benchmark failed"
fi

kill $pid
sleep 1
kill -0 $pid && echo "Force-kill asymmetric" && kill -9 $pid
wait $pid

sleep 1

"$RPC_TESTER" --conf "$CONFIG_PATH" --listen "$IP_ADDRESS" --reactor-backend io_uring --cpuset "0-$((HALF_CPUS-1))" &
pid=$!

sleep 1

if "$RPC_TESTER" --conf "$CONFIG_PATH" --connect "$IP_ADDRESS" --reactor-backend io_uring --cpuset "$HALF_CPUS-$((CPUS-1))" > symmetric.in; then
    echo "Successfully ran symmetric benchmark"
else
    echo "Symmetric benchmark failed"
fi

kill $pid
sleep 1
kill -0 $pid && echo "Force-kill symmetric " && kill -9 $pid
wait $pid

rmdir temp

python3 ../main.py