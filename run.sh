#!/usr/bin/env bash

IO_TESTER="$(realpath $1)"

mkdir -p build
cd build

mkdir -p temp

if "$IO_TESTER" --conf ../config.yaml --storage './temp' > asymmetric.in; then
    echo "Successfully ran asymmetric benchmark"
else
    echo "Asymmetric benchmark failed"
fi

if "$IO_TESTER" --conf ../config.yaml --storage './temp' --reactor-backend io_uring > symmetric.in; then
    echo "Successfully ran symmetric benchmark"
else
    echo "Symmetric benchmark failed"
fi

rmdir temp

python3 ../main.py