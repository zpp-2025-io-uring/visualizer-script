#!/usr/bin/env bash

IO_TESTER=$1

mkdir -p build
cd build

mkdir -p temp

if $IO_TESTER --conf ../config.yaml --storage './temp' > asynchronous.in; then
    echo "Successfully ran asynchronous banchmark"
else
    echo "Asynchronous benchmark failed"
fi

if $IO_TESTER --conf ../config.yaml --storage './temp' --reactor-backend io_uring > synchronous.in; then
    echo "Successfully ran synchronous banchmark"
else
    echo "Synchronous benchmark failed"
fi

rmdir temp

python3 ../main.py