#!/usr/bin/env bash

BUILD_DIR="$(realpath $1)"
MAIN_DIR="$(realpath './main.py')"

cd "$BUILD_DIR"
python3 "$MAIN_DIR"