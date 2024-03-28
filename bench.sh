#!/bin/bash

pip install pyarrow pandas

rm -rf bench_files

# Create dir to store benchmark parquet files named "bench_files"
mkdir -p bench_files

# This generates benchmark parquet files
python pqt_gen.py --num-rows 10000 --num-cols 10 --num-files 5

cp bench_files/* /server/S3/

# When server is not available, we put the files on client side
# This should be commented in format testing
cp bench_files/* /client/parquet_files/


cd client

# This build and run client/src/benchmark.rs, check the code for details
cargo build --bin benchmark

cargo run --package client --bin benchmark

