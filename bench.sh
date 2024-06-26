#!/bin/bash

echo $HOME

pip install pyarrow pandas

rm -rf bench_files
rm client/parquet_files/*

# Create dir to store benchmark parquet files named "bench_files"
mkdir -p bench_files

# This generates benchmark parquet files
python pqt_gen.py --num-rows 1000000 --num-cols 10 --num-files 5

cp bench_files/* server/tests/test_s3_files/

# When server is not available, we put the files on client side local cache
# cp bench_files/* /client/parquet_files/


cd client

# This build and run client/src/benchmark.rs, check the code for details
cargo build --bin benchmark

export CLIENT_FILES_DIR=$HOME/15721-s24-cache2/client/parquet_files/
export SERVER_URL=http://localhost:26380
export RUST_BACKTRACE=full
RUST_BACKTRACE=1 cargo run --package istziio-client --bin benchmark

