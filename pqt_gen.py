import argparse
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
import numpy as np


def create_parquet_file(num_rows, num_cols, identifier, output_dir):
    # Create a DataFrame with random data
    df = pd.DataFrame({f"col{i}": np.random.randn(num_rows) for i in range(num_cols)})

    # Convert the DataFrame to a PyArrow Table
    table = pa.Table.from_pandas(df)

    # Construct the filename with the number of rows, columns, and identifier
    filename = os.path.join(
        output_dir, f"{num_rows}row_{num_cols}col_{identifier}.parquet"
    )

    # Write the table to a Parquet file
    pq.write_table(table, filename)


def main():
    # Set up command-line argument parsing
    parser = argparse.ArgumentParser(description="Create Parquet files.")
    parser.add_argument(
        "--num-rows", type=int, default=100, help="Number of rows in each file"
    )
    parser.add_argument(
        "--num-cols", type=int, default=5, help="Number of columns in each file"
    )
    parser.add_argument(
        "--num-files", type=int, default=3, help="Number of files to create"
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="bench_files",
        help="Output directory for the Parquet files",
    )
    args = parser.parse_args()

    # Ensure the output directory exists
    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)

    # Create the specified number of Parquet files
    for i in range(args.num_files):
        create_parquet_file(args.num_rows, args.num_cols, i, args.output_dir)
        print(
            f"Created file {args.num_rows}row_{args.num_cols}col_{i}.parquet with {args.num_rows} rows and {args.num_cols} columns."
        )


if __name__ == "__main__":
    main()
