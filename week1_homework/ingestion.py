#!/usr/bin/env python
# coding: utf-8

import argparse
from time import time
import pandas as pd
from sqlalchemy import create_engine
import yaml


def ingest_file(engine, file_name, table_name):
    """Helper function to load one file into one table."""
    compression = "gzip" if file_name.endswith(".gz") else None
    print(f"Starting ingestion for file: {file_name} → table: {table_name}")

    # Read CSV in chunks
    df_iter = pd.read_csv(file_name, iterator=True, chunksize=100000, compression=compression)
    df = next(df_iter)

    # 🔹 Make columns lowercase
    df.columns = [col.lower() for col in df.columns]

    # Convert datetime columns if they exist
    datetime_cols = [col for col in ["tpep_pickup_datetime", "tpep_dropoff_datetime"] if col in df.columns]
    for col in datetime_cols:
        df[col] = pd.to_datetime(df[col])

    # Create table schema
    df.head(0).to_sql(name=table_name, con=engine, if_exists="replace")
    print(f"Created table '{table_name}'")

    # Insert first chunk
    df.to_sql(name=table_name, con=engine, if_exists="append")
    print(f"Inserted first chunk ({len(df)} rows)")

    # Insert remaining chunks
    chunk_idx = 1
    while True:
        try:
            t_start = time()
            df = next(df_iter)

            # 🔹 Lowercase columns for each chunk
            df.columns = [col.lower() for col in df.columns]

            for col in datetime_cols:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col])
            df.to_sql(name=table_name, con=engine, if_exists="append")
            t_end = time()
            chunk_idx += 1
            print(f"Inserted chunk {chunk_idx} in {t_end - t_start:.2f} seconds")
        except StopIteration:
            print(f"Finished ingesting {file_name} into {table_name}.")
            break



def main(params):
    engine = create_engine(f"postgresql://{params.user}:{params.password}@{params.host}:{params.port}/{params.db}")
    print(f"Connected to Postgres database: {params.db}")

    files = params.file_name.split(',')
    tables = params.table_name.split(',')

    if len(files) != len(tables):
        raise ValueError("The number of files and table names must be equal!")

    for file_name, table_name in zip(files, tables):
        ingest_file(engine, file_name.strip(), table_name.strip())



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest multiple CSV/CSV.GZ files to Postgres")
    parser.add_argument("--config", help="Path to YAML config file")
    parser.add_argument("--user")
    parser.add_argument("--password")
    parser.add_argument("--host")
    parser.add_argument("--port")
    parser.add_argument("--db")
    parser.add_argument("--table_name")
    parser.add_argument("--file_name")
    args = parser.parse_args()

    # If config file provided, load values
    if args.config:
        with open(args.config) as f:
            cfg = yaml.safe_load(f)
        for key, val in cfg.items():
            setattr(args, key, val)

    main(args)

