import argparse
import os
import time

SALES_FILE = "sales.parquet"
ARTICLES_FILE = "articles.parquet"

def duckdb_command(args):
    import duckdb
    conn = duckdb.connect()

    if args.workers:
        conn.execute(f"SET worker_threads={args.workers};")
    
    start = time.time()
    for i in range(args.repeats):
        first_product_sales = conn.execute(f"""
            SELECT min(week), min(yearday)
            FROM read_parquet('{SALES_FILE}') as sales, read_parquet('{ARTICLES_FILE}') as articles
            WHERE sales.article_id = articles.article_id
            GROUP BY product_code;
        """).fetchall()
    end = time.time()
    avg_time = (end - start)/args.repeats
    print(f"duckdb,,{args.workers},{avg_time}")
    return(avg_time)

def pandas_command(args):
    import pandas as pd

    start = time.time()
    for i in range(args.repeats):
        articles = pd.read_parquet(ARTICLES_FILE, dtype_backend=args.backend)
        sales = pd.read_parquet(SALES_FILE, dtype_backend=args.backend)
        first_product_sales = (
            sales
            .merge(articles, on="article_id")
            .groupby("product_code")
            .agg(
                product_first_week=("week", "min"),
                product_first_yearday=("yearday", "min")
            )
        )
    end = time.time()
    avg_time = (end - start)/args.repeats
    print(f"pandas,{args.backend},1,{avg_time}")
    return(avg_time)

def polars_command(args):
    if args.workers:
        os.environ["POLARS_MAX_THREADS"] = str(args.workers)
    import polars as pl
    if args.lazy:
        start = time.time()
        for i in range(args.repeats):
            articles = pl.scan_parquet(ARTICLES_FILE)
            sales = pl.scan_parquet(SALES_FILE)
            first_product_sales = (
                sales
                .join(articles, on="article_id")
                .groupby("product_code")
                .agg(
                    pl.col("week").min().alias("product_first_week"),
                    pl.col("yearday").min().alias("product_first_yearday"),
                )
                .collect()
            )
        end = time.time()
        avg_time = (end - start)/args.repeats
        print(f"polars,lazy,{args.workers},{avg_time}")
        return(avg_time)
    else:
        start = time.time()
        for i in range(args.repeats):
            articles = pl.read_parquet(ARTICLES_FILE)
            sales = pl.read_parquet(SALES_FILE)
            first_product_sales = (
                sales
                .join(articles, on="article_id")
                .groupby("product_code")
                .agg(
                    pl.col("week").min().alias("product_first_week"),
                    pl.col("yearday").min().alias("product_first_yearday"),
                )
            )
        end = time.time()
        avg_time = (end - start)/args.repeats
        print(f"polars,eager,{args.workers},{avg_time}")
        return(avg_time)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()

    parser_pandas = subparsers.add_parser(
        "pandas"
    )
    parser_pandas.add_argument("--backend", type=str, choices=["numpy_nullable", "pyarrow"])
    parser_pandas.add_argument("--repeats", type=int, default=1)
    parser_pandas.set_defaults(func=pandas_command)

    parser_duckdb = subparsers.add_parser(
        "duckdb"
    )
    parser_duckdb.add_argument("--repeats", type=int, default=1)
    parser_duckdb.set_defaults(func=duckdb_command)
    parser_duckdb.add_argument("--workers", type=int)

    parser_polars = subparsers.add_parser(
        "polars"
    )
    parser_polars.add_argument('--lazy', action=argparse.BooleanOptionalAction)
    parser_polars.add_argument("--repeats", type=int, default=1)
    parser_polars.add_argument("--workers", type=int)
    parser_polars.set_defaults(func=polars_command)

    args = parser.parse_args()

    args.func(args)
