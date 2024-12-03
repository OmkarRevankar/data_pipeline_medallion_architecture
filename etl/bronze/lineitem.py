import duckdb
import polars as pl

def create_dataset(source_database):
    con = duckdb.connect(source_database)
    pulled_df = con.sql(f"select * from lineitem").pl()
    return pulled_df.rename(lambda col_name: col_name[2:]).rename({"orderkey": "order_key"})

