from etl.bronze import customer, lineitem, nation, orders, region
from etl.silver import dim_customer, fct_lineitem, fct_orders
from etl.gold.obt import wide_lineitem, wide_orders
from etl.gold.pre_aggregated import customer_outreach_metrics
from utils.run_metrics import insert_run_metrics,read_run_metrics
from datetime import datetime
import polars as pl

tpch_db = 'd:\\dataengineering_projects\\data_pipeline_medallion_architecture\\database\\tpch.db'
metadata_db="d:\\dataengineering_projects\\data_pipeline_medallion_architecture\\database\\metadata.db"


def create_customer_outreach_metrics():
    # create necessary bronze table
    customer_df = customer.create_dataset(tpch_db)
    lineitem_df = lineitem.create_dataset(tpch_db)
    nation_df = nation.create_dataset(tpch_db)
    orders_df = orders.create_dataset(tpch_db)
    region_df = region.create_dataset(tpch_db)

    # Create silver tables
    dim_customer_df = dim_customer.create_dataset(customer_df, nation_df, region_df)
    fct_lineitem_df = fct_lineitem.create_dataset(lineitem_df)
    fct_orders_df = fct_orders.create_dataset(orders_df)

    # Create gold obt tables
    wide_lineitem_df = wide_lineitem.create_dataset(fct_lineitem_df)
    wide_orders_df = wide_orders.create_dataset(fct_orders_df, dim_customer_df)

    # create gold pre-aggregated tables
    customer_outreach_metrics_df = customer_outreach_metrics.create_dataset(
        wide_lineitem_df, wide_orders_df
    )

    print("Pipeline started now for current data processing time:",datetime.now().strftime('%Y-%m-%d-%H-%M'))
    # read the run_metadata table
    read_run_metrics(metadata_db)

    # validate data quality
    if customer_outreach_metrics.validate_dataset(customer_outreach_metrics_df,metadata_db):
        #  get current run's metrics and store in run_metadata table if the data validation is passed else we are
        #  not going to add this metrics in table, validate_dataset method will raise exception if calculation is invalid.
        insert_run_metrics(customer_outreach_metrics_df, metadata_db)
        print("This data is valid as there is no much difference between current and previous pipeline run: our data validation is successful")
        return customer_outreach_metrics_df.limit(10)
    else:
        return "Pipeline Ended with invalid data"


if __name__ == "__main__":
    print(create_customer_outreach_metrics())
