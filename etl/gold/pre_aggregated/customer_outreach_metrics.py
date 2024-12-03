import json
import polars as pl
from datetime import datetime

from utils.run_metrics import get_latest_run_metrics, insert_run_metrics


def create_dataset(wide_lineitem, wide_orders):
    order_lineitem_metrics = wide_lineitem.group_by(pl.col("order_key")).agg(
        pl.col("linenumber").count().alias("num_lineitems")
    )
    return (
        wide_orders.join(order_lineitem_metrics, on="order_key", how="left")
        .group_by(pl.col("customer_key"), pl.col("name").alias("customer_name"))
        .agg(
            pl.min("totalprice").alias("min_order_value"),
            pl.max("totalprice").alias("max_order_value"),
            pl.mean("totalprice").alias("avg_order_value"),
            pl.mean("num_lineitems").alias("avg_num_items_per_order"),
        )
    )


def percentage_difference(val1, val2):
    if val1 == 0 and val2 == 0:
        return 0.0
    elif val1 == 0 or val2 == 0:
        return 100.0
    return (abs((val1 - val2)) / ((val1 + val2) / 2)) * 100


def check_no_duplicates(customer_outreach_metrics_df):
    # check uniqueness
    if (
        customer_outreach_metrics_df.filter(
            customer_outreach_metrics_df.select(pl.col("customer_key")).is_duplicated()
        ).shape[0]
        > 0
    ):
        #raise Exception("Duplicate customer_keys")
        print("Duplicate customer_keys")
        return False
    else:
        return True


def check_variance(customer_outreach_metrics_df,metadata_db, perc_threshold=5):
    prev_metric = get_latest_run_metrics(metadata_db)
    if prev_metric is None or len(prev_metric) == 0:
        return
    prev_metric['sum_avg_order_value'] = int(float(prev_metric['sum_avg_order_value']))
    curr_metric = json.loads(
        customer_outreach_metrics_df.select(
            pl.col("avg_num_items_per_order").alias("sum_avg_num_items_per_order"),
            pl.col("avg_order_value").cast(int).alias("sum_avg_order_value"),
        )
        .sum()
        .write_json()
    )[0]
    comparison = {}
    for key in curr_metric:
        if key in prev_metric:
            comparison[key] = percentage_difference(int(curr_metric[key]), int(prev_metric[key]))
    print("Current Successfully run metrics:{0} {1}".format(datetime.now().strftime('%Y-%m-%d-%H-%M'),
                                                                         curr_metric))
    print("Result for current data percentage comparison:{0} {1}".format(datetime.now().strftime('%Y-%m-%d-%H-%M'),comparison))
    for k, v in comparison.items():
        if int(v) >= perc_threshold:
            print(f"Difference for {k} is greater than 5%: {v}%")
            print("This data is not-valid as there is difference between current and previous pipeline run: our data validation is un-successful")
            return False
            #raise Exception(f"Difference for {k} is greater than 5%: {v}%")
        else:
            return True


def validate_dataset(customer_outreach_metrics_df,metadata_db):
    # data quality checks
    if check_no_duplicates(customer_outreach_metrics_df) and check_variance(customer_outreach_metrics_df,metadata_db):
        return True
    else:
        return False
