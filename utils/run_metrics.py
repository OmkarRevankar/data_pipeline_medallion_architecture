import json
import polars as pl
import sqlite3
from datetime import datetime


def read_run_metrics(metadata_db):
    # Connect to SQLite database
    conn = sqlite3.connect(metadata_db)
    # Create a cursor object
    cursor = conn.cursor()
    # Fetch the most recent row based on run_id
    cursor.execute(
        """SELECT * FROM run_metadata"""
    )
    # Get the result
    most_recent_row = cursor.fetchall()
    print("All the records of run_table")
    for i in range(0,len(most_recent_row)): print(most_recent_row[i])
    # Close the connection
    conn.close()

def get_latest_run_metrics(metadata_db):
    # Connect to SQLite database
    conn = sqlite3.connect(metadata_db)

    # Create a cursor object
    cursor = conn.cursor()

    # Fetch the most recent row based on run_id
    cursor.execute(
        """
        SELECT * FROM run_metadata
        ORDER BY run_id DESC
        LIMIT 1
    """
    )

    # Get the result
    most_recent_row = cursor.fetchone()
    print("Previous Successfully run metrics:",most_recent_row)
    # Close the connection
    conn.close()
    return (
        json.loads(most_recent_row[1])
        if most_recent_row and len(most_recent_row) > 0
        else None
    )


def insert_run_metrics(customer_outreach_metrics,metadata_db):
    curr_metrics = json.loads(
    customer_outreach_metrics\
    .select(
    pl.col("avg_num_items_per_order").alias("sum_avg_num_items_per_order"),
    pl.col("avg_order_value").cast(int).alias("sum_avg_order_value")
    ).sum().write_json())[0]

    # Connect to SQLite database
    conn = sqlite3.connect(metadata_db)

    # Create a cursor object
    cursor = conn.cursor()
    curr_metrics_json = json.dumps(curr_metrics)

    current_timestamp = datetime.now().strftime('%Y-%m-%d-%H-%M')
    # Insert data into the run_metadata table
    cursor.execute(
        """
        INSERT INTO run_metadata (run_id, metadata)
        VALUES (?, ?)
    """,
        (current_timestamp, curr_metrics_json),
    )
    # Commit the changes and close the connection
    conn.commit()
    conn.close()
