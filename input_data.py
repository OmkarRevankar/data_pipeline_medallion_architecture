import os
import duckdb
import sqlite3
import random

tpch_db = 'd:\\dataengineering_projects\\data_pipeline_medallion_architecture\\database\\tpch.db'
metadata_db="d:\\dataengineering_projects\\data_pipeline_medallion_architecture\\database\\metadata.db"

def clean_up(file):
    # Remove the file if it exists
    if os.path.exists(file):
        os.remove(file)
    else:
        print(f"The file {file} does not exist.")

def create_tpch_data(tpch_db,random_number):
    con = duckdb.connect(
        tpch_db
    )
    con.sql(
        "INSTALL tpch;LOAD tpch;CALL "+"dbgen(sf = 0.0{0}".format(random_number)+");"
    )
    con.commit()
    con.close()

def create_metadata_table(metadata_db):
    # Connect to SQLite database (or create it)
    conn = sqlite3.connect(metadata_db)
    # Create a cursor object
    cursor = conn.cursor()
    # Create the run_metadata table
    cursor.execute(
        """
        CREATE TABLE run_metadata (
            run_id TEXT PRIMARY KEY,
            metadata TEXT
        )
    """
    )
    # Commit the changes and close the connection
    conn.commit()
    conn.close()


if __name__ == "__main__":
    user_input = input("Please enter 1 for first time run else 2: ")
    print("Cleaning all the TPCH data.")
    clean_up(tpch_db)
    print("Creating TPCH input data")
    create_tpch_data(tpch_db,random.randint(1, 10))
    if user_input == "1":
        print("Cleaning run_metadata data")
        clean_up(metadata_db)
        print("Creating metadata table")
        create_metadata_table(metadata_db)