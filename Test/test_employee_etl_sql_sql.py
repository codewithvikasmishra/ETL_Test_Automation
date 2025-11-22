import pandas as pd
from udfs.multiple_functions import *
from Target_queries.tgt_test_employee_etl import tgt_sql
import Source_queries.src_test_employee_etl as src_query
from connection.db_connection import MSSQLConnection
import os

def test_employee_etl():
    # Database connection parameters
    db = MSSQLConnection(database="Test")
    conn = db.get_connection()

    # Load source data
    src_df, src_shape = load_sql(src_query.src_sql, conn)
    print(f"Source Data Loaded: {src_shape}")

    # Load target data
    tgt_df, tgt_shape = load_sql(tgt_sql, conn)
    print(f"Target Data Loaded: {tgt_shape}")

    return src_df, tgt_df

if __name__ == "__main__":
    src_df, tgt_df = test_employee_etl()
    compare_dataframes(src_df, tgt_df, primary_key=["Employee_ID"])