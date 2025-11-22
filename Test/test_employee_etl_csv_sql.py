import pandas as pd
from udfs.multiple_functions import *
from Target_queries.tgt_test_employee_etl import tgt_sql
import Source_queries.src_test_employee_etl as src_query
from connection.db_connection import MSSQLConnection
import numpy as np
from datetime import datetime
import os

def test_employee_etl():
    # Database connection parameters
    db = MSSQLConnection(database="Test")
    conn = db.get_connection()

    primary_key = ["employee_id"]
    source_file_path = r"C:\Vikas_Drive\Projects\ETL_Data_Validation_Framework\Test_Data\employee_dataset_10000.csv"

    # Load source data
    src_df, src_shape = load_csv(source_file_path)
    print(f"Source Data Loaded: {src_shape}")

    dept_map = {
        'Market Dept': 'Marketing',
        'Marketing': 'Marketing',
        'MKT': 'Marketing',
        'Hr Dept': 'Human Resources',
        'HR': 'Human Resources',
        'Human Resources': 'Human Resources',
        'H.R.': 'Human Resources',
        'IT': 'Information Technology',
        'I.T.': 'Information Technology',
        'Information Technology': 'Information Technology',
        'OPR': 'Operations',
        'Ops': 'Operations',
        'Operations': 'Operations',
        'FIN': 'Finance',
        'Finance Dept': 'Finance',
        'Finance': 'Finance'
    }

    src_df['Department'] = src_df['Department'].map(dept_map)

    # -------------------------------
    # Working_Days (DATEDIFF logic)
    # -------------------------------
    today = pd.to_datetime(datetime.today().date())
    print("checking today date format",today)

    src_df['Join_Date'] = pd.to_datetime(src_df['Join_Date'], errors='coerce')
    src_df['Leave_Date'] = pd.to_datetime(src_df['Leave_Date'], errors='coerce')

    # Normalize to remove time from both timestamps
    src_df['Join_Date'] = src_df['Join_Date'].dt.normalize()
    src_df['Leave_Date'] = src_df['Leave_Date'].dt.normalize()
    today = pd.to_datetime("today").normalize()

    src_df['Working_Days'] = np.where(
        src_df['Employment_Status'] == 'Active',
        (today - src_df['Join_Date']).dt.days,
        (src_df['Leave_Date'] - src_df['Join_Date']).dt.days
    )

    # Step 2: Convert valid values to int, leave NA as NA
    working_days = pd.to_numeric(src_df['Working_Days'], errors='coerce').astype('Int64')

    src_df['Working_Days'] = working_days


    # -------------------------------
    # Current_Employee_Flag
    # -------------------------------
    src_df['Current_Employee_Flag'] = np.where(
        src_df['Employment_Status'] == 'Active', '1', '0'
    )

    # -------------------------------
    # Promotion Eligibility
    # -------------------------------
    src_df['Promotion_Eligibility'] = np.where(
        (src_df['Employment_Status'] == 'Active') &
        (src_df['Performance_Score'] >= 80) &
        (src_df['Experience_Years'] >= 3),
        'Eligible',
        'Not Eligible'
    )

    # -------------------------------
    # Extract numeric characters from Salary
    # SQL Unicode equivalent: keep only digits
    # -------------------------------
    src_df['Salary_Numeric'] = src_df['Salary'].astype(str).str.replace(r'\D', '', regex=True)

    # -------------------------------
    # Salary Currency Detection
    # -------------------------------
    src_df['Salary_Currency'] = np.where(
        src_df['Salary'].str.contains(r'\$|USD', case=False, na=False),
        'USD',
        np.where(
            src_df['Salary'].str.contains(r'₹|â‚¹|INR', case=False, na=False),
            'INR',
            'UNKNOWN'
        )
    )

    # -------------------------------
    # Action Items Logic
    # -------------------------------
    src_df['Action_Items'] = np.where(
        (src_df['Leave_Date'].notna()) & (src_df['Employment_Status'] == 'Active'),
        'Employee Left but Status Active, Fix Required',
        np.where(
            (src_df['Leave_Date'].isna()) & (src_df['Employment_Status'] != 'Active'),
            'Employee is Active but leave_date mentioned, Fix Required',
            np.where(
                src_df['Leave_Date'] > src_df['Join_Date'],
                'Wrong Entry Flag',
                None
            )
        )
    )

    # Load target data
    tgt_df, tgt_shape = load_sql(tgt_sql, conn)
    print(f"Target Data Loaded: {tgt_shape}")

    return src_df, tgt_df, primary_key

if __name__ == "__main__":
    src_df, tgt_df, primary_key = test_employee_etl()
    compare_dataframes(src_df, tgt_df, primary_key)