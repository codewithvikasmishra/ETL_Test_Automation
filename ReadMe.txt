📌 ETL Test Automation Framework – Summary for README.txt
We are handling AWS, Azure (With Databrikcs), GCP (Google) and local as well in this Framework.
Like, using AWS - boto3

This framework automates end-to-end ETL data comparison across multiple data sources. It supports validation between:
Source : parquet, CSV, SQL Tables, excel, xsd, API
target :parquet, CSV, SQL Tables, excel, xsd, API

The tool reads data from both source and target, applies preprocessing rules, aligns schemas, compares primary keys, and generates a detailed Excel report with four tabs.
🟦 1. Compare Overview (Tab 1)

This sheet presents a high-level summary of the comparison:
Product Tested
No. of Rows × Columns in Source
No. of Rows × Columns in Target
Primary Key Used for Comparison
Extra Rows in Source
Extra Rows in Target
No. of Mismatched Records
Final Status: PASSED / FAILED

🟩 2. Primary Key Comparison (Tab 2)
Contains all primary-key-level differences:
PKs present in source but missing in target
PKs present in target but missing in source
Duplicate PKs (if any)
A consolidated view to help data teams quickly identify structural gaps

🟨 3. Column-Wise Comparison (Tab 3)
Shows detailed mismatches for each column:
Cell-level differences (source vs. target)
Normalized formatting for dates, numbers, decimals
Highlighted mismatched values
The exact PKs where mismatches occurred
This tab helps pinpoint what is mismatching at a granular level.

🟧 4. Full Data Snapshot (Tab 4)
A combined view showing source and target data side-by-side:
Useful for manual validation and audit purposes
Ensures transparency of the comparison
Helps developers and QA teams trace any transformation differences

✨ Additional Features
Automatic handling of datatypes (int, float, decimal, date, timestamp)
Configurable rules for value normalisation
Intelligent diff engine to avoid false mismatches
Logging for each run
Supports environment-based configuration
Lightweight, modular, and easily extendable for new file formats
2. Differences in Rows (Sample)
3. Extra Rows in Source (Sample)
4. Extra Rows in Target (Sample)

What you need to install, before starting:
pip install openpyxl
pip install pyodbc
pip install numpy
pip install pandas
pip install requests
pip install boto3

Also, please check which driver you need to mention in db_connection.py (like I mentioned "ODBC Driver 17 for SQL Server")
To check this you can use,
import pyodbc
print(pyodbc.drivers())

To encrypt the username and password in base64 encoding
import base64
enc_server = base64.b64encode("YourPassword123".encode()).decode()
print(enc_server)

How to run the code?
python -m Test.test_employee_etl_csv_sql
(Where Test is the folder and test_employee_etl_csv_sql.py is file to execute for comparision)

python -m Test.test_employee_etl_sql_sql
(Where Test is the folder and test_employee_etl_sql_sql.py is file to execute for comparision)

It will create the comparision files under Test_Report folder with the same naming convention
as in python file, like if you will run this command "python -m Test.test_employee_etl_csv_sql"
then comparision file get saved by "test_employee_etl_csv_sql"

Test Data holds the data for practice
1. employee_dataset_10000.csv
2. source_file.csv
3. target_file.csv

I have created a sample transformation logic to implement and check
Where is transformation logic?
Test_Data\transformation_logics.txt

I have used "employee_dataset_10000.csv" for explanation using sql (as source) to sql (as target)
and csv (employee_dataset_10000.csv) and sql (as target)

You can find the sql queries for transaformation under Source_queries/src_test_employee_etl.py (as source)
when same csv data loaded into sql table

You can find the SQL code for target under Target_queries/tgt_test_employee_etl.py (as source)
when same csv data after transformation loaded into sql table

You can find the python code for trasformation under  Test/test_employee_etl_csv_sql.py (as source)