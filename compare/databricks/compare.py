import pandas as pd
from azure.storage.filedatalake import DataLakeServiceClient

def compare_dataframes(src_df, tgt_df, output_file, primary_key):
    try:
        # Read input files
        print("Reading source file...")
        spark_src_df1 = spark.read.csv(src_df, header=True, inferSchema=True)
        src_df1 = spark_src_df1.toPandas()
        df1 = src_df1.reindex(columns=sorted(src_df1.columns))
        source_shape = f"{df1.shape[0]} rows and {df1.shape[1]} columns"

        print("Reading target file...")
        spark_src_df2 = spark.read.csv(tgt_df, header=True, inferSchema=True)
        src_df2 = spark_src_df2.toPandas()
        df2 = src_df2.reindex(columns=sorted(src_df2.columns))
        target_shape = f"{df2.shape[0]} rows and {df2.shape[1]} columns"

        # Remove duplicates from each dataset for clean comparison
        df1.drop_duplicates(inplace=True)
        df2.drop_duplicates(inplace=True)

        # Ensure all columns are of type string and remove spaces from colunn names
        df1 = df1.astype(str)
        df2 = df2.astype(str)
        df1.columns = df1.columns.str.strip().str.lower()
        df2.columns = df2.columns.str.strip().str.lower()

        # Prepare primary key columns (strip whitespace)
        print("Cleaning primary key columns...")
        primary_key = [key.lower() for key in primary_key]
        for key in primary_key:
            df1[key] = df1[key].astype(str).str.strip()
            df2[key] = df2[key].astype(str).str.strip()

        # Remove duplicates based on primary key
        df1.drop_duplicates(subset=primary_key, inplace=True)
        df2.drop_duplicates(subset=primary_key, inplace=True)

        # Identify extra rows in Source File
        print("Identifying extra rows in source file...")
        extra_in_source = df1[~df1[primary_key].apply(tuple, axis=1).isin(df2[primary_key].apply(tuple, axis=1))]

        # Identify extra rows in Target File
        print("Identifying extra rows in target file...")
        extra_in_target = df2[~df2[primary_key].apply(tuple, axis=1).isin(df1[primary_key].apply(tuple, axis=1))]

        # Compare matching rows for cell wise differences
        print("Finding differences in shared rows...")
        common_rows_df1 = df1[df1[primary_key].apply(tuple, axis=1).isin(df2[primary_key].apply(tuple, axis=1))]
        common_rows_df2 = df2[df2[primary_key].apply(tuple, axis=1).isin(df1[primary_key].apply(tuple, axis=1))]

        common_rows_df1 = common_rows_df1.sort_values(by=primary_key)
        common_rows_df2 = common_rows_df2.sort_values(by=primary_key)

        differences = common_rows_df1.compare(common_rows_df2)

        #Sort results by readability
        print("Sorting results...")
        extra_in_source = extra_in_source.sort_values(by=primary_key)
        extra_in_target = extra_in_target.sort_values(by=primary_key)
        differences = differences.sort_index()

        # Create samples (limit to first 100 rows)
        differences_sample = differences.head(100)
        extra_in_source_sample = extra_in_source.head(100)
        extra_in_target_sample = extra_in_target.head(100)

        # Count results for the Compare Overview tab
        num_extra_in_source = len(extra_in_source)
        num_extra_in_target = len(extra_in_target)
        num_differences = len(differences)

        # Generate overview status
        print("Generating comparision overview...")
        product_tested = "Product Tested Example"
        status = "Passed" if num_extra_in_source == 0 and num_extra_in_target == 0 and num_differences == 0 else "Failed"

        compare_overview = pd.DataFrame({
            "Product Tested": [product_tested],
            "No. of Rows X Col in Source": [source_shape],
            "No. of Rows X Col in Target": [target_shape],
            "Primary Key": [", ".join(primary_key)],
            "Extra Rows in Source": [num_extra_in_source],
            "Extra Rows in Target": [num_extra_in_target],
            "Mismatched Records": [num_differences],
            "Status": [status]
        })

        local_file_path = f"/Workspace/{user_id}/{output_file_name}.xlsx"

        # Write everything to an Excel file with multiple sheets
        print("Writing results to {local_file_path}...")
        with pd.ExcelWriter(local_file_path, engine='openpyxl') as writer:
            compare_overview.to_excel(writer, sheet_name='Compare Overview', index=False)
            
            # Write sample data for Differences
            if not differences_sample.empty:
                differences_sample.to_excel(writer, sheet_name='Differences in Rows (Sample)', index=True)
            else:
                pd.DataFrame({"Message":["No differences found"]}).to_excel(writer, sheet_name='Differences in Rows (Sample)', index=False)
            
            # Write sample data for Extra Rows in Source
            if not extra_in_source_sample.empty:
                extra_in_source_sample.to_excel(writer, sheet_name='Extra Rows in Source (Sample)', index=False)
            else:
                pd.DataFrame({"Message":["No extra rows in source"]}).to_excel(writer, sheet_name='Extra Rows in Source (Sample)', index=False)

            # Write sample data for Extra Rows in Target
            if not extra_in_target_sample.empty:
                extra_in_target_sample.to_excel(writer, sheet_name='Extra Rows in Target (Sample)', index=False)
            else:   
                pd.DataFrame({"Message":["No extra rows in target"]}).to_excel(writer, sheet_name='Extra Rows in Target (Sample)', index=False)

            print("local file path", local_file_path)
            dbutils.fs.cp(f"file:{local_file_path}", output_file, True)
    except Exception as e:
        print(f"Error occurred during comparision: {e}")

# --------------------------------------------
# Next cell
dbutils.widgets.text("user_id", "")
user_id = dbutils.widgets.get("user_id")

dbutils.widgets.text("output_file_name", "")
output_file_name = dbutils.widgets.get("output_file_name")

# --------------------------------------------
# Next cell
source_file = "dbfs:/FileStore/shared_uploads/sample_source.csv"
target_file = "dbfs:/FileStore/shared_uploads/sample_target.csv"
output_file = f"dbfs:/FileStore/{user_id}/{output_file_name}.xlsx"
primary_key = ["ID", "Name"]

compare_dataframes(source_file, target_file, output_file, primary_key)
