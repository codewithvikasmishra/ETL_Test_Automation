import pandas as pd
import os
import inspect

# -----------------------------------------------------------
# 1. Load and prepare data
# -----------------------------------------------------------

def load_csv(path):
    print(f"Reading file: {path} ...")
    df = pd.read_csv(path, header=0, encoding='utf-8')
    shape_info = f"{df.shape[0]} rows and {df.shape[1]} columns"
    return df, shape_info


def load_parquet(path):
    print(f"Reading Parquet file: {path} ...")
    df = pd.read_parquet(path)
    shape_info = f"{df.shape[0]} rows and {df.shape[1]} columns"
    return df, shape_info

def load_excel(path, sheet_name=0):
    print(f"Reading Excel file: {path} ...")
    df = pd.read_excel(path, sheet_name=sheet_name)
    shape_info = f"{df.shape[0]} rows and {df.shape[1]} columns"
    return df, shape_info

def load_json(path):
    print(f"Reading JSON file: {path} ...")
    df = pd.read_json(path)
    shape_info = f"{df.shape[0]} rows and {df.shape[1]} columns"
    return df, shape_info

def load_sql(query_or_table, engine):
    print("Reading SQL data...")
    df = pd.read_sql(query_or_table, con=engine)
    shape_info = f"{df.shape[0]} rows and {df.shape[1]} columns"
    return df, shape_info

def standardize_dataframe(df):
    df = df.reindex(columns=sorted(df.columns))
    df.drop_duplicates(inplace=True)
    df = df.astype(str)
    df.columns = df.columns.str.strip().str.lower()
    return df

# -----------------------------------------------------------
# 2. Primary key cleaning & duplicate removal
# -----------------------------------------------------------

def clean_primary_key_columns(df, primary_key):
    print("Cleaning primary key columns...")
    for key in primary_key:
        df[key] = df[key].astype(str).str.strip()
    return df


def remove_pk_duplicates(df, primary_key):
    print("Removing duplicates based on primary key...")
    df.drop_duplicates(subset=primary_key, inplace=True)
    df = df.reset_index(drop=True)
    return df


# -----------------------------------------------------------
# 3. Core comparison logic
# -----------------------------------------------------------

def find_extra_rows(df_main, df_compare, primary_key):
    print("Finding extra rows...")
    return df_main[
        ~df_main[primary_key].apply(tuple, axis=1).isin(
            df_compare[primary_key].apply(tuple, axis=1)
        )
    ]


# def find_common_rows(df1, df2, primary_key):
#     print("Finding common rows...")
#     mask = df1[primary_key].apply(tuple, axis=1).isin(
#         df2[primary_key].apply(tuple, axis=1)
#     )
#     return df1[mask].sort_values(by=primary_key), df2[mask].sort_values(by=primary_key)


# def compare_common_rows(df1_common, df2_common):
#     print("Comparing common rows...")
#     return df1_common.compare(df2_common, keep_equal=False)

def find_common_rows(df1, df2, primary_key):
    print("Finding common rows...")

    df1_pk = df1[primary_key].apply(tuple, axis=1)
    df2_pk = df2[primary_key].apply(tuple, axis=1)

    common_keys = set(df1_pk).intersection(set(df2_pk))

    df1_common = df1[df1_pk.isin(common_keys)].copy()
    df2_common = df2[df2_pk.isin(common_keys)].copy()

    df1_common = df1_common.sort_values(by=primary_key).reset_index(drop=True)
    df2_common = df2_common.sort_values(by=primary_key).reset_index(drop=True)

    # align columns exactly
    common_cols = sorted(set(df1_common.columns).intersection(set(df2_common.columns)))
    df1_common = df1_common[common_cols]
    df2_common = df2_common[common_cols]

    return df1_common, df2_common

def compare_common_rows(df1, df2):
    df1, df2 = df1.align(df2)   # ensures perfect alignment
    return df1.compare(df2, keep_equal=False)



# -----------------------------------------------------------
# 4. Reporting and Excel output
# -----------------------------------------------------------

def build_overview(product_tested, source_shape, target_shape, primary_key,
                   extra_src, extra_tgt, differences):
    print("Building comparison overview...")
    
    status = "Passed" if len(extra_src)==0 and len(extra_tgt)==0 and len(differences)==0 else "Failed"
    
    return pd.DataFrame({
        "Product Tested": [product_tested],
        "No. of Rows X Col in Source": [source_shape],
        "No. of Rows X Col in Target": [target_shape],
        "Primary Key": [", ".join(primary_key)],
        "Extra Rows in Source": [len(extra_src)],
        "Extra Rows in Target": [len(extra_tgt)],
        "Mismatched Records": [len(differences)],
        "Status": [status]
    })


def write_to_excel(output_file, overview, differences, extra_src, extra_tgt):
    print("Writing results to output file...")

    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        overview.to_excel(writer, sheet_name='Compare Overview', index=False)

        (differences.head(100) if not differences.empty else
         pd.DataFrame({"Message":["No differences found"]})
        ).to_excel(writer, "Differences in Rows (Sample)")

        (extra_src.head(100) if not extra_src.empty else
         pd.DataFrame({"Message":["No extra rows in source"]})
        ).to_excel(writer, "Extra Rows in Source (Sample)", index=False)

        (extra_tgt.head(100) if not extra_tgt.empty else
         pd.DataFrame({"Message":["No extra rows in target"]})
        ).to_excel(writer, "Extra Rows in Target (Sample)", index=False)


# -----------------------------------------------------------
# 5. File_naming utility
# -----------------------------------------------------------

def generate_output_file(fixed_folder=r"C:\Vikas_Drive\Projects\ETL_Data_Validation_Framework\Test_Report", caller_depth=2):
    """
    Auto-generates an Excel output file based on the calling Python script name.

    Parameters:
        fixed_folder (str): Folder where output Excel file must be saved.
        caller_depth (int): Stack depth to detect calling script (default=2).

    Returns:
        str: Final output file path.
    """

    # Get the calling script
    caller_frame = inspect.stack()[caller_depth]
    caller_file_path = caller_frame.filename
    caller_file = os.path.basename(caller_file_path)
    caller_name = os.path.splitext(caller_file)[0]

    # Ensure folder exists
    os.makedirs(fixed_folder, exist_ok=True)

    # Build output file path
    output_file = os.path.join(fixed_folder, f"{caller_name}.xlsx")

    print(f"Auto-generated output file: {output_file}")
    return output_file, caller_name

# -----------------------------------------------------------
# 6. Main Orchestrator (same logic as your original function)
# -----------------------------------------------------------

def compare_dataframes(src_df, tgt_df, primary_key):

    try:
        output_file, file_name = generate_output_file()
        print("Starting DataFrame comparison...")
        # Validate input
        if not isinstance(src_df, pd.DataFrame):
            raise TypeError("src_df must be a Pandas DataFrame. Load using load_csv/load_sql/etc.")
        if not isinstance(tgt_df, pd.DataFrame):
            raise TypeError("tgt_df must be a Pandas DataFrame. Load using load_csv/load_sql/etc.")

        # Shapes for overview
        source_shape = f"{src_df.shape[0]} rows and {src_df.shape[1]} columns"
        target_shape = f"{tgt_df.shape[0]} rows and {tgt_df.shape[1]} columns"

        # Standardize
        df1 = standardize_dataframe(src_df)
        df2 = standardize_dataframe(tgt_df)

        # PK handling
        primary_key = [key.lower() for key in primary_key]
        df1 = clean_primary_key_columns(df1, primary_key)
        df2 = clean_primary_key_columns(df2, primary_key)

        df1 = remove_pk_duplicates(df1, primary_key)
        df2 = remove_pk_duplicates(df2, primary_key)

        # Comparison steps
        extra_in_source = find_extra_rows(df1, df2, primary_key)
        extra_in_target = find_extra_rows(df2, df1, primary_key)

        common1, common2 = find_common_rows(df1, df2, primary_key)
        differences = compare_common_rows(common1, common2)

        # Overview
        overview = build_overview(
            product_tested=file_name,
            source_shape=source_shape,
            target_shape=target_shape,
            primary_key=primary_key,
            extra_src=extra_in_source,
            extra_tgt=extra_in_target,
            differences=differences,
        )

        # Write results
        write_to_excel(output_file, overview, differences, extra_in_source, extra_in_target)

    except Exception as e:
        print(f"Error occurred during comparison: {e}")