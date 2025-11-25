import os
import base64
import boto3
import configparser
import pandas as pd
import io
import logging

# ======================================================
#  LOGGER CONFIGURATION
# ======================================================
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)

# ======================================================
#  AWS CREDENTIAL RESOLVER
# ======================================================
def _get_aws_creds(aws_cred_local_file_path):
    """
    Returns AWS Access Key + Secret Key.
    Reads from either:
        - Jenkins environment variables, OR
        - Local .ini file (base64 encoded secret)
    """

    # Prefer Jenkins if available
    aws_id = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY")

    if aws_id and aws_secret:
        log.info("Using AWS credentials from Jenkins environment variables.")
        return aws_id, aws_secret

    # Local file
    if not os.path.isabs(aws_cred_local_file_path):
        aws_cred_local_file_path = os.path.abspath(aws_cred_local_file_path)

    config = configparser.ConfigParser()
    config.read(aws_cred_local_file_path)

    ACCESS_KEY_ID = config["AWS_Credentials"]["ACCESS_KEY_ID"]
    SECRET_KEY = base64.b64decode(config["AWS_Credentials"]["SECRET_KEY"]).decode()

    log.info(f"Using AWS credentials from file: {aws_cred_local_file_path}")

    return ACCESS_KEY_ID, SECRET_KEY

# ======================================================
#  S3 FILE UPLOAD
# ======================================================
def upload_file_to_s3(local_filepath, bucket_name, s3_key, aws_creds_file):
    ACCESS_KEY_ID, SECRET_KEY = _get_aws_creds(aws_creds_file)

    client = boto3.client(
        "s3",
        aws_access_key_id=ACCESS_KEY_ID,
        aws_secret_access_key=SECRET_KEY
    )

    client.upload_file(local_filepath, bucket_name, s3_key)
    log.info(f"Uploaded → {local_filepath} → s3://{bucket_name}/{s3_key}")


# ======================================================
#  S3 FILE DOWNLOAD
# ======================================================
def download_file_from_s3(save_path, bucket_name, s3_key, aws_creds_file):
    ACCESS_KEY_ID, SECRET_KEY = _get_aws_creds(aws_creds_file)

    client = boto3.client(
        "s3",
        aws_access_key_id=ACCESS_KEY_ID,
        aws_secret_access_key=SECRET_KEY
    )

    client.download_file(bucket_name, s3_key, save_path)
    log.info(f"Downloaded → s3://{bucket_name}/{s3_key} → {save_path}")


# ======================================================
#  READ S3 FILE DIRECTLY INTO DATAFRAME
# ======================================================
def read_s3_to_dataframe(bucket_name, s3_key, aws_creds_file, file_type=None, fw_schema=None):
    """
    Reads an S3 file directly into a DataFrame.
    
    Supports:
        - CSV
        - Excel
        - Parquet
        - JSON
        - Fixed-width (fwf)
    """
    ACCESS_KEY_ID, SECRET_KEY = _get_aws_creds(aws_creds_file)

    client = boto3.client(
        "s3",
        aws_access_key_id=ACCESS_KEY_ID,
        aws_secret_access_key=SECRET_KEY
    )

    log.info(f"Reading S3 file → s3://{bucket_name}/{s3_key}")

    obj = client.get_object(Bucket=bucket_name, Key=s3_key)
    data = io.BytesIO(obj["Body"].read())

    # Auto-detect extension
    if file_type is None:
        file_type = s3_key.split(".")[-1].lower()

    # -------------------------------------
    # FILE TYPE HANDLING
    # -------------------------------------
    if file_type == "csv":
        df = pd.read_csv(data)

    elif file_type in ["xlsx", "xls"]:
        df = pd.read_excel(data)

    elif file_type == "parquet":
        df = pd.read_parquet(data)

    elif file_type == "json":
        df = pd.read_json(data)

    elif file_type in ["fwf", "fixed", "txt", "dat"]:
        if not fw_schema:
            raise ValueError("fw_schema (column specs) required for fixed-width files.")

        colspecs = [(col[1], col[2]) for col in fw_schema]
        names = [col[0] for col in fw_schema]

        df = pd.read_fwf(data, colspecs=colspecs, names=names)

    else:
        raise ValueError(f"Unsupported file type: {file_type}")

    shape_info = f"{df.shape[0]} rows × {df.shape[1]} columns"
    log.info(f"S3 Read Complete: {shape_info}")

    return df, shape_info
