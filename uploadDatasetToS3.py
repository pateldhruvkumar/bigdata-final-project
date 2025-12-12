"""
Simple helper script to upload the local Companies House CSV file to S3.

Behavior:
  - Uses your AWS CLI / environment credentials.
  - Uploads the CSV from your local Windows path to the configured S3 bucket.
  - Stores it under the 'raw/' prefix so it matches the pipeline trigger.
"""

import os

import boto3
from botocore.exceptions import ClientError

# ==============================
# CONFIGURATION
# ==============================

# S3 bucket name — keep this in sync with S3_BUCKET_NAME in final-script.py
S3_BUCKET_NAME = "aws-aly6110-final-assignment"

# Local path to the CSV on your machine
LOCAL_CSV_PATH = (
    r"D:\NEU\Subjects\ALY6110\bigdata-final-project\dataset"
    r"\BasicCompanyData.csv"
)

# Destination key in S3 (will create/implicitly use the 'raw/' prefix)
S3_KEY = "raw/BasicCompanyData.csv"

# AWS region (should match the region used elsewhere, e.g. in final-script.py)
AWS_REGION = "us-east-2"


def upload_file():
    """Upload the local CSV file into s3://<bucket>/raw/…"""
    s3 = boto3.client("s3", region_name=AWS_REGION)

    if not os.path.exists(LOCAL_CSV_PATH):
        raise FileNotFoundError(f"Local file not found: {LOCAL_CSV_PATH}")

    file_size_mb = os.path.getsize(LOCAL_CSV_PATH) / (1024 * 1024)

    print("=" * 80)
    print("UPLOADING COMPANY CSV TO S3")
    print("=" * 80)
    print(f"Local file : {LOCAL_CSV_PATH}")
    print(f"File size  : {file_size_mb:.2f} MB")
    print(f"S3 bucket  : {S3_BUCKET_NAME}")
    print(f"S3 key     : {S3_KEY}")
    print("=" * 80)

    try:
        # In S3, "folders" are just key prefixes, so we simply use 'raw/…' in the key.
        s3.upload_file(LOCAL_CSV_PATH, S3_BUCKET_NAME, S3_KEY)
        print("Upload complete.")
        print(f"   s3://{S3_BUCKET_NAME}/{S3_KEY}")
    except ClientError as e:
        print(f"Failed to upload file: {e}")
        raise


if __name__ == "__main__":
    upload_file()