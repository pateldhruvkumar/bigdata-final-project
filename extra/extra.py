"""
Upload CSV to S3 and Trigger Glue Job
=====================================
This script:
1. Uploads the CSV file to S3
2. Waits briefly for S3 event notification
3. Checks if Glue job was triggered automatically
4. If not, manually triggers the Glue job

Author: Dhruvkumar, Poe, Deepkumar
Course: ALY6110 - Data Management and Big Data
"""

import os
import time
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

# ==============================
# CONFIGURATION
# ==============================
AWS_REGION = "us-east-2"
S3_BUCKET_NAME = "aws-aly6110-final-8"
GLUE_JOB_NAME = "CSVPushToDynamoDB8"
DYNAMODB_TABLE_NAME = "company8"

# Local path to the CSV on your machine
LOCAL_CSV_PATH = (
    r"D:\NEU\Subjects\ALY6110\bigdata-final-project\dataset"
    r"\BasicCompanyData.csv"
)

# Destination key in S3
S3_KEY = "raw/BasicCompanyData.csv"

# Initialize AWS clients
s3_client = boto3.client("s3", region_name=AWS_REGION)
glue_client = boto3.client("glue", region_name=AWS_REGION)


def upload_file():
    """Upload the local CSV file to S3."""
    print("\n" + "=" * 80)
    print("📤 STEP 1: UPLOADING CSV TO S3")
    print("=" * 80)

    if not os.path.exists(LOCAL_CSV_PATH):
        raise FileNotFoundError(f"Local file not found: {LOCAL_CSV_PATH}")

    file_size_mb = os.path.getsize(LOCAL_CSV_PATH) / (1024 * 1024)

    print(f"   📁 Local file: {LOCAL_CSV_PATH}")
    print(f"   📊 File size: {file_size_mb:.2f} MB")
    print(f"   📍 Destination: s3://{S3_BUCKET_NAME}/{S3_KEY}")

    try:
        print("   → Uploading (this may take a few minutes for large files)...")
        start_time = time.time()
        
        s3_client.upload_file(LOCAL_CSV_PATH, S3_BUCKET_NAME, S3_KEY)
        
        elapsed = time.time() - start_time
        print(f"   ✓ Upload complete in {elapsed:.1f} seconds")
        print(f"   📍 Location: s3://{S3_BUCKET_NAME}/{S3_KEY}")
        
        return True
        
    except ClientError as e:
        print(f"   ✗ Failed to upload file: {e}")
        raise


def check_glue_job_triggered():
    """Check if Glue job was triggered automatically by S3 event."""
    print("\n" + "=" * 80)
    print("🔍 STEP 2: CHECKING IF GLUE JOB WAS AUTO-TRIGGERED")
    print("=" * 80)

    print("   → Waiting 10 seconds for S3 event notification to propagate...")
    time.sleep(10)

    try:
        response = glue_client.get_job_runs(JobName=GLUE_JOB_NAME, MaxResults=1)

        if response['JobRuns']:
            latest_run = response['JobRuns'][0]
            run_state = latest_run['JobRunState']
            run_id = latest_run['Id']
            started_on = latest_run.get('StartedOn')

            # Check if this run started within the last 60 seconds
            if started_on:
                now = datetime.now(timezone.utc)
                time_diff = (now - started_on.replace(tzinfo=timezone.utc)).total_seconds()

                if time_diff < 60 and run_state in ['STARTING', 'RUNNING', 'SUCCEEDED']:
                    print(f"   ✓ Glue job was auto-triggered!")
                    print(f"   📋 Job Run ID: {run_id}")
                    print(f"   📊 Status: {run_state}")
                    print(f"   ⏱️ Started: {time_diff:.0f} seconds ago")
                    return True

        print("   ⚠ Glue job was NOT auto-triggered by S3 event")
        print("   → Will manually trigger the job...")
        return False

    except Exception as e:
        print(f"   ⚠ Could not check job status: {e}")
        return False


def trigger_glue_job():
    """Manually trigger the Glue job."""
    print("\n" + "=" * 80)
    print("🚀 STEP 3: MANUALLY TRIGGERING GLUE JOB")
    print("=" * 80)

    print(f"   📋 Job Name: {GLUE_JOB_NAME}")
    print(f"   📦 S3 Bucket: {S3_BUCKET_NAME}")
    print(f"   📄 S3 Key: {S3_KEY}")
    print(f"   🗄️ DynamoDB Table: {DYNAMODB_TABLE_NAME}")

    try:
        response = glue_client.start_job_run(
            JobName=GLUE_JOB_NAME,
            Arguments={
                "--S3_BUCKET": S3_BUCKET_NAME,
                "--S3_KEY": S3_KEY,
                "--DYNAMODB_TABLE": DYNAMODB_TABLE_NAME
            }
        )

        job_run_id = response["JobRunId"]
        print(f"   ✓ Glue job started successfully!")
        print(f"   📋 Job Run ID: {job_run_id}")

        return job_run_id

    except glue_client.exceptions.ConcurrentRunsExceededException:
        print("   ⚠ Glue job is already running")
        return None

    except Exception as e:
        print(f"   ✗ Failed to start Glue job: {e}")
        raise


def print_monitoring_info(job_run_id=None):
    """Print helpful monitoring links."""
    print("\n" + "=" * 80)
    print("📊 MONITORING YOUR PIPELINE")
    print("=" * 80)
    print(f"""
   AWS Glue Console:
   https://{AWS_REGION}.console.aws.amazon.com/glue/home?region={AWS_REGION}#/v2/etl-configuration/jobs/view/{GLUE_JOB_NAME}

   CloudWatch Logs:
   https://{AWS_REGION}.console.aws.amazon.com/cloudwatch/home?region={AWS_REGION}#logsV2:log-groups

   DynamoDB Table:
   https://{AWS_REGION}.console.aws.amazon.com/dynamodbv2/home?region={AWS_REGION}#table?name={DYNAMODB_TABLE_NAME}
""")

    if job_run_id:
        print(f"   Job Run ID: {job_run_id}")

    print("=" * 80)


def main():
    print("=" * 80)
    print("🚀 CSV TO DYNAMODB PIPELINE TRIGGER")
    print(f"📅 Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)

    try:
        # Step 1: Upload CSV to S3
        upload_file()

        # Step 2: Check if Glue job was auto-triggered
        auto_triggered = check_glue_job_triggered()

        # Step 3: If not auto-triggered, manually start the job
        if not auto_triggered:
            job_run_id = trigger_glue_job()
        else:
            job_run_id = None

        # Print monitoring info
        print_monitoring_info(job_run_id)

        print(f"\n✅ Pipeline triggered successfully!")
        print(f"📅 Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    except Exception as e:
        print(f"\n❌ Pipeline trigger failed: {e}")
        raise


if __name__ == "__main__":
    main()