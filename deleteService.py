"""
AWS Teardown Script
===================
This script deletes all AWS resources created by `createService.py`:

- S3 bucket (and all its contents)
- Glue ETL job
- Lambda function
- IAM roles for Lambda and Glue
- S3 → Lambda event notification
- (Optionally) DynamoDB table used by the pipeline

Run this only when you are sure you want to remove the whole pipeline.

Author: Dhruvkumar, Poe, Deepkumar
Course: ALY6110 - Data Management and Big Data
"""

import json

import boto3
from botocore.exceptions import ClientError


# ============================================
# CONFIGURATION (must match createService.py)
# ============================================
AWS_REGION = "us-east-2"
S3_BUCKET_NAME = "aws-aly6110-final-assignment"
GLUE_JOB_NAME = "DataPushToDynamoDB"
LAMBDA_FUNCTION_NAME = "S3ToGlueTrigger"
DYNAMODB_TABLE_NAME = "company"
LAMBDA_ROLE_NAME = "LambdaGlueTriggerRole"
GLUE_ROLE_NAME = "GlueETLServiceRole"
CSV_UPLOAD_PREFIX = "raw/"


# Initialize clients
session = boto3.Session(region_name=AWS_REGION)
s3_client = session.client("s3")
lambda_client = session.client("lambda")
glue_client = session.client("glue")
iam_client = session.client("iam")
dynamodb_client = session.client("dynamodb")

# Remove the S3 → Lambda trigger and the Lambda permission that allows S3 to invoke it.
# This "disconnects" S3 from Lambda so no new events will fire after teardown.
def delete_s3_trigger_and_lambda_permission():
    """Remove S3 event notification and Lambda invoke permission."""
    print("\n" + "=" * 80)
    print("STEP 1: REMOVING S3 EVENT TRIGGER & LAMBDA PERMISSION")
    print("=" * 80)

    # 1) Remove S3 bucket notification configuration
    try:
        print("   Clearing S3 bucket notification configuration...")
        s3_client.put_bucket_notification_configuration(
            Bucket=S3_BUCKET_NAME,
            NotificationConfiguration={},
        )
        print("   S3 notification configuration cleared")
    except ClientError as e:
        print(f"   Warning: Could not clear S3 notifications (may not exist): {e}")

    # 2) Remove Lambda permission that allows S3 to invoke it
    try:
        print("   Removing Lambda permission 'S3InvokePermission'...")
        lambda_client.remove_permission(
            FunctionName=LAMBDA_FUNCTION_NAME,
            StatementId="S3InvokePermission",
        )
        print("   Lambda invoke permission removed")
    except lambda_client.exceptions.ResourceNotFoundException:
        print("   Warning: Lambda permission not found (already removed)")
    except ClientError as e:
        print(f"   Warning: Could not remove Lambda permission: {e}")

# Delete the Lambda function that was used to start the Glue job when CSV files arrived.
# It first checks for existence via the AWS API and reports a friendly message if missing.
def delete_lambda_function():
    """Delete the Lambda function used to trigger the Glue job."""
    print("\n" + "=" * 80)
    print("STEP 2: DELETING LAMBDA FUNCTION")
    print("=" * 80)

    try:
        lambda_client.delete_function(FunctionName=LAMBDA_FUNCTION_NAME)
        print(f"  Lambda function '{LAMBDA_FUNCTION_NAME}' deleted")
    except lambda_client.exceptions.ResourceNotFoundException:
        print(f"  Lambda function '{LAMBDA_FUNCTION_NAME}' does not exist")
    except ClientError as e:
        print(f"   Error deleting Lambda function: {e}")
        raise

# Delete the Glue ETL job that performed the CSV → DynamoDB transformation.
# If the job is already gone, it logs a warning instead of failing.
def delete_glue_job():
    """Delete the Glue ETL job."""
    print("\n" + "=" * 80)
    print("STEP 3: DELETING GLUE JOB")
    print("=" * 80)

    try:
        glue_client.delete_job(JobName=GLUE_JOB_NAME)
        print(f"   Glue job '{GLUE_JOB_NAME}' deleted")
    except glue_client.exceptions.EntityNotFoundException:
        print(f"   Warning: Glue job '{GLUE_JOB_NAME}' does not exist")
    except ClientError as e:
        print(f"   Error deleting Glue job: {e}")
        raise

# Internal helper that fully cleans up an IAM role:
# - Detaches all managed policies
# - Deletes all inline policies
# - Deletes the role itself if it still exists.
def _delete_iam_role(role_name: str):
    """Helper to detach policies and delete an IAM role if it exists."""
    try:
        iam_client.get_role(RoleName=role_name)
    except iam_client.exceptions.NoSuchEntityException:
        print(f"   Warning: IAM role '{role_name}' does not exist")
        return

    print(f"   Processing IAM role '{role_name}'")

    # Detach managed policies
    attached = iam_client.list_attached_role_policies(RoleName=role_name)
    for p in attached.get("AttachedPolicies", []):
        arn = p["PolicyArn"]
        print(f"     - Detaching managed policy: {arn}")
        iam_client.detach_role_policy(RoleName=role_name, PolicyArn=arn)

    # Delete inline policies
    inline = iam_client.list_role_policies(RoleName=role_name)
    for policy_name in inline.get("PolicyNames", []):
        print(f"     - Deleting inline policy: {policy_name}")
        iam_client.delete_role_policy(RoleName=role_name, PolicyName=policy_name)

    # Finally delete the role
    iam_client.delete_role(RoleName=role_name)
    print(f"   Role '{role_name}' deleted")

# Delete both IAM roles created by the service script (Lambda role and Glue role).
# Uses the helper above to safely detach policies before removing the roles.
def delete_iam_roles():
    """Delete the IAM roles created for Lambda and Glue."""
    print("\n" + "=" * 80)
    print("STEP 4: DELETING IAM ROLES")
    print("=" * 80)

    try:
        _delete_iam_role(LAMBDA_ROLE_NAME)
        _delete_iam_role(GLUE_ROLE_NAME)
    except ClientError as e:
        print(f"   Error deleting IAM roles: {e}")
        raise

# Delete the DynamoDB table that stored the transformed company records.
# Waits until AWS confirms the table no longer exists.
def delete_dynamodb_table():
    """Delete the DynamoDB table used by the pipeline (if it exists)."""
    print("\n" + "=" * 80)
    print("STEP 5: DELETING DYNAMODB TABLE")
    print("=" * 80)

    try:
        dynamodb_client.delete_table(TableName=DYNAMODB_TABLE_NAME)
        print(f"   Deleting table '{DYNAMODB_TABLE_NAME}'...")
        waiter = dynamodb_client.get_waiter("table_not_exists")
        waiter.wait(TableName=DYNAMODB_TABLE_NAME)
        print(f"   Table '{DYNAMODB_TABLE_NAME}' deleted")
    except dynamodb_client.exceptions.ResourceNotFoundException:
        print(f"   Warning: Table '{DYNAMODB_TABLE_NAME}' does not exist")
    except ClientError as e:
        print(f"   Error deleting DynamoDB table: {e}")
        raise

# Empty the S3 bucket (delete all objects) and then remove the bucket itself.
# Uses pagination and batched deletes so it can handle many objects safely.
def empty_and_delete_bucket():
    """Delete all objects in the S3 bucket and then delete the bucket itself."""
    print("\n" + "=" * 80)
    print("STEP 6: DELETING S3 BUCKET AND CONTENTS")
    print("=" * 80)

    # First, try to delete all objects (if bucket exists)
    try:
        print(f"   Listing objects in bucket '{S3_BUCKET_NAME}'...")
        paginator = s3_client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=S3_BUCKET_NAME)

        to_delete = []
        for page in pages:
            for obj in page.get("Contents", []):
                to_delete.append({"Key": obj["Key"]})

                # Batch delete every 1000 objects
                if len(to_delete) >= 1000:
                    s3_client.delete_objects(
                        Bucket=S3_BUCKET_NAME,
                        Delete={"Objects": to_delete},
                    )
                    to_delete = []

        # Delete any remaining objects
        if to_delete:
            s3_client.delete_objects(
                Bucket=S3_BUCKET_NAME,
                Delete={"Objects": to_delete},
            )

        print("   All objects deleted (if any existed)")
    except ClientError as e:
        # If the bucket doesn't exist, nothing else to do here
        if e.response.get("Error", {}).get("Code") in ("NoSuchBucket", "404"):
            print(f"   Warning: Bucket '{S3_BUCKET_NAME}' does not exist")
            return
        print(f"   Error deleting objects from bucket: {e}")
        raise

    # Now delete the bucket itself
    try:
        s3_client.delete_bucket(Bucket=S3_BUCKET_NAME)
        print(f"   Bucket '{S3_BUCKET_NAME}' deleted")
    except ClientError as e:
        print(f"   Error deleting bucket: {e}")
        raise

# Orchestrator for the teardown flow:
# - Shows the user a summary of what will be deleted
# - Asks for confirmation
# - Runs each teardown step in a safe order.
def main():
    print("=" * 80)
    print("AWS PIPELINE TEARDOWN")
    print("=" * 80)
    print("This will attempt to delete the following resources:")
    print(json.dumps(
        {
            "region": AWS_REGION,
            "s3_bucket": S3_BUCKET_NAME,
            "glue_job": GLUE_JOB_NAME,
            "lambda_function": LAMBDA_FUNCTION_NAME,
            "dynamodb_table": DYNAMODB_TABLE_NAME,
            "lambda_role": LAMBDA_ROLE_NAME,
            "glue_role": GLUE_ROLE_NAME,
        },
        indent=2,
    ))
    confirm = input("\nType 'delete' to confirm teardown: ").strip().lower()
    if confirm != "delete":
        print("Teardown cancelled.")
        return

    delete_s3_trigger_and_lambda_permission()
    delete_lambda_function()
    delete_glue_job()
    delete_iam_roles()
    delete_dynamodb_table()
    empty_and_delete_bucket()

    print("\nTeardown completed.")


if __name__ == "__main__":
    main()