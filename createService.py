"""
AWS Infrastructure Setup Script
================================
This script creates the complete pipeline:
S3 Bucket → Lambda → Glue ETL → DynamoDB

Author: Dhruvkumar, Deepkumar, Poe
Course: ALY6110 - Data Management and Big Data
"""

import boto3
import json
import time
import zipfile
import io
import os
from botocore.exceptions import ClientError
from datetime import datetime

# ============================================
# CONFIGURATION
# ============================================
AWS_REGION = "us-east-2"
S3_BUCKET_NAME = "aws-aly6110-final-assignment"
GLUE_JOB_NAME = "DataPushToDynamoDB"
LAMBDA_FUNCTION_NAME = "S3ToGlueTrigger"
DYNAMODB_TABLE_NAME = "company"

# Glue Job Configuration
GLUE_VERSION = "4.0"
WORKER_TYPE = "G.1X"
NUMBER_OF_WORKERS = 20
JOB_TIMEOUT = 60  # minutes

# S3 Paths
GLUE_SCRIPT_S3_KEY = "scripts/DataPushToDynamoDB.py"
CSV_UPLOAD_PREFIX = "raw/"

# IAM Role Names
LAMBDA_ROLE_NAME = "LambdaGlueTriggerRole"
GLUE_ROLE_NAME = "GlueETLServiceRole"

# Initialize AWS Clients
print("=" * 80)
print("AWS INFRASTRUCTURE SETUP SCRIPT")
print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f" Region: {AWS_REGION}")
print("=" * 80)

s3_client = boto3.client('s3', region_name=AWS_REGION)
iam_client = boto3.client('iam', region_name=AWS_REGION)
lambda_client = boto3.client('lambda', region_name=AWS_REGION)
glue_client = boto3.client('glue', region_name=AWS_REGION)
sts_client = boto3.client('sts', region_name=AWS_REGION)

# Get AWS Account ID
ACCOUNT_ID = sts_client.get_caller_identity()['Account']
print(f"AWS Account ID: {ACCOUNT_ID}")

# ============================================
# GLUE ETL SCRIPT (will be uploaded to S3)
# ============================================
GLUE_ETL_SCRIPT = '''
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, current_timestamp, md5, concat_ws, lit
from pyspark.sql.types import StringType
import boto3

# ============================================
# INITIALIZATION
# ============================================

# Get job parameters
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'S3_BUCKET',
    'S3_KEY',
    'DYNAMODB_TABLE'
])

# Initialize DynamoDB client
dynamodb = boto3.client('dynamodb')

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Extract parameters
S3_BUCKET = args['S3_BUCKET']
S3_KEY = args['S3_KEY']
DYNAMODB_TABLE = args['DYNAMODB_TABLE']
S3_PATH = f"s3://{S3_BUCKET}/{S3_KEY}"

print("=" * 80)
print(f"Starting ETL Job: {args['JOB_NAME']}")
print(f"Source: {S3_PATH}")
print(f"Destination: {DYNAMODB_TABLE}")
print("=" * 80)

# ============================================
# CREATE DYNAMODB TABLE IF NOT EXISTS
# ============================================
print("[STEP 0] Checking/Creating DynamoDB Table...")

def create_dynamodb_table_if_not_exists(table_name):
    """Create DynamoDB table if it doesn't exist"""
    try:
        dynamodb.describe_table(TableName=table_name)
        print(f"Table '{table_name}' already exists")
        return True
    except dynamodb.exceptions.ResourceNotFoundException:
        print(f"Table '{table_name}' not found. Creating...")
        
        dynamodb.create_table(
            TableName=table_name,
            KeySchema=[
                {'AttributeName': 'id', 'KeyType': 'HASH'}
            ],
            AttributeDefinitions=[
                {'AttributeName': 'id', 'AttributeType': 'S'}
            ],
            BillingMode='PAY_PER_REQUEST'
        )
        
        print(f"Table '{table_name}' creation initiated")
        print("Waiting for table to become active...")
        
        waiter = dynamodb.get_waiter('table_exists')
        waiter.wait(TableName=table_name)
        
        print(f"Table '{table_name}' is now active")
        return True
    except Exception as e:
        print(f"Error with table: {str(e)}")
        raise

create_dynamodb_table_if_not_exists(DYNAMODB_TABLE)

# ============================================
# STEP 1: READ DATA FROM S3
# ============================================
print("[STEP 1] Reading data from S3...")

file_extension = S3_KEY.split('.')[-1].lower()

try:
    if file_extension == 'csv':
        dynamic_frame = glueContext.create_dynamic_frame.from_options(
            connection_type="s3",
            connection_options={"paths": [S3_PATH], "recurse": False},
            format="csv",
            format_options={
                "withHeader": True,
                "separator": ",",
                "quoteChar": '"',
                "escaper": "\"
            }
        )
    else:
        raise ValueError(f"Unsupported file format: {file_extension}")
    
    initial_count = dynamic_frame.count()
    print(f"Successfully read {initial_count} records from S3")
    
    # FIX: Handle empty file without sys.exit()
    if initial_count == 0:
        print("Warning: No records found in source file. Completing job.")
        job.commit()
        # Don't use sys.exit() - just let the script end naturally

except Exception as e:
    print(f"Error reading from S3: {str(e)}")
    raise

# Only proceed if we have records
if initial_count > 0:
    
    # ============================================
    # STEP 2: DATA TRANSFORMATION
    # ============================================
    print("[STEP 2] Transforming data...")
    
    df = dynamic_frame.toDF()
    
    print("Source Schema:")
    df.printSchema()
    
    # Repartition for distributed processing
    partition_count = 150
    df = df.repartition(partition_count)
    print(f"Repartitioned data into {partition_count} partitions")
    
    # Add metadata columns
    df = df.withColumn("ingestion_timestamp", current_timestamp())
    df = df.withColumn("source_file", lit(S3_KEY))
    
    # Generate unique ID if not present
    if "id" not in df.columns:
        hash_cols = [df[c] for c in df.columns if "." not in c]
        df = df.withColumn(
            "id",
            concat_ws(
                "-",
                lit("record"),
                md5(concat_ws("_", *hash_cols)).cast(StringType()),
            ),
        )
    
    print(f"Added metadata columns: ingestion_timestamp, source_file, id")
    
    # Filter null IDs
    required_columns = ["id"]
    for column in required_columns:
        if column in df.columns:
            before_count = df.count()
            df = df.filter(col(column).isNotNull())
            after_count = df.count()
            filtered = before_count - after_count
            if filtered > 0:
                print(f"Filtered {filtered} records with null values in '{column}'")
    
    # Remove duplicates
    before_dedup = df.count()
    df = df.dropDuplicates(["id"])
    after_dedup = df.count()
    duplicates_removed = before_dedup - after_dedup
    if duplicates_removed > 0:
        print(f"Removed {duplicates_removed} duplicate records")
    
    print(f"Transformation complete: {df.count()} records ready for loading")
    
    # ============================================
    # STEP 3: PREPARE FOR DYNAMODB
    # ============================================
    print("[STEP 3] Preparing data for DynamoDB...")
    
    dynamic_frame_transformed = DynamicFrame.fromDF(df, glueContext, "transformed_data")
    
    # Build mappings dynamically
    # Use simple Glue types; treat all non-metadata fields as strings when writing to DynamoDB
    mappings = [
        ("id", "string", "id", "string"),
        ("ingestion_timestamp", "timestamp", "timestamp", "string"),
        ("source_file", "string", "source_file", "string"),
    ]
    
    for field in df.schema.fields:
        column_name = field.name
        if column_name not in ["id", "ingestion_timestamp", "source_file"]:
            mappings.append((column_name, "string", column_name, "string"))
    
    print(f"Configured {len(mappings)} column mappings")
    
    mapped_frame = ApplyMapping.apply(
        frame=dynamic_frame_transformed,
        mappings=mappings
    )
    
    final_count = mapped_frame.count()
    print(f"Data mapped and ready: {final_count} records")
    
    # ============================================
    # STEP 4: WRITE TO DYNAMODB
    # ============================================
    print("[STEP 4] Writing to DynamoDB...")
    
    try:
        glueContext.write_dynamic_frame.from_options(
            frame=mapped_frame,
            connection_type="dynamodb",
            connection_options={
                "dynamodb.output.tableName": DYNAMODB_TABLE,
                "dynamodb.throughput.write.percent": "0.5",
                "dynamodb.output.retry": "10",
                "dynamodb.output.numParallelTasks": "20",
                "dynamodb.output.batchSize": "25"
            }
        )
        
        print(f"Successfully wrote {final_count} records to DynamoDB")
        
    except Exception as e:
        print(f"Error writing to DynamoDB: {str(e)}")
        raise
    
    # ============================================
    # STEP 5: JOB SUMMARY
    # ============================================
    print("[STEP 5] Job Summary:")
    print("=" * 80)
    print(f"Source File: {S3_PATH}")
    print(f"Records Read: {initial_count}")
    print(f"Records Written: {final_count}")
    print(f"Duplicates Removed: {duplicates_removed}")
    print(f"Target Table: {DYNAMODB_TABLE}")
    print(f"Status: SUCCESS")
    print("=" * 80)

# Commit the job (always at the end)
job.commit()
print("Job completed successfully!")
'''

# ============================================
# LAMBDA FUNCTION CODE
# ============================================
LAMBDA_CODE = '''
import os
import json
import boto3
import logging
import urllib.parse

GLUE_JOB_NAME = os.getenv("GLUE_JOB_NAME")
DYNAMODB_TABLE = os.getenv("DYNAMODB_TABLE_NAME")

logger = logging.getLogger()
logger.setLevel(logging.INFO)

glue_client = boto3.client("glue")

def lambda_handler(event, context):
    logger.info("Lambda triggered - S3 event received")
    logger.info(f"Event: {json.dumps(event)}")

    if not GLUE_JOB_NAME:
        logger.error("Missing GLUE_JOB_NAME environment variable")
        return {"statusCode": 500, "body": "Missing GLUE_JOB_NAME"}

    if not DYNAMODB_TABLE:
        logger.error("Missing DYNAMODB_TABLE_NAME environment variable")
        return {"statusCode": 500, "body": "Missing DYNAMODB_TABLE_NAME"}

    if "Records" not in event:
        logger.error("No Records found in event")
        return {"statusCode": 400, "body": "Invalid event format"}

    for record in event["Records"]:
        bucket = record["s3"]["bucket"]["name"]
        key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])

        logger.info(f"File detected: s3://{bucket}/{key}")

        if not key.lower().endswith(".csv"):
            logger.info(f"Skipping non-CSV file: {key}")
            continue

        try:
            response = glue_client.start_job_run(
                JobName=GLUE_JOB_NAME,
                Arguments={
                    "--S3_BUCKET": bucket,
                    "--S3_KEY": key,
                    "--DYNAMODB_TABLE": DYNAMODB_TABLE
                }
            )

            job_run_id = response["JobRunId"]
            logger.info(f"Glue job started - Job Run ID: {job_run_id}")

        except glue_client.exceptions.ConcurrentRunsExceededException:
            logger.warning("Glue job already running")
            return {"statusCode": 429, "body": "Job already running"}

        except Exception as e:
            logger.error(f"Failed to start Glue job: {str(e)}")
            return {"statusCode": 500, "body": str(e)}

    return {
        "statusCode": 200,
        "body": json.dumps({"message": "Glue job triggered", "job_run_id": job_run_id})
    }
'''


# ============================================
# STEP 1: CREATE S3 BUCKET
# ============================================
def create_s3_bucket():
    print("\n" + "=" * 80)
    print("STEP 1: CREATING S3 BUCKET")
    print("=" * 80)

    try:
        # Check if bucket exists
        try:
            s3_client.head_bucket(Bucket=S3_BUCKET_NAME)
            print(f"   Bucket '{S3_BUCKET_NAME}' already exists")
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                print(f"   Bucket '{S3_BUCKET_NAME}' not found, creating...")
            else:
                raise

        # Create bucket (us-east-1 doesn't need LocationConstraint)
        if AWS_REGION == 'us-east-1':
            s3_client.create_bucket(Bucket=S3_BUCKET_NAME)
        else:
            s3_client.create_bucket(
                Bucket=S3_BUCKET_NAME,
                CreateBucketConfiguration={'LocationConstraint': AWS_REGION}
            )

        print(f"Bucket '{S3_BUCKET_NAME}' created successfully")

        # Wait for bucket to be available
        print("Waiting for bucket to be available...")
        waiter = s3_client.get_waiter('bucket_exists')
        waiter.wait(Bucket=S3_BUCKET_NAME)
        print("Bucket is now available")

        return True

    except ClientError as e:
        print(f"   Error creating bucket: {e}")
        raise


# ============================================
# STEP 2: UPLOAD FILES TO S3
# ============================================
def upload_glue_script():
    print("\n" + "=" * 80)
    print("STEP 2: UPLOADING GLUE ETL SCRIPT TO S3")
    print("=" * 80)

    try:
        print(f"Uploading Glue script to s3://{S3_BUCKET_NAME}/{GLUE_SCRIPT_S3_KEY}")

        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=GLUE_SCRIPT_S3_KEY,
            Body=GLUE_ETL_SCRIPT.encode('utf-8'),
            ContentType='text/x-python'
        )

        print("   Glue script uploaded successfully")
        print(f"   Location: s3://{S3_BUCKET_NAME}/{GLUE_SCRIPT_S3_KEY}")

        return True

    except ClientError as e:
        print(f"   Error uploading script: {e}")
        raise


# ============================================
# STEP 3: CREATE IAM ROLES
# ============================================
def create_lambda_role():
    print("\n" + "=" * 80)
    print("STEP 3A: CREATING LAMBDA IAM ROLE")
    print("=" * 80)

    # Trust policy for Lambda
    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"Service": "lambda.amazonaws.com"},
            "Action": "sts:AssumeRole"
        }]
    }

    # Permissions policy
    permissions_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents"
                ],
                "Resource": "arn:aws:logs:*:*:*"
            },
            {
                "Effect": "Allow",
                "Action": ["glue:StartJobRun"],
                "Resource": f"arn:aws:glue:{AWS_REGION}:{ACCOUNT_ID}:job/{GLUE_JOB_NAME}"
            },
            {
                "Effect": "Allow",
                "Action": ["s3:GetObject"],
                "Resource": f"arn:aws:s3:::{S3_BUCKET_NAME}/*"
            }
        ]
    }

    try:
        # Check if role exists
        try:
            response = iam_client.get_role(RoleName=LAMBDA_ROLE_NAME)
            print(f"   Role '{LAMBDA_ROLE_NAME}' already exists")
            return response['Role']['Arn']
        except iam_client.exceptions.NoSuchEntityException:
            print(f"   Creating role '{LAMBDA_ROLE_NAME}'...")

        # Create role
        response = iam_client.create_role(
            RoleName=LAMBDA_ROLE_NAME,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Description="Role for Lambda function to trigger Glue jobs"
        )
        role_arn = response['Role']['Arn']
        print(f"   Role created: {role_arn}")

        # Attach inline policy
        print("   Attaching permissions policy...")
        iam_client.put_role_policy(
            RoleName=LAMBDA_ROLE_NAME,
            PolicyName="LambdaGluePermissions",
            PolicyDocument=json.dumps(permissions_policy)
        )
        print("   Permissions policy attached")

        # Wait for role to propagate
        print("   Waiting for role to propagate (10 seconds)...")
        time.sleep(10)

        return role_arn

    except ClientError as e:
        print(f"   Error creating Lambda role: {e}")
        raise


def create_glue_role():
    print("\n" + "=" * 80)
    print("STEP 3B: CREATING GLUE IAM ROLE")
    print("=" * 80)

    # Trust policy for Glue
    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"Service": "glue.amazonaws.com"},
            "Action": "sts:AssumeRole"
        }]
    }

    # Permissions policy
    permissions_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:ListBucket"
                ],
                "Resource": [
                    f"arn:aws:s3:::{S3_BUCKET_NAME}",
                    f"arn:aws:s3:::{S3_BUCKET_NAME}/*"
                ]
            },
            {
                "Effect": "Allow",
                "Action": [
                    "dynamodb:*"
                ],
                "Resource": f"arn:aws:dynamodb:{AWS_REGION}:{ACCOUNT_ID}:table/{DYNAMODB_TABLE_NAME}"
            },
            {
                "Effect": "Allow",
                "Action": [
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents"
                ],
                "Resource": "arn:aws:logs:*:*:*"
            },
            {
                "Effect": "Allow",
                "Action": [
                    "glue:*"
                ],
                "Resource": "*"
            }
        ]
    }

    try:
        # Check if role exists
        try:
            response = iam_client.get_role(RoleName=GLUE_ROLE_NAME)
            print(f"   Role '{GLUE_ROLE_NAME}' already exists")
            return response['Role']['Arn']
        except iam_client.exceptions.NoSuchEntityException:
            print(f"   Creating role '{GLUE_ROLE_NAME}'...")

        # Create role
        response = iam_client.create_role(
            RoleName=GLUE_ROLE_NAME,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Description="Role for Glue ETL job to access S3 and DynamoDB"
        )
        role_arn = response['Role']['Arn']
        print(f"   Role created: {role_arn}")

        # Attach inline policy
        print("   Attaching permissions policy...")
        iam_client.put_role_policy(
            RoleName=GLUE_ROLE_NAME,
            PolicyName="GlueETLPermissions",
            PolicyDocument=json.dumps(permissions_policy)
        )
        print("   Permissions policy attached")

        # Attach AWS managed policy for Glue
        print("   Attaching AWSGlueServiceRole managed policy...")
        iam_client.attach_role_policy(
            RoleName=GLUE_ROLE_NAME,
            PolicyArn="arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
        )
        print("   Managed policy attached")

        # Wait for role to propagate
        print("   Waiting for role to propagate (10 seconds)...")
        time.sleep(10)

        return role_arn

    except ClientError as e:
        print(f"   Error creating Glue role: {e}")
        raise


# ============================================
# STEP 4: CREATE GLUE JOB
# ============================================
def create_glue_job(glue_role_arn):
    print("\n" + "=" * 80)
    print("STEP 4: CREATING GLUE ETL JOB")
    print("=" * 80)

    try:
        # Check if job exists
        try:
            glue_client.get_job(JobName=GLUE_JOB_NAME)
            print(f"   Glue job '{GLUE_JOB_NAME}' already exists")
            return True
        except glue_client.exceptions.EntityNotFoundException:
            print(f"   Creating Glue job '{GLUE_JOB_NAME}'...")

        # Create Glue job
        glue_client.create_job(
            Name=GLUE_JOB_NAME,
            Description="ETL job to load CSV data from S3 into DynamoDB",
            Role=glue_role_arn,
            Command={
                'Name': 'glueetl',
                'ScriptLocation': f's3://{S3_BUCKET_NAME}/{GLUE_SCRIPT_S3_KEY}',
                'PythonVersion': '3'
            },
            DefaultArguments={
                '--job-language': 'python',
                '--enable-metrics': 'true',
                '--enable-spark-ui': 'true',
                '--enable-job-insights': 'true',
                '--enable-continuous-cloudwatch-log': 'true',
                '--TempDir': f's3://{S3_BUCKET_NAME}/temp/'
            },
            GlueVersion=GLUE_VERSION,
            WorkerType=WORKER_TYPE,
            NumberOfWorkers=NUMBER_OF_WORKERS,
            Timeout=JOB_TIMEOUT,
            MaxRetries=0
        )

        print(f"   Glue job '{GLUE_JOB_NAME}' created successfully")
        print("   Configuration:")
        print(f"      - Glue Version: {GLUE_VERSION}")
        print(f"      - Worker Type: {WORKER_TYPE}")
        print(f"      - Number of Workers: {NUMBER_OF_WORKERS}")
        print(f"      - Timeout: {JOB_TIMEOUT} minutes")
        print(f"      - Script: s3://{S3_BUCKET_NAME}/{GLUE_SCRIPT_S3_KEY}")

        return True

    except ClientError as e:
        print(f"   Error creating Glue job: {e}")
        raise

# ============================================
# STEP 5: CREATE LAMBDA FUNCTION
# ============================================
def create_lambda_function(lambda_role_arn):
    print("\n" + "=" * 80)
    print("STEP 5: CREATING LAMBDA FUNCTION")
    print("=" * 80)

    try:
        # Check if function exists
        try:
            lambda_client.get_function(FunctionName=LAMBDA_FUNCTION_NAME)
            print(f"   Lambda function '{LAMBDA_FUNCTION_NAME}' already exists")
            return True
        except lambda_client.exceptions.ResourceNotFoundException:
            print(f"   Creating Lambda function '{LAMBDA_FUNCTION_NAME}'...")

        # Create ZIP file with Lambda code
        print("   Packaging Lambda code...")
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
            zf.writestr('lambda_function.py', LAMBDA_CODE)
        zip_buffer.seek(0)

        # Create Lambda function
        lambda_client.create_function(
            FunctionName=LAMBDA_FUNCTION_NAME,
            Runtime='python3.11',
            Role=lambda_role_arn,
            Handler='lambda_function.lambda_handler',
            Code={'ZipFile': zip_buffer.read()},
            Description='Triggers Glue ETL job when CSV is uploaded to S3',
            Timeout=30,
            MemorySize=128,
            Environment={
                'Variables': {
                    'GLUE_JOB_NAME': GLUE_JOB_NAME,
                    'DYNAMODB_TABLE_NAME': DYNAMODB_TABLE_NAME
                }
            }
        )

        print(f"   Lambda function '{LAMBDA_FUNCTION_NAME}' created successfully")
        print("   Configuration:")
        print("      - Runtime: Python 3.11")
        print("      - Memory: 128 MB")
        print("      - Timeout: 30 seconds")
        print(f"      - GLUE_JOB_NAME: {GLUE_JOB_NAME}")
        print(f"      - DYNAMODB_TABLE_NAME: {DYNAMODB_TABLE_NAME}")

        return True

    except ClientError as e:
        print(f"   Error creating Lambda function: {e}")
        raise


# ============================================
# STEP 6: CONFIGURE S3 EVENT NOTIFICATION
# ============================================
def configure_s3_trigger():
    print("\n" + "=" * 80)
    print("STEP 6: CONFIGURING S3 EVENT TRIGGER")
    print("=" * 80)

    lambda_arn = f"arn:aws:lambda:{AWS_REGION}:{ACCOUNT_ID}:function:{LAMBDA_FUNCTION_NAME}"

    try:
        # Add permission for S3 to invoke Lambda
        print("   Adding S3 invoke permission to Lambda...")
        try:
            lambda_client.add_permission(
                FunctionName=LAMBDA_FUNCTION_NAME,
                StatementId='S3InvokePermission',
                Action='lambda:InvokeFunction',
                Principal='s3.amazonaws.com',
                SourceArn=f'arn:aws:s3:::{S3_BUCKET_NAME}',
                SourceAccount=ACCOUNT_ID
            )
            print("   S3 invoke permission added")
        except lambda_client.exceptions.ResourceConflictException:
            print("   S3 invoke permission already exists")

        # Configure S3 bucket notification
        print("   Configuring S3 bucket notification...")
        notification_config = {
            'LambdaFunctionConfigurations': [
                {
                    'Id': 'CSVUploadTrigger',
                    'LambdaFunctionArn': lambda_arn,
                    'Events': ['s3:ObjectCreated:*'],
                    'Filter': {
                        'Key': {
                            'FilterRules': [
                                {'Name': 'prefix', 'Value': CSV_UPLOAD_PREFIX},
                                {'Name': 'suffix', 'Value': '.csv'}
                            ]
                        }
                    }
                }
            ]
        }

        s3_client.put_bucket_notification_configuration(
            Bucket=S3_BUCKET_NAME,
            NotificationConfiguration=notification_config
        )

        print("   S3 event notification configured successfully")
        print("   Trigger Configuration:")
        print("      - Event: s3:ObjectCreated:*")
        print(f"      - Prefix: {CSV_UPLOAD_PREFIX}")
        print("      - Suffix: .csv")
        print(f"      - Target: {LAMBDA_FUNCTION_NAME}")

        return True

    except ClientError as e:
        print(f"   Error configuring S3 trigger: {e}")
        raise


# ============================================
# SUMMARY
# ============================================
def print_summary():
    print("\n" + "=" * 80)
    print("INFRASTRUCTURE SETUP COMPLETE!")
    print("=" * 80)
    print(f"""
S3 Bucket:        {S3_BUCKET_NAME}
   - Scripts:        s3://{S3_BUCKET_NAME}/scripts/
   - CSV Upload:     s3://{S3_BUCKET_NAME}/raw/

Lambda Function:   {LAMBDA_FUNCTION_NAME}
   - Trigger:        S3 ObjectCreated (*.csv in raw/)

Glue Job:         {GLUE_JOB_NAME}
   - Workers:        {NUMBER_OF_WORKERS} x {WORKER_TYPE}
   - Timeout:        {JOB_TIMEOUT} minutes

DynamoDB Table:   {DYNAMODB_TABLE_NAME}
   - Created by:     Glue job (on first run)

IAM Roles:
   - Lambda Role:    {LAMBDA_ROLE_NAME}
   - Glue Role:      {GLUE_ROLE_NAME}
""")
    print("=" * 80)
    print("NEXT STEPS:")
    print("=" * 80)
    print(f"""
1. Upload a CSV file to trigger the pipeline:
   aws s3 cp your-file.csv s3://{S3_BUCKET_NAME}/raw/

2. Monitor the pipeline:
   - Lambda logs: CloudWatch > Log groups > /aws/lambda/{LAMBDA_FUNCTION_NAME}
   - Glue job: AWS Glue Console > Jobs > {GLUE_JOB_NAME}
   - DynamoDB: DynamoDB Console > Tables > {DYNAMODB_TABLE_NAME}

3. To manually test:
   aws glue start-job-run --job-name {GLUE_JOB_NAME} \\
       --arguments '--S3_BUCKET={S3_BUCKET_NAME},--S3_KEY=raw/your-file.csv,--DYNAMODB_TABLE={DYNAMODB_TABLE_NAME}'
""")
    print("=" * 80)


# ============================================
# INTERACTIVE STEP CONTROL
# ============================================
def prompt_continue(step_label: str) -> bool:
    """
    Ask the user if they want to continue to the given step.
    Returns True if the user answers yes, False if no.
    """
    while True:
        answer = input(f"\nDo you want to continue to {step_label}? (y/n): ").strip().lower()
        if answer in ("y", "yes"):
            return True
        if answer in ("n", "no"):
            print("Stopping setup as requested.")
            return False
        print("Please enter 'y' or 'n'.")


# ============================================
# MAIN EXECUTION
# ============================================
def main():
    try:
        # Step 1: Create S3 Bucket
        create_s3_bucket()

        # Ask before proceeding to each subsequent step
        if not prompt_continue("Step 2 - upload the Glue script to S3"):
            return

        # Step 2: Upload Glue Script
        upload_glue_script()

        if not prompt_continue("Step 3 - create IAM roles for Lambda and Glue"):
            return

        # Step 3: Create IAM Roles
        lambda_role_arn = create_lambda_role()
        glue_role_arn = create_glue_role()

        if not prompt_continue("Step 4 - create the Glue job"):
            return

        # Step 4: Create Glue Job
        create_glue_job(glue_role_arn)

        if not prompt_continue("Step 5 - create the Lambda function"):
            return

        # Step 5: Create Lambda Function
        create_lambda_function(lambda_role_arn)

        if not prompt_continue("Step 6 - configure the S3 trigger for Lambda"):
            return

        # Step 6: Configure S3 Trigger
        configure_s3_trigger()

        # Print Summary
        print_summary()

        print(f"\nSetup completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    except Exception as e:
        print(f"\nSetup failed with error: {e}")
        raise


if __name__ == "__main__":
    main()