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
print("\n[STEP 0] Checking/Creating DynamoDB Table...")

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
print("\n[STEP 1] Reading data from S3...")

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
                "escaper": "\\"
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
    print("\n[STEP 2] Transforming data...")
    
    df = dynamic_frame.toDF()
    
    print("\nSource Schema:")
    df.printSchema()
    
    # Repartition for distributed processing
    partition_count = 50
    df = df.repartition(partition_count)
    print(f"Repartitioned data into {partition_count} partitions")
    
    # Add metadata columns
    df = df.withColumn("ingestion_timestamp", current_timestamp())
    df = df.withColumn("source_file", lit(S3_KEY))
    
    # Generate unique ID if not present
    # Use only "simple" column names (no dots) to avoid nested-field resolution issues.
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
    print("\n[STEP 3] Preparing data for DynamoDB...")
    
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
    print("\n[STEP 4] Writing to DynamoDB...")
    
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
    print("\n[STEP 5] Job Summary:")
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
print("\nJob completed successfully!")