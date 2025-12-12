## Big Data Final Project (ALY6110) — S3 → Lambda → Glue → DynamoDB

This project provisions and runs an AWS data pipeline that:

- **Ingests** a CSV file uploaded to **Amazon S3**
- **Triggers** an **AWS Lambda** function on `ObjectCreated` events
- **Starts** an **AWS Glue ETL** job
- **Transforms** and **loads** the data into a **DynamoDB** table

### Architecture diagram

![Pipeline Architecture](workflow/Workflow.png)

---

## Project outcome (results)

- **Load size achieved**: **5.6 million rows** successfully written into **Amazon DynamoDB**.
- **End-to-end flow**: Upload CSV → S3 event → Lambda → Glue ETL → DynamoDB items.

> Note: Final load time and throughput depend on AWS region, Glue worker count, DynamoDB account limits, and dataset size.

---

## Repository contents

- **`createService.py`**: One-click infrastructure setup. Creates:
  - S3 bucket
  - IAM roles (Lambda role + Glue role)
  - Glue job (ETL)
  - Lambda function (S3 trigger → Glue start)
  - S3 event notification (prefix `raw/` and suffix `.csv`)
  - Uploads the Glue ETL script to S3 under `scripts/…`
- **`uploadDatasetToS3.py`**: Uploads the local CSV to S3 under `raw/…` to trigger the pipeline.
- **`deleteService.py`**: Teardown script. Removes S3 notifications and permissions, then deletes:
  - Lambda function
  - Glue job
  - IAM roles
  - (Optionally) DynamoDB table
  - S3 bucket (and all objects in it)
- **`dataset/`**: Local CSV input (ignored by git).
- **`extra/`**: Misc/backup files (ignored by git except `extra/dataset link.txt`).

---

## How the pipeline works

### Phase 1: Infrastructure setup (`createService.py`)

1. **Create S3 bucket**
2. **Upload Glue ETL script** to `s3://<bucket>/scripts/...`
3. **Create IAM roles**
   - Lambda role: permissions for logs + starting Glue job + reading S3 objects
   - Glue role: permissions to read from S3 and write to DynamoDB (and Glue service role access)
4. **Create Glue job** pointing at the uploaded ETL script
5. **Create Lambda function** with environment variables:
   - `GLUE_JOB_NAME`
   - `DYNAMODB_TABLE_NAME`
6. **Configure S3 event trigger**
   - Event: `s3:ObjectCreated:*`
   - Filter: prefix `raw/`, suffix `.csv`

### Phase 2: Runtime execution (after a CSV upload)

1. Upload a CSV to **`s3://<bucket>/raw/<file>.csv`**
2. S3 emits an event → **Lambda** is invoked
3. Lambda calls **Glue `StartJobRun`** and passes:
   - `--S3_BUCKET`
   - `--S3_KEY`
   - `--DYNAMODB_TABLE`
4. Glue ETL job:
   - Creates the DynamoDB table if it does not exist (partition key: `id`)
   - Reads the CSV from S3
   - Adds metadata columns (timestamp, source file)
   - Generates `id` if missing, drops duplicates, then writes to DynamoDB

---

## Dataset

- **Input format**: CSV with header row.
- **Upload location**: `s3://<bucket>/raw/`
- **Trigger rule**: only objects that match:
  - **Prefix**: `raw/`
  - **Suffix**: `.csv`

---

## DynamoDB data model

The Glue job writes items to a single DynamoDB table:

- **Table name**: configured in `createService.py` (`DYNAMODB_TABLE_NAME`)
- **Partition key**: `id` (string)
- **Billing mode**: `PAY_PER_REQUEST` (on-demand)
- **Additional attributes**:
  - `ingestion_timestamp`: load time metadata
  - `source_file`: S3 key that produced the record
  - Other CSV columns: written as DynamoDB string attributes (via Glue mapping)

If the source CSV does not contain an `id` column, the ETL generates a deterministic ID by hashing the row contents.

---

## Implementation details (what each script does)

### `createService.py`

- **S3 bucket**:
  - Creates the bucket (or reuses it if it already exists)
  - Uploads the Glue ETL script to `scripts/…`
- **IAM roles**:
  - **Lambda role**: CloudWatch Logs + `glue:StartJobRun` + `s3:GetObject`
  - **Glue role**: read S3 + write DynamoDB + `AWSGlueServiceRole` managed policy
- **Glue job**:
  - Glue version configured in the script (example: Glue 4.0)
  - Worker configuration configured in the script (worker type + count + timeout)
- **Lambda function**:
  - Triggered by S3 `ObjectCreated` events filtered to `raw/*.csv`
  - Starts the Glue job and passes S3 bucket/key + DynamoDB table name

### `uploadDatasetToS3.py`

- Uploads the local CSV file to the configured S3 bucket/key under `raw/…`
- This upload is what triggers the pipeline

### `deleteService.py`

- Removes S3 notification configuration and the Lambda invoke permission
- Deletes Lambda, Glue job, IAM roles, DynamoDB table (optional), and S3 bucket contents + bucket
- Requires typing `delete` to confirm

---

## Prerequisites

- **Python 3.x**
- **AWS credentials configured** (AWS CLI, environment variables, or any standard `boto3` credential method)
- IAM permissions to create/manage:
  - S3 bucket + bucket notifications
  - Lambda function + permissions
  - Glue job
  - IAM roles + policies
  - DynamoDB table (if enabled/used)

---

## Configuration (update before running)

These values are hard-coded in the scripts and must match across files:

- **`AWS_REGION`**
- **`S3_BUCKET_NAME`**
- **`GLUE_JOB_NAME`**
- **`LAMBDA_FUNCTION_NAME`**
- **`DYNAMODB_TABLE_NAME`**
- **`CSV_UPLOAD_PREFIX`** (expected to be `raw/`)

If you change resource names in `createService.py`, update the same names in `uploadDatasetToS3.py` and `deleteService.py`.

---

## Quick start (reproduce the pipeline)

### 1) Create the infrastructure

Run:

```bash
python createService.py
```

### 2) Upload the dataset to trigger the pipeline

Run:

```bash
python uploadDatasetToS3.py
```

### 3) Monitor

- **Lambda logs**: CloudWatch → Log groups → `/aws/lambda/<lambda-name>`
- **Glue job**: AWS Glue Console → Jobs → `<job-name>`
- **DynamoDB table**: DynamoDB Console → Tables → `<table-name>`

---

## Troubleshooting

- **Lambda triggered but Glue didn’t start**
  - Check Lambda logs for missing environment variables (`GLUE_JOB_NAME`, `DYNAMODB_TABLE_NAME`)
  - Confirm the Lambda IAM role allows `glue:StartJobRun`
- **Upload doesn’t trigger Lambda**
  - Confirm the object key is under `raw/` and ends with `.csv`
  - Confirm the bucket notification configuration includes the Lambda ARN and filter rules
- **Glue job fails reading CSV**
  - Confirm the CSV has a header row
  - Confirm Glue role has `s3:GetObject` permissions for the bucket
- **DynamoDB throttling / slow writes**
  - With on-demand tables, spikes may still be throttled depending on account limits
  - Reduce Glue parallelism or adjust DynamoDB write options in the ETL mapping/writer

---

## Cost notes (AWS)

Running this pipeline may incur charges for:

- AWS Glue (workers and duration)
- Lambda invocations (typically small)
- DynamoDB writes/storage (potentially significant at millions of items)
- S3 storage and requests

---

## Teardown (delete all resources)

Run:

```bash
python deleteService.py
```

You will be prompted to type **`delete`** to confirm.

> Warning: This permanently deletes the S3 bucket (including all objects) and other pipeline resources.
