## Big Data Final Project (ALY6110) — S3 → Lambda → Glue → DynamoDB

This project provisions and runs an AWS data pipeline that:

- **Ingests** a CSV file uploaded to **Amazon S3**
- **Triggers** an **AWS Lambda** function on `ObjectCreated` events
- **Starts** an **AWS Glue ETL** job
- **Transforms** and **loads** the data into a **DynamoDB** table

### Architecture diagram

Architecture image (hosted on Google Drive):

![Pipeline Architecture](https://drive.google.com/uc?export=view&id=1uUIoyfXjhdF4S8pjK2ZusG_zAW2pVPDr)

If the image does not render (Drive permissions), place it at `docs/pipeline-architecture.png` and use:

![Pipeline Architecture (local)](docs/pipeline-architecture.png)

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

## Quick start

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

## Teardown (delete all resources)

Run:

```bash
python deleteService.py
```

You will be prompted to type **`delete`** to confirm.

> Warning: This permanently deletes the S3 bucket (including all objects) and other pipeline resources.
