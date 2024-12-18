# S3 to SQS to MySQL Processor

## Overview
This project consists of two AWS Lambda functions that process CSV files uploaded to S3. Data from the CSV is first sent to an Amazon SQS FIFO queue and then processed into a MySQL database.

---

## Architecture

1. **S3 Event**:
   - Triggers the `processS3Upload` Lambda function on file upload.
   
2. **SQS FIFO Queue**:
   - Receives chunks of CSV data from `processS3Upload`.

3. **Database Insertion**:
   - `processCSV` Lambda function processes the queue messages and inserts data into the MySQL database.

---

## Prerequisites

1. **AWS Resources**:
   - S3 bucket (with event triggers).
   - SQS FIFO queue.
   - MySQL database.

2. **Environment Variables**:
   - `AWS_REGION`: AWS region (e.g., `us-east-1`).
   - `SQS_QUEUE_URL`: SQS FIFO queue URL.

3. **SSM Parameters**:
   - Store database credentials as SSM parameters:
     - `/dev/MYSQL_HOST`
     - `/dev/MYSQL_USER`
     - `/dev/MYSQL_PASSWORD`
     - `/dev/MYSQL_DATABASE`

---

## Deployment

1. Install dependencies:
   ```bash
   npm install @aws-sdk/client-s3 @aws-sdk/client-sqs @aws-sdk/client-ssm csv-parser mysql2
