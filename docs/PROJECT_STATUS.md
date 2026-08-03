# Fleet Telematics Pipeline
## Project Status & Developer Guide

---

# Project Overview

This project implements an end-to-end IoT Data Engineering pipeline for vehicle telemetry data using Apache Kafka, AWS, Databricks and PySpark.

The objective is to simulate a real-world streaming pipeline that ingests telemetry data, stores it in Amazon S3, processes it using Databricks, loads the processed data into DynamoDB and Amazon RDS, and finally presents insights through a dashboard.

---

# Technology Stack

- Python 3.11.15
- Apache Kafka
- Docker
- Amazon S3
- AWS Lambda
- AWS Step Functions
- Databricks
- PySpark
- Amazon DynamoDB
- Amazon RDS (MySQL)
- Git & GitHub

---

# Python Version (IMPORTANT)

Use:

```text
Python 3.11.15
```

Do NOT use Python 3.14.

Verify using

```bash
python --version
```

Expected Output

```text
Python 3.11.15
```

---

# Project Structure

```
fleet-telematics-pipeline/

├── aws/
│   └── s3/
│       └── create_bucket.py
│
├── configuration/
│   └── settings.py
│
├── datasets/
│
├── databricks/
│
├── docker/
│   └── docker-compose.yml
│
├── docs/
│
├── lambda/
│   ├── lambda_function.py
│   └── lambda.zip
│
├── scripts/
│   └── tests/
│       └── test_lambda_trigger.py
│
├── sql/
│
├── streaming/
│   ├── kafka-producer.py
│   └── kafka-consumer.py
│
├── README.md
├── .env
└── .gitignore
```

---

# Architecture

```
CSV Dataset
      │
      ▼
Kafka Producer
      │
      ▼
Apache Kafka
      │
      ▼
Kafka Consumer
      │
      ▼
Amazon S3 (raw JSONL)
      │
      ▼
S3 Event Notification
      │
      ▼
AWS Lambda
      │
      ▼
AWS Step Functions
      │
      ▼
Databricks (PySpark ETL)
      │
      ▼
Processed Dataset (S3)
      │
      ├──────────────► DynamoDB
      │
      └──────────────► Amazon RDS
                              │
                              ▼
                         Dashboard
```

---

# Dataset

Dataset

```
v2.csv
```

Approximate Size

```
700 MB
```

Approximate Records

```
3.1 Million
```

Columns

- tripID
- deviceID
- timeStamp
- accData
- gps_speed
- battery
- cTemp
- dtc
- eLoad
- iat
- imap
- kpl
- maf
- rpm
- speed
- tAdv
- tPos

---

# Current Progress

## Completed

- AWS Account upgraded to Pay-As-You-Go
- Databricks Workspace created
- Project IAM User created
- AWS CLI configured
- Docker configured
- Kafka running in Docker (KRaft Mode)
- Kafka Topic created

```
vehicle-telemetry
```

- Dataset downloaded
- Dataset inspected
- Kafka Producer completed
- Kafka Consumer completed
- Amazon S3 bucket created

```
capstone-fleet-telematics
```

- Bucket folders created

```
raw/
processed/
curated/
reports/
```

- Lambda Function created
- Lambda deployed
- S3 Event Notification configured
- Lambda successfully triggered from S3 upload
- Lambda trigger test script created

---

# Current Pipeline

```
CSV

↓

Kafka Producer

↓

Kafka Topic

↓

Kafka Consumer

↓

Single JSONL File

↓

Amazon S3 (raw/)

↓

S3 Event Notification

↓

Lambda
```

---

# Immediate Next Steps

## Phase 1

Implement AWS Step Functions

Lambda

↓

Start Step Functions execution

Pass

- Bucket Name
- Object Key

---

## Phase 2

Create Databricks Notebook

Notebook should

- Read JSONL from S3
- Clean data
- Handle missing values
- Parse timestamp
- Feature engineering
- Write processed dataset

```
processed/
```

---

## Phase 3

Integrate Databricks with Step Functions

End Result

```
Upload File

↓

Lambda

↓

Step Functions

↓

Databricks Job
```

---

## Phase 4

Load processed data into

- Amazon DynamoDB
- Amazon RDS (MySQL)

---

## Phase 5

Dashboard

Visualize

- Vehicle Speed
- Fuel Efficiency
- Battery
- RPM
- Engine Temperature
- Trips
- Vehicle Health

---

# Coding Standards

- Store secrets inside `.env`
- Configuration belongs inside `configuration/settings.py`
- One responsibility per module
- Use meaningful commit messages
- Test every component independently before integration

---

# Utility Scripts

Current utility scripts

```
scripts/tests/test_lambda_trigger.py
```

Purpose

- Uploads a sample JSONL file to `raw/`
- Verifies S3 Event Notification
- Verifies Lambda Trigger
- Useful after any AWS configuration changes

---

# Git Workflow

```
git pull

git add .

git commit -m "<message>"

git push
```

---

# Current Status

| Component | Status |
|-----------|--------|
| Docker | 1 |
| Kafka | 1 |
| Producer | 1 |
| Consumer | 1 |
| S3 Bucket | 1 |
| Lambda | 1 |
| S3 Event Notification | 1 |
| Step Functions | 0 |
| Databricks | 0 |
| DynamoDB | 0 |
| Amazon RDS | 0 |
| Dashboard | 0 |

---

# Maintainers

Ashutosh Anand

Mukund