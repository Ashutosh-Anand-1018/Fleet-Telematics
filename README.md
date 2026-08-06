<div align="center">

# 🚛 Fleet Telematics — IoT Sensor Data Analytics Pipeline

### A cloud-native, end-to-end streaming analytics platform for fleet operations

*Collaborative capstone project · Kafka → S3 → Databricks → RDS/DynamoDB → Live Dashboard*

[![AWS](https://img.shields.io/badge/AWS-232F3E?style=for-the-badge&logo=amazonwebservices&logoColor=white)](https://aws.amazon.com/)
[![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)](https://databricks.com/)
[![Apache Kafka](https://img.shields.io/badge/Kafka-231F20?style=for-the-badge&logo=apachekafka&logoColor=white)](https://kafka.apache.org/)
[![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-009688?style=for-the-badge&logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com/)
[![MySQL](https://img.shields.io/badge/MySQL-4479A1?style=for-the-badge&logo=mysql&logoColor=white)](https://www.mysql.com/)

</div>

---

## 📋 Overview

Two engineers, one shared platform, two independently owned business use cases — mirroring how real data engineering teams split ownership while integrating on common infrastructure.

Vehicle telemetry (speed, RPM, engine temp, fuel efficiency, and more) streams through Kafka, lands in an S3 data lake, gets cleaned and validated with PySpark on Databricks, and feeds two parallel analytics pipelines: **driver behavior scoring** and **vehicle health monitoring**. Results are queryable from both a relational store (RDS) and a fast key-value store (DynamoDB), and visualized in a live operations dashboard.

| | |
|---|---|
| **Team size** | 2 engineers |
| **Domain** | IoT · Cloud · Big Data |
| **Dataset** | [Levin Vehicle Telematics](https://www.kaggle.com/datasets/yunlevin/levin-vehicle-telematics) — 3.1M+ rows |
| **Region** | ap-south-1 (Mumbai) |

---

## 🏗️ Architecture

```
┌─────────────┐     ┌──────────┐     ┌─────────────┐     ┌───────────────────┐
│   Kafka      │ ──▶ │  S3 raw/  │ ──▶ │  Lambda      │ ──▶ │  Step Functions     │
│  (producer/   │     │           │     │  (validate)   │     │  (orchestration)     │
│   consumer)   │     └──────────┘     └─────────────┘     └──────────┬──────────┘
└─────────────┘                                                        │
                                                                         ▼
                                                          ┌──────────────────────────┐
                                                          │   Databricks — PySpark     │
                                                          │   Shared cleaning job       │
                                                          │   (dedup, type-cast, filter) │
                                                          └──────────────┬───────────┘
                                                                         │
                                                              S3 processed/
                                                                         │
                              ┌──────────────────────────────┬──────────┘
                              ▼                                ▼
                ┌───────────────────────┐          ┌───────────────────────┐
                │  UC-1: Vehicle Health    │          │  UC-2: Driver Behavior   │
                │  & Predictive Maintenance │          │  & Fuel Efficiency        │
                └───────────┬───────────┘          └───────────┬───────────┘
                            │                                    │
                    S3 curated/uc1/                      S3 curated/uc2/
                            │                                    │
                            ▼                                    ▼
                ┌────────────────────────────────────────────────────────┐
                │       RDS (MySQL)              DynamoDB                  │
                │  VehicleHealthReport         VehicleAlerts                │
                │  DriverPerformance            DriverScore                  │
                └────────────────────────┬─────────────────────────────────┘
                                          │
                                          ▼
                              ┌───────────────────────┐
                              │   FastAPI Dashboard      │
                              │   (live ops console)      │
                              └───────────────────────┘
```

---

## 👥 Ownership

Per the project's individual-accountability model — shared platform built together, one use case owned end-to-end by each engineer, cross-reviewed before integration.

| Component | Owner | Reviewer |
|---|---|---|
| Shared platform (Kafka, S3, Lambda, Step Functions, Databricks cleaning, common RDS/DynamoDB tables) | Both | Both |
| **UC-1 — Vehicle Health & Predictive Maintenance** | Engineer 1 | Engineer 2 |
| **UC-2 — Driver Behavior & Fuel Efficiency** | Engineer 2 | Engineer 1 |
| Integration & final testing | Both | Both |

---

## 🔍 UC-2: Driver Behavior & Fuel Efficiency

Detects five driving behavior signals from real-time telemetry, using **evidence-based thresholds** derived from profiling the actual 3.1M-row dataset rather than guessed values.

| Detection | Rule | Basis |
|---|---|---|
| High RPM | `rpm > 2000` | ~p95 of RPM distribution |
| Rapid acceleration | `Δspeed > 5 km/h` in 1 second | ~p99 of positive speed deltas |
| Hard braking | `Δspeed < -6 km/h` in 1 second | ~p01 of speed deltas |
| Poor fuel efficiency | `kpl < 1.2` while moving | ~p25 of efficiency, excluding idle |
| **Aggressive driving** | 2+ signals firing on the same reading | Composite — a single event isn't "aggressive," a pattern is |

**Scoring:** Driver Safety Score uses **relative percentile ranking** within the fleet rather than a fixed absolute penalty — at ~1-second sampling density, even genuinely risky drivers' events are a tiny fraction of total readings, so percentile rank guarantees meaningful spread across the fleet regardless of absolute event rarity.

---

## 🩺 UC-1: Vehicle Health & Predictive Maintenance

Flags potential mechanical issues before they become breakdowns.

| Detection | Rule | Basis |
|---|---|---|
| High RPM | `rpm > 2000` | ~p95 |
| Engine overheating | `cTemp > 91°C`, excluding cold-start zeros | ~p90-p95 |
| Excessive engine load | `eLoad > 81`, excluding idle zeros | ~p95 |
| Low battery voltage | `battery < 12.0V`, excluding sensor-inactive zeros | Standard 12V automotive convention + bottom ~0.3% of real readings |
| Frequent DTC events | `dtc > 0` | ⚠️ Fires on only 71 of 3.1M rows (0.002%) in this dataset — logic is correct, signal is genuinely sparse |

---

## 📊 Dashboard Screenshots

<!--
  Drop dashboard screenshots into this folder: dashboard/screenshots/
  Then reference them below, e.g.:
  ![Driver Behavior Console](dashboard/screenshots/driver-console.png)
-->

<div align="center">

### Driver Behavior Console — RDS View

<!-- ![Driver Behavior — RDS](dashboard/screenshots/driver-rds.png) -->
*Screenshot goes here — `dashboard/screenshots/driver-rds.png`*

### Driver Behavior Console — DynamoDB Comparison View

<!-- ![Driver Behavior — DynamoDB](dashboard/screenshots/driver-dynamo.png) -->
*Screenshot goes here — `dashboard/screenshots/driver-dynamo.png`*

</div>

---

## 🛠️ Tech Stack

<table>
<tr><td><b>Ingestion</b></td><td>Apache Kafka (KRaft mode, Docker)</td></tr>
<tr><td><b>Storage</b></td><td>Amazon S3 (raw / processed / curated / reports)</td></tr>
<tr><td><b>Orchestration</b></td><td>AWS Step Functions, AWS Lambda</td></tr>
<tr><td><b>Processing</b></td><td>Databricks, PySpark 3.5.x</td></tr>
<tr><td><b>Databases</b></td><td>Amazon RDS (MySQL), Amazon DynamoDB</td></tr>
<tr><td><b>Dashboard</b></td><td>FastAPI, Jinja2, Chart.js</td></tr>
<tr><td><b>Automation</b></td><td>Boto3, Python 3.11</td></tr>
<tr><td><b>Infra Access</b></td><td>IAM (least-privilege, per-engineer scoped users)</td></tr>
</table>

---

## 🚀 Getting Started

```bash
# 1. Clone and set up environment
git clone https://github.com/Ashutosh-Anand-1018/Fleet-Telematics.git
cd Fleet-Telematics
python3.11 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# 2. Configure environment variables
cp .env.example .env   # fill in AWS, Kafka, RDS, Databricks credentials

# 3. Bring up local Kafka
cd docker && docker compose up -d && cd ..

# 4. Run the dashboard
pip install fastapi uvicorn jinja2
uvicorn dashboard.app:app --reload --port 8000
# → open http://localhost:8000
```

Databricks notebooks (`databricks/`) run on a Databricks cluster with S3 cross-account access configured via Databricks Secrets — see inline documentation in `configure_s3_credentials.py`.

---

## 📁 Repository Structure

```
Fleet-Telematics/
├── common/                    # Shared boto3 helpers, Kafka config
├── databricks/                 # PySpark notebooks (cleaning + both UCs)
├── dashboard/                   # FastAPI live ops console
│   ├── app.py
│   ├── templates/
│   └── screenshots/              ← dashboard screenshots go here
├── sql/                          # RDS table DDL
├── scripts/                       # Local utility/test scripts
├── streaming/                      # Kafka producer/consumer
├── lambda/                          # Lambda functions
├── docs/                             # Architecture diagrams, build notes
└── docker/                            # Kafka docker-compose setup
```

---

## 📝 Notable Engineering Decisions & Findings

Documented honestly, including things that didn't go as expected — treated as evidence of process, not hidden.

- **Embedded header rows**: the source CSV contains 33 duplicated header rows scattered through the file (signature of concatenated source files) — required explicit filtering beyond standard null-handling.
- **Exact duplicates**: ~27% of rows in early testing were byte-identical duplicates (verified, not distinct sub-second readings) — safe to drop via full-row dedup.
- **Cross-account Databricks access**: compute (Databricks workspace) and storage (S3/RDS) live in different AWS accounts — resolved via explicit IAM credentials through Databricks Secrets rather than instance profiles.
- **DTC sparsity**: "frequent DTC events" detection is logically correct but the real dataset exercises it on only 0.002% of rows — a genuine data characteristic, not a bug.
- **RDS capacity**: hit an `insufficient-capacity` error on `db.t4g.micro`; resolved by switching to `db.t3.micro` with an explicit Availability Zone.

---

<div align="center">

*Built as part of the BridgeLabz AWS Data Engineering Fellowship*

</div>