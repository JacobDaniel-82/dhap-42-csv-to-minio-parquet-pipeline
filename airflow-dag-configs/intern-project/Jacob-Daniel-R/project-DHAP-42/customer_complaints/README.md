# DHAP-42 — Local CSV to Parquet Ingestion Pipeline with MinIO

## Overview

DHAP-42 implements a local data ingestion pipeline that reads customer complaint records from a CSV file, validates the input against a strict schema contract, transforms the data, and stores partitioned Parquet files in MinIO object storage.

The pipeline is orchestrated using Apache Airflow and runs locally through Docker Compose.

## Architecture

```text
Local CSV
   |
   v
Schema Validation
   |
   v
Data Transformation
   |
   v
Parquet Conversion
   |
   v
MinIO Object Storage
   |
   v
Product-Based Partitions
```

## Technology Stack

- Python
- Apache Airflow
- Pandas
- PyArrow
- Boto3
- MinIO
- Docker Compose
- YAML

## Project Structure

```text
customer_complaints/
├── dags/
│   ├── extraction/
│   │   └── sample.csv
│   └── csv_to_parquet_minio.py
├── .env.example
├── docker-compose.yaml
├── manifest.yaml
├── schema_contract.yaml
└── README.md
```

## Dataset

**Source:** `dags/extraction/sample.csv`

The dataset contains 1,750 customer complaint records with the following columns:

| Column | Data Type | Constraints |
|---|---|---|
| Consumer_complaint | String | Not nullable |
| Product | String | Not nullable |
| Sentiment | Integer | Not nullable; values 0 or 1 |
| Priority | Integer | Not nullable; values 0 or 1 |

## Schema Contract and Validation

The expected schema is defined in `schema_contract.yaml`.

Before loading data, the pipeline validates:

- Exact column names and column order.
- Required data types.
- Nullability constraints.
- Allowed values for `Sentiment` and `Priority`.

If the input violates the schema contract, the `validate_csv` task raises an error and the DAG fails before the loading task executes.

## Data Transformation and Storage

After successful validation, the pipeline trims surrounding whitespace from string columns and converts the dataset to Parquet format using PyArrow.

The output is stored in the MinIO bucket:

`customer-complaints`

Data is partitioned by the `Product` column using Hive-style partition paths.

Example object paths:

```text
customer-complaints/
└── customer_complaints/
    ├── Product=Debt collection/
    │   └── part-00000.parquet
    ├── Product=Mortgage/
    │   └── part-00000.parquet
    └── Product=Credit card or prepaid card/
        └── part-00000.parquet
```

Each product partition contains its corresponding Parquet data.

## Local Setup

### Prerequisites

- Docker Desktop
- Git
- Access to the project files

### 1. Configure Environment Variables

Create a local `.env` file from the example:

```bash
cp .env.example .env
```

Update `.env` with your desired local MinIO and Airflow credentials.

**Security:** Do not commit `.env` or expose real credentials in the repository.

### 2. Start the Services

From the project directory, run:

```bash
docker compose up -d
```

Docker Compose starts the Airflow and MinIO services with separate persistent volumes for this project.

### 3. Access Airflow

Open:

`http://localhost:8080`

Log in using the Airflow credentials configured in `.env`.

Locate the DAG:

`csv_to_parquet_minio`

Trigger the DAG and verify that both `validate_csv` and `transform_and_load` complete successfully.

### 4. Inspect MinIO

Open the MinIO Console:

`http://localhost:9001`

Log in using the MinIO credentials configured in `.env`.

Navigate to the `customer-complaints` bucket and inspect the generated product partitions and Parquet files.

### 5. Stop the Services

```bash
docker compose down
```

This stops and removes the project containers and network while preserving named volumes. Do not use `docker compose down -v` unless you intentionally want to remove the project's persistent volumes.

## Testing

### Successful Ingestion

The pipeline was executed against the original CSV dataset.

**Expected result:**
- Schema validation succeeds.
- The transformation and loading task succeeds.
- Partitioned Parquet files appear in MinIO.

### Invalid Schema Test

The CSV header was temporarily modified by renaming `Sentiment` to `Sentiment_Test`.

**Expected result:**
- The `validate_csv` task fails with a schema mismatch error.
- The loading task does not execute.
- Existing MinIO output remains untouched by the failed validation run.

The original CSV was restored and the pipeline was executed again successfully.

## Configuration Reference

| File | Purpose |
|---|---|
| `manifest.yaml` | Defines the source, destination, format, partition column, and schema contract reference |
| `schema_contract.yaml` | Defines the required columns, types, nullability, and allowed values |
| `dags/csv_to_parquet_minio.py` | Implements validation, transformation, and MinIO loading |
| `docker-compose.yaml` | Configures the local Airflow and MinIO services |
| `.env.example` | Provides example environment variables |

## Outcome

DHAP-42 demonstrates a reproducible local CSV-to-Parquet ingestion workflow with Airflow orchestration, strict schema validation, product-based partitioning, and MinIO object storage.
