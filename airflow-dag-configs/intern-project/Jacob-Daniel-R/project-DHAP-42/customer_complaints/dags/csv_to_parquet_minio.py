
import os
from pathlib import Path

import boto3
import pandas as pd
import yaml
from botocore.exceptions import ClientError
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


BASE_DIR = Path("/opt/airflow")
CSV_PATH = BASE_DIR / "dags" / "extraction" / "sample.csv"
MANIFEST_PATH = BASE_DIR / "manifest.yaml"
CONTRACT_PATH = BASE_DIR / "schema_contract.yaml"


def validate_csv():
    with open(MANIFEST_PATH, "r", encoding="utf-8") as file:
        manifest = yaml.safe_load(file)

    with open(CONTRACT_PATH, "r", encoding="utf-8") as file:
        contract = yaml.safe_load(file)

    df = pd.read_csv(CSV_PATH)

    expected_columns = [column["name"] for column in contract["columns"]]
    actual_columns = list(df.columns)

    if actual_columns != expected_columns:
        raise ValueError(
            f"Schema mismatch. Expected columns {expected_columns}; "
            f"received {actual_columns}"
        )

    for column in contract["columns"]:
        name = column["name"]
        expected_type = column["type"]

        if not column.get("nullable", True) and df[name].isna().any():
            raise ValueError(f"Column '{name}' contains null values")

        if expected_type == "string":
            if not df[name].map(lambda value: isinstance(value, str)).all():
                raise ValueError(f"Column '{name}' must contain strings")

        elif expected_type == "integer":
            numeric = pd.to_numeric(df[name], errors="coerce")

            if numeric.isna().any():
                raise ValueError(f"Column '{name}' contains non-integer values")

            if ((numeric % 1) != 0).any():
                raise ValueError(f"Column '{name}' contains non-integer values")

            df[name] = numeric.astype("int64")

        if "allowed_values" in column:
            invalid = ~df[name].isin(column["allowed_values"])
            if invalid.any():
                raise ValueError(
                    f"Column '{name}' contains disallowed values: "
                    f"{df.loc[invalid, name].unique().tolist()}"
                )

    return df


def transform_and_load():
    df = validate_csv()

    # Keep the contract-defined columns and remove surrounding whitespace.
    for column in df.select_dtypes(include=["object"]).columns:
        df[column] = df[column].str.strip()

    endpoint = os.environ["MINIO_ENDPOINT"]
    access_key = os.environ["MINIO_ROOT_USER"]
    secret_key = os.environ["MINIO_ROOT_PASSWORD"]
    bucket = os.environ["MINIO_BUCKET"]

    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",
    )

    try:
        client.head_bucket(Bucket=bucket)
    except ClientError as error:
        code = error.response.get("Error", {}).get("Code", "")
        if code not in ("404", "NoSuchBucket", "NotFound"):
            raise
        client.create_bucket(Bucket=bucket)

    partition_column = "Product"
    for product, partition_df in df.groupby(partition_column, dropna=False):
        partition_value = str(product).strip() or "unknown"
        safe_value = (
            partition_value.replace("/", "_")
            .replace("\\", "_")
            .replace("=", "_")
        )

        object_key = (
            f"customer_complaints/{partition_column}={safe_value}/part-00000.parquet"
        )

        output = partition_df.to_parquet(index=False, engine="pyarrow")

        client.put_object(
            Bucket=bucket,
            Key=object_key,
            Body=output,
            ContentType="application/octet-stream",
        )

    print(f"Successfully loaded {len(df)} records into s3://{bucket}/")


with DAG(
    dag_id="csv_to_parquet_minio",
    description="Validate a local CSV and load partitioned Parquet files into MinIO",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["DHAP-42", "minio", "parquet"],
) as dag:
    validate_task = PythonOperator(
        task_id="validate_csv",
        python_callable=validate_csv,
    )

    load_task = PythonOperator(
        task_id="transform_and_load",
        python_callable=transform_and_load,
    )

    validate_task >> load_task
