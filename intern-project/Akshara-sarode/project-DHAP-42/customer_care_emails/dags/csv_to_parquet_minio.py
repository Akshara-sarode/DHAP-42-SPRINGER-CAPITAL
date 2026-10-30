"""Strictly validate the DHAP-34 customer-care CSV and write partitioned Parquet to MinIO."""
from __future__ import annotations

import csv
import logging
import os
import re
import tempfile
from datetime import date, datetime, timezone
from pathlib import Path

import boto3
import pandas as pd
import yaml
from airflow import DAG
from airflow.operators.python import PythonOperator
from botocore.config import Config

log = logging.getLogger(__name__)
DAG_ID = "customer_care_emails_csv_to_minio_parquet"
DAG_DIR = Path(__file__).parent
CSV_PATH = DAG_DIR / "extraction" / "sample.csv"
CONTRACT_PATH = DAG_DIR.parent / "schema_contract.yaml"
DATASET = "customer_care_emails"


def _contract():
    with CONTRACT_PATH.open(encoding="utf-8") as handle:
        contract = yaml.safe_load(handle)
    return contract, contract["columns"]


def validate_contract(**context):
    """Fail before transformation or upload if the exact CSV contract is violated."""
    contract, columns = _contract()
    expected = [column["name"] for column in columns]
    if not CSV_PATH.is_file():
        raise FileNotFoundError(f"Expected input CSV at {CSV_PATH}")

    with CSV_PATH.open(encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle, restkey="__extra_fields__", restval=None)
        actual = reader.fieldnames
        if actual is None:
            raise ValueError("CSV is empty or has no header")
        if len(set(actual)) != len(actual):
            raise ValueError(f"CSV contains duplicate column names: {actual}")
        if actual != expected:
            missing = [name for name in expected if name not in actual]
            extra = [name for name in actual if name not in expected]
            raise ValueError(
                f"CSV header does not match schema contract version {contract['version']}. "
                f"Expected {expected}; got {actual}; missing={missing}; extra={extra}"
            )

        rows = []
        for line_number, row in enumerate(reader, start=2):
            if row.pop("__extra_fields__", None) is not None:
                raise ValueError(f"Row {line_number} has more fields than the contract")
            errors = []
            for column in columns:
                name = column["name"]
                value = row.get(name)
                if value is None or not value.strip():
                    if not column.get("nullable", True):
                        errors.append(f"{name} must not be null or empty")
                    continue
                value = value.strip()
                kind = column["type"]
                try:
                    if kind == "timestamp_utc":
                        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
                        if parsed.tzinfo is None or parsed.utcoffset() is None:
                            raise ValueError("timestamp must include a UTC offset")
                        row[name] = parsed.astimezone(timezone.utc)
                    elif kind == "float64":
                        parsed_number = float(value)
                        if parsed_number != parsed_number or parsed_number in (float("inf"), float("-inf")):
                            raise ValueError("must be a finite number")
                        if "minimum" in column and parsed_number < column["minimum"]:
                            raise ValueError(f"must be >= {column['minimum']}")
                        if "maximum" in column and parsed_number > column["maximum"]:
                            raise ValueError(f"must be <= {column['maximum']}")
                        row[name] = parsed_number
                    elif kind == "string":
                        row[name] = value
                    if "allowed_values" in column and value not in column["allowed_values"]:
                        raise ValueError(f"must be one of {column['allowed_values']}")
                except (ValueError, TypeError) as exc:
                    errors.append(f"{name}: {exc}")
            if errors:
                raise ValueError(f"Invalid CSV row {line_number}: {'; '.join(errors)}")
            rows.append(row)

    if not rows:
        raise ValueError("CSV has a valid header but contains no data rows")
    log.info("Contract v%s passed: %d rows, %d columns", contract["version"], len(rows), len(expected))
    return {"row_count": len(rows), "columns": expected}


def transform_to_partitioned_parquet(**context):
    """Re-read the contract-validated CSV and prepare a local date-partitioned Parquet artifact."""
    # Revalidate at the transformation boundary to guard against changes between DAG tasks.
    validate_contract(**context)
    frame = pd.read_csv(CSV_PATH, dtype=str, keep_default_na=False, encoding="utf-8-sig")
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True, errors="raise")
    frame["customer_satisfaction"] = pd.to_numeric(frame["customer_satisfaction"], errors="raise").astype("float64")
    frame["timestamp_date"] = frame["timestamp"].dt.strftime("%Y-%m-%d")

    ti = context["ti"]
    run_token = re.sub(r"[^A-Za-z0-9_-]", "_", context["run_id"])[-80:]
    path = tempfile.mkdtemp(prefix="dhap42-")
    try:
        frame.to_parquet(path, engine="pyarrow", index=False, partition_cols=["timestamp_date"])
        output = {"path": path, "row_count": len(frame), "run_token": run_token}
        ti.xcom_push(key="parquet_artifact", value=output)
        log.info("Prepared %d rows across %d UTC date partitions", len(frame), frame["timestamp_date"].nunique())
        return output
    except Exception:
        import shutil
        shutil.rmtree(path, ignore_errors=True)
        raise


def upload_parquet_to_minio(**context):
    """Upload Hive-style partition objects. No destination object is created before validation."""
    artifact = context["ti"].xcom_pull(task_ids="transform_to_partitioned_parquet", key="parquet_artifact")
    root = Path(artifact["path"])
    endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    bucket = os.environ.get("MINIO_BUCKET", "customer-care-emails")
    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin123"),
        region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        config=Config(s3={"addressing_style": "path"}),
    )
    uploaded = []
    try:
        for parquet_file in sorted(root.rglob("*.parquet")):
            relative = parquet_file.relative_to(root)
            partition = relative.parts[0]
            if not partition.startswith("timestamp_date="):
                raise ValueError(f"Unexpected Parquet partition path: {relative}")
            day = partition.split("=", 1)[1]
            date.fromisoformat(day)
            key = f"{DATASET}/{partition}/part-{artifact['run_token']}.parquet"
            client.upload_file(str(parquet_file), bucket, key)
            uploaded.append(f"s3://{bucket}/{key}")
    finally:
        import shutil
        shutil.rmtree(root, ignore_errors=True)
    if not uploaded:
        raise ValueError("Parquet transformation produced no files to upload")
    log.info("Uploaded %d Parquet partitions: %s", len(uploaded), uploaded)
    return {"objects": uploaded, "row_count": artifact["row_count"]}


with DAG(
    dag_id=DAG_ID,
    description="Validate the DHAP-34 customer care email CSV and load date-partitioned Parquet into MinIO.",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    tags=["DHAP-42", "csv", "parquet", "minio"],
    max_active_runs=1,
) as dag:
    validate = PythonOperator(task_id="validate_contract", python_callable=validate_contract)
    transform = PythonOperator(task_id="transform_to_partitioned_parquet", python_callable=transform_to_partitioned_parquet)
    upload = PythonOperator(task_id="upload_parquet_to_minio", python_callable=upload_parquet_to_minio)
    validate >> transform >> upload
