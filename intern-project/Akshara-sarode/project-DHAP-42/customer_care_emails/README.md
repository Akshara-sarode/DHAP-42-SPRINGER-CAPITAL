# DHAP-42: Customer care CSV to MinIO Parquet

An end-to-end, locally reproducible Apache Airflow pipeline for the DHAP-34 `customer_care_emails` dataset. Airflow validates every CSV header, required value, timestamp, numeric value and allowed-value rule against `schema_contract.yaml`, then writes date-partitioned Parquet to a local MinIO bucket.

## Project layout

```text
customer_care_emails/
├── dags/extraction/sample.csv       # same five-row DHAP-34 dataset
├── dags/csv_to_parquet_minio.py     # validation, transformation and S3 upload tasks
├── manifest.yaml
├── schema_contract.yaml
├── Dockerfile
├── docker-compose.yaml
├── .env.example                     # placeholder and local development values only
└── requirements.txt
```

The repository instructions say to commit this folder under `intern-project/<your-name>/project-DHAP-42/<dataset_name>/`. The prepared location is `intern-project/Akshara-sarode/project-DHAP-42/customer_care_emails/`.

## Prerequisites

- Docker Desktop with Docker Compose v2, with at least 4 GB of memory available.
- Ports 8080, 9000 and 9001 free.

No host Python installation or AWS account is required. The Airflow image installs pinned Python dependencies at build time. Docker downloads the Airflow constraints file during that build.

## Start the stack

From this folder, copy `.env.example` to `.env`. For a persistent local setup, replace `AIRFLOW_FERNET_KEY` and `AIRFLOW_WEBSERVER_SECRET_KEY` with generated random values. For example:

```bash
cp .env.example .env
openssl rand -base64 32 | tr '+/' '-_' | tr -d '\n'
openssl rand -hex 32
```

Put the first generated value in `AIRFLOW_FERNET_KEY`, the second in `AIRFLOW_WEBSERVER_SECRET_KEY`, then start the services:

```bash
docker compose up --build
```

Docker Compose initializes Airflow's metadata database and admin user, waits for MinIO, and creates the `customer-care-emails` bucket. Open:

- Airflow: <http://localhost:8080> (defaults: `airflow` / `airflow`)
- MinIO console: <http://localhost:9001> (defaults: `minioadmin` / `minioadmin123`)
- MinIO API: <http://localhost:9000>

All credentials above are local-only example defaults. Change them in `.env` before sharing the host or using a non-local environment. Never commit `.env`.

## Run and verify the DAG

In the Airflow UI, enable `customer_care_emails_csv_to_minio_parquet` and select **Trigger DAG**. The schedule is manual (`schedule=None`) and `catchup=False`. Alternatively:

```bash
docker compose exec airflow-scheduler airflow dags trigger customer_care_emails_csv_to_minio_parquet
```

The DAG runs `validate_contract` → `transform_to_partitioned_parquet` → `upload_parquet_to_minio`. The provided contract requires all 13 columns in the exact order, rejects missing or renamed columns, duplicate headers, extra fields, malformed timestamps, null or empty required fields, values outside documented categories, and satisfaction scores outside `[0, 1]`. A failure in validation blocks both downstream tasks, so no Parquet is uploaded for an invalid CSV.

Inspect successful objects in the MinIO console under the bucket and `customer_care_emails/` prefix. The Hive-style layout is:

```text
customer_care_emails/timestamp_date=2023-10-26/part-<run-id>.parquet
customer_care_emails/timestamp_date=2023-10-27/part-<run-id>.parquet
```

`timestamp_date` is derived from each source timestamp normalized to UTC. Each partition's Parquet file contains the cleaned original columns; the partition date is represented by the directory. Run-specific object names avoid overwriting previous runs. A small temporary Parquet file is created under `/tmp` inside the Airflow container and removed after upload.

## Safe contract check

To try a negative check, back up the sample CSV and temporarily change a header (for example, `sender` to `sender_typo`). Trigger the DAG and confirm `validate_contract` fails before either transform or upload runs. Restore the original file and trigger again. Because the DAG folder is mounted read-only, edit the host CSV and retry.

## Operations

```bash
docker compose ps
docker compose logs -f airflow-scheduler
docker compose down                 # stop services, retain named volumes
docker compose down --volumes       # also delete Airflow/MinIO persisted data
```

The final command permanently deletes the local Airflow database and MinIO objects. The project intentionally uses Airflow LocalExecutor and a single Airflow metadata Postgres service to keep this training stack small.
