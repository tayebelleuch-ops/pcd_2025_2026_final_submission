# PCD 2026 TNS

PCD 2026 TNS is a data engineering and AI-powered decision-support project built around agricultural analytics.
It combines Airflow orchestration, a FastAPI analytics/chat backend, MinIO object storage, PostgreSQL operational storage, and ClickHouse analytical storage.

## Project structure

- `airflow/` — DAGs, plugins, and Docker image for orchestration and ingestion pipelines.
- `api/` — FastAPI backend with analytics, tool integration, and API key authentication.
- `frontend/` — Vite/React UI for user interaction.
- `docker-compose.yaml` — local deployment manifest for Airflow, MinIO, PostgreSQL, ClickHouse, and the analytics API.
- `testenv/` — test utilities and example scripts for local development.

## Features

- Ingests data into MinIO and PostgreSQL via Airflow.
- Executes analytics using ClickHouse.
- Exposes a fast analytics API with secure API key support.
- Uses environment variables for configuration and secret management.

## Prerequisites

- Docker
- Docker Compose v2
- Node.js and npm/yarn (for frontend development)

## Setup

1. Copy the environment templates:

```bash
cp .env.example .env
cp api/.env.example api/.env
```

2. Edit `.env` and `api/.env` with your local values.
3. Build and start the stack:

```bash
docker compose up -d --build
```

4. Optionally initialize Airflow if the init service does not run automatically:

```bash
docker compose up airflow-init
```

## Services

- Airflow webserver: `http://localhost:8080`
- MinIO object store: `http://localhost:9000`
- MinIO console: `http://localhost:9001`
- Analytics API: `http://localhost:8001`

## Frontend

To run the frontend locally:

```bash
cd frontend
npm install
npm run dev
```

The default Vite host is usually `http://localhost:5173`.

## Environment variables

### Root `.env`

Required variables for Docker Compose and Airflow:

```ini
# Airflow metadata database
AIRFLOW_DB_NAME=airflow
AIRFLOW_DB_USER=airflow
AIRFLOW_DB_PASS=airflow_pass

# MinIO storage
MINIO_ENDPOINT=http://minio:9000
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minio123

# Operational PostgreSQL
OP_DB_HOST=op-db
OP_DB_NAME=op_db
OP_DB_USER=op_user
OP_DB_PASS=op_pass
OP_DB_PORT=5432

# Analytical ClickHouse
CLICKHOUSE_HOST=analytics-db
CLICKHOUSE_DB=analytics_db
CLICKHOUSE_USER=analytics_user
CLICKHOUSE_PASSWORD=analytics_pass
CLICKHOUSE_HTTP_PORT=8123
CLICKHOUSE_TCP_PORT=9000

# AI and API keys
OPENAI_API_KEY=
GEMINI_API_KEY=
API_KEYS=demo_key_12345
```

### API `api/.env`

The FastAPI service expects:

```ini
POSTGRES_URL=postgresql+asyncpg://op_user:op_pass@op-db:5432/op_db
CLICKHOUSE_HOST=analytics-db
CLICKHOUSE_HTTP_PORT=8123
CLICKHOUSE_TCP_PORT=9000
CLICKHOUSE_DATABASE=analytics_db
CLICKHOUSE_USER=analytics_user
CLICKHOUSE_PASSWORD=analytics_pass
API_KEYS=demo_key_12345
LOG_LEVEL=INFO
OPENAI_API_KEY=
GEMINI_API_KEY=
```

## Notes

- `.env` files should not be committed to version control. `.gitignore` already excludes `.env`.
- The example credentials in this repo are for development only and should be replaced in production.
- The Docker Compose stack uses internal service hostnames such as `op-db`, `analytics-db`, and `minio`.

## Troubleshooting

- If Airflow cannot connect to PostgreSQL, verify `AIRFLOW_DB_USER`, `AIRFLOW_DB_PASS`, and `AIRFLOW_DB_NAME`.
- If the API cannot connect to ClickHouse, verify `CLICKHOUSE_HOST`, `CLICKHOUSE_USER`, and `CLICKHOUSE_PASSWORD` plus the port mappings.
- Use `docker compose logs -f <service>` to inspect individual container logs.
