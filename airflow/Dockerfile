FROM apache/airflow:2.10.2

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.2/constraints-3.12.txt" \
    pandas \
    numpy \
    #postgres
    psycopg2-binary \
    clickhouse-connect \
    #MinIO
    boto3 \
    s3fs \
    #parquet
    pyarrow \
    #clickhouse
    clickhouse-driver \
    #misallagnous
    openmeteo_requests \
    requests_cache \
    retry_requests

