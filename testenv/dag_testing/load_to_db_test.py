import pandas as pd
import s3fs
from sqlalchemy import create_engine

print("SCRIPT STARTED")

# Read from MinIO
fs = s3fs.S3FileSystem(
    key="minio",
    secret="minio123",
    client_kwargs={"endpoint_url": "http://localhost:9000"},
)

df = pd.read_parquet(
    "raw/test/openmeteo/year=2026/month=01/day=24/data.parquet",
    filesystem=fs,
)

# Write to PostgreSQL
engine = create_engine(
    "postgresql+psycopg2://op_user:op_pass@localhost:5433/op_db"
)

df.to_sql(
    "weather_daily_op",
    engine,
    if_exists="append",
    index=False,
)
