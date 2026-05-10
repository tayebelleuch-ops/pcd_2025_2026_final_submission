import pandas as pd
import s3fs
from sqlalchemy import create_engine

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

engine = create_engine(
    "postgresql+psycopg2://analytics_user:analytics_pass@localhost:5434/analytics_db"
)

df.to_sql(
    "weather_daily_analytics",
    engine,
    if_exists="append",
    index=False,
)
