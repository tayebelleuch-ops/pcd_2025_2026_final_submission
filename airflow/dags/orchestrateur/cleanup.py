from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging
from loading.loading import get_pg_engine, get_ch_client
from sqlalchemy import text, inspect

logger = logging.getLogger(__name__)

def cleanup_clickhouse():
    """Drops every table in the current ClickHouse database."""
    client = get_ch_client()
    try:
        # Get list of tables
        tables = client.query("SHOW TABLES").result_rows
        for [table] in tables:
            logger.info(f"Dropping ClickHouse table: {table}")
            client.command(f"DROP TABLE IF EXISTS {table}")
    finally:
        client.close()
def cleanup_postgres():
    """Drops every table in the public schema of PostgreSQL."""
    engine = get_pg_engine()
    inspector = inspect(engine)
    
    try:
        # Get all table names
        tables = inspector.get_table_names(schema='public')
        
        with engine.begin() as conn:
            for table in tables:
                logger.info(f"Dropping Postgres table: {table}")
                # CASCADE is vital here to handle foreign keys/dependencies
                conn.execute(text(f'DROP TABLE IF EXISTS public."{table}" CASCADE;'))
    except Exception as e:
        logger.error(f"Postgres Drop Failed: {e}")
        raise
with DAG(
    dag_id='test_db_cleanup',
    start_date=datetime(2026, 1, 1),
    schedule=None,  # Manual trigger only
    catchup=False,
    tags=['testing'],
) as dag:

    task_ck = PythonOperator(
        task_id='cleanup_clickhouse',
        python_callable=cleanup_clickhouse,
    )

    task_pg = PythonOperator(
        task_id='cleanup_postgres',
        python_callable=cleanup_postgres,
    )

    task_ck >> task_pg