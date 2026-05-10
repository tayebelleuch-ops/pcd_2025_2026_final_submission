from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Imports depuis les modules réorganisés
# Imports depuis les modules réorganisés
from openmeteo import fetch as openmeteo_fetch, normalize_openmeteo_daily
from nasapower import fetch as nasapower_fetch, normalize_nasapower
from loading import (
    load_daily_pg,
    load_hourly_pg,
    load_daily_ch,
    load_hourly_ch
)

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="pcd_ingestion_weather",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["pcd", "ingestion", "weather"],
) as dag:

    # --- 1. Extraction Tasks ---
    extract_openmeteo = PythonOperator(
        task_id="extract_openmeteo",
        python_callable=openmeteo_fetch,
        op_kwargs={
            "latitude": 34,
            "longitude": 9,
        },
    )

    extract_nasa_power = PythonOperator(
        task_id="extract_nasa_power",
        python_callable=nasapower_fetch,
        op_kwargs={
            "latitude": 34,
            "longitude": 9,
        },
    )

    # --- 2. Normalization Tasks ---
    normalize_openmeteo_task = PythonOperator(
        task_id="normalize_openmeteo",
        python_callable=normalize_openmeteo_daily,
    )

    normalize_nasa_power = PythonOperator(
        task_id="normalize_nasa_power",
        python_callable=normalize_nasapower,
    )

    # --- 3. Loading Tasks (Unified Tables) ---
    # Données Daily (Fusion OpenMeteo + NasaPower)
    
    load_daily_pg_task = PythonOperator(
        task_id="load_daily_pg",
        python_callable=load_daily_pg,
    )
    
    load_daily_ch_task = PythonOperator(
        task_id="load_daily_ch",
        python_callable=load_daily_ch,
    )
    
    # Données Hourly (OpenMeteo uniquement)
    load_hourly_pg_task = PythonOperator(
        task_id="load_hourly_pg",
        python_callable=load_hourly_pg,
    )
    
    load_hourly_ch_task = PythonOperator(
        task_id="load_hourly_ch",
        python_callable=load_hourly_ch,
    )

    # --- 4. Dependencies ---
    # Extract -> Normalize -> Load (Unified Tables)
    
    # Les deux sources doivent être normalisées avant la fusion et le chargement Daily
    extract_openmeteo >> normalize_openmeteo_task
    extract_nasa_power >> normalize_nasa_power
    
    # Chargement Daily : Attend les 2 normalisations (fusion OpenMeteo+NasaPower)
    # Syntaxe correcte pour multiple sources -> multiple destinations
    [normalize_openmeteo_task, normalize_nasa_power] >> load_daily_pg_task
    [normalize_openmeteo_task, normalize_nasa_power] >> load_daily_ch_task
    
    # Chargement Hourly : Dépend uniquement de la normalisation OpenMeteo
    normalize_openmeteo_task >> [load_hourly_pg_task, load_hourly_ch_task]
