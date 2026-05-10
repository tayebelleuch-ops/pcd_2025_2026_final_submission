
import openmeteo_requests
import requests_cache
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from retry_requests import retry
from datetime import timedelta, date
import logging
from commun import get_fs

# Configuration du logging
logger = logging.getLogger(__name__)
FORECAST_DAILY_PARAMETERS = [
    "weather_code",
    "temperature_2m_max",
    "temperature_2m_min",
    "precipitation_sum",
    "wind_speed_10m_max",
    "soil_moisture_0_to_100cm_mean",
]

def fetch_forecast(latitude, longitude, **context):
    """
    Fonction principale pour extraire les prévisions météo (Forecast) depuis Open-Meteo.
    
    Cette fonction :
    1. Détermine si la date demandée est dans le passé ou le futur.
    2. Sélectionne intelligemment l'API :
       - 'Forecast' pour les dates futures ou aujourd'hui.
       - 'Archive' pour reconstituer des prévisions passées (Backfill).
    3. Extrait les données sur 7 jours à partir de la date d'exécution.
    4. Sauvegarde les résultats au format Parquet dans le dossier 'prediction'.

    Args:
        latitude (float): Latitude du lieu.
        longitude (float): Longitude du lieu.
        **context: Contexte d'exécution Airflow (contient 'execution_date').
    """
    
    # -------------------------------------------------------------------------
    # 1. INITIALISATION ET CALCUL DES DATES
    # -------------------------------------------------------------------------
    
    if 'execution_date' not in context:
        logger.error("Contexte Airflow manquant (execution_date requis).")
        raise ValueError("Le paramètre 'execution_date' est manquant dans le contexte.")

    execution_date = context['execution_date']
    
    # Conversion en date simple pour la comparaison
    exec_date_obj = execution_date.date() if hasattr(execution_date, 'date') else execution_date
    today = date.today()
    
    logger.info(f"Début de l'extraction prévisionnelle OpenMeteo pour la date : {execution_date}")

    # Période de prévision : 7 jours à partir de la date d'exécution
    # Du jour J au jour J+7 inclus (donc 8 jours au total, ou J à J+6 pour 7 jours stricts)
    # Le script original demandait implicitement les 7 prochains jours.
    # Ici, on définit explicitement la plage.
    target_start_date = execution_date
    target_end_date = execution_date + timedelta(days=7)
    
    start_date_str = target_start_date.strftime("%Y-%m-%d")
    end_date_str = target_end_date.strftime("%Y-%m-%d")
    
    logger.info(f"Intervalle demandé : Du {start_date_str} au {end_date_str}")

    # -------------------------------------------------------------------------
    # 2. SELECTION DE L'API ET CONFIGURATION
    # -------------------------------------------------------------------------
    
    cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    archive_latest_date = today
    target_end_date_obj = target_end_date.date() if hasattr(target_end_date, 'date') else target_end_date

    # Si toute la fenêtre J -> J+7 est déjà historisée, on utilise Archive.
    # Le Forecast endpoint peut retourner les dates demandées mais avec des valeurs nulles
    # pour une fenêtre passée, ce qui produisait ensuite metrics={} dans PostgreSQL.
    if target_end_date_obj <= archive_latest_date:
        logger.info(
            f"Fenêtre passée {start_date_str} -> {end_date_str}. "
            "Utilisation de l'API ARCHIVE."
        )
        url = "https://archive-api.open-meteo.com/v1/archive"
    else:
        logger.info(
            f"Fenêtre récente/future {start_date_str} -> {end_date_str}. "
            "Utilisation de l'API FORECAST."
        )
        url = "https://api.open-meteo.com/v1/forecast"

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date_str,
        "end_date": end_date_str,
        "daily": FORECAST_DAILY_PARAMETERS,
    }

    # -------------------------------------------------------------------------
    # 3. APPEL API ET TRAITEMENT
    # -------------------------------------------------------------------------
    try:
        logger.info(f"Appel de l'API Open-Meteo ({url}) avec params : {params}")
        responses = openmeteo.weather_api(url, params=params)
    except Exception as e:
        logger.error(f"Erreur lors de l'appel API Open-Meteo : {e}")
        raise

    response = responses[0]
    
    logger.info(f"Coordonnées reçues : {response.Latitude()}°N {response.Longitude()}°E")

    # Traitement des données journalières
    logger.info("Traitement des données journalières (Daily)...")
    daily = response.Daily()
    
    # Récupération des variables dans l'ordre demandé
    daily_weather_code = daily.Variables(0).ValuesAsNumpy()
    daily_temperature_2m_max = daily.Variables(1).ValuesAsNumpy()
    daily_temperature_2m_min = daily.Variables(2).ValuesAsNumpy()
    daily_precipitation_sum = daily.Variables(3).ValuesAsNumpy()
    daily_wind_speed_10m_max = daily.Variables(4).ValuesAsNumpy()
    daily_soil_moisture_0_to_100cm_mean = daily.Variables(5).ValuesAsNumpy()

    daily_data = {
        "date": pd.date_range(
            start=pd.to_datetime(daily.Time(), unit="s", utc=True),
            end=pd.to_datetime(daily.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=daily.Interval()),
            inclusive="left"
        )
    }
    
    daily_data["weather_code"] = daily_weather_code
    daily_data["temperature_2m_max"] = daily_temperature_2m_max
    daily_data["temperature_2m_min"] = daily_temperature_2m_min
    daily_data["precipitation_sum"] = daily_precipitation_sum
    daily_data["wind_speed_10m_max"] = daily_wind_speed_10m_max
    daily_data["soil_moisture_0_to_100cm_mean"] = daily_soil_moisture_0_to_100cm_mean

    daily_dataframe = pd.DataFrame(data=daily_data)
    logger.info(f"Données de prévision extraites : {len(daily_dataframe)} lignes.")
    non_null_counts = daily_dataframe[FORECAST_DAILY_PARAMETERS].notna().sum().to_dict()
    logger.info(f"Valeurs prévisionnelles non nulles : {non_null_counts}")

    if (daily_dataframe[FORECAST_DAILY_PARAMETERS].notna().sum(axis=1) == 0).any():
        raise ValueError(
            "OpenMeteo a retourne au moins une ligne de prediction sans aucune metrique. "
            f"Endpoint utilise: {url}, intervalle: {start_date_str} -> {end_date_str}, "
            f"valeurs non nulles: {non_null_counts}"
        )

    # -------------------------------------------------------------------------
    # 4. SAUVEGARDE DANS MINIO (DOSSIER PREDICTION)
    # -------------------------------------------------------------------------
    
    try:
        fs = get_fs()
    except Exception as e:
        logger.critical(f"Erreur de connexion MinIO : {e}")
        raise

    # Structure : prediction/openmeteo/year=YYYY/month=MM/day=DD/data.parquet
    year = execution_date.year
    month = f"{execution_date.month:02d}"
    day = f"{execution_date.day:02d}"

    # NOTE : Le dossier s'appelle 'prediction' (en anglais correct) comme validé dans le plan.
    path_prediction = f"raw/prediction/openmeteo/year={year}/month={month}/day={day}/data.parquet"

    try:
        logger.info(f"Sauvegarde des prévisions dans MinIO : {path_prediction}")
        table = pa.Table.from_pandas(daily_dataframe)
        with fs.open(path_prediction, "wb") as f:
            pq.write_table(table, f)
        logger.info("Sauvegarde Prediction terminée avec succès.")
    except Exception as e:
        logger.error(f"Echec sauvegarde Prediction MinIO : {e}")
        raise

    logger.info("Extraction Prévision OpenMeteo terminée.")
