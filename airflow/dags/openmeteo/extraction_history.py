
import openmeteo_requests
import requests_cache
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from retry_requests import retry
from datetime import timedelta
import logging
from commun import get_fs

# Configuration du logging
logger = logging.getLogger(__name__)

def fetch_history(latitude, longitude, **context):
    """
    Fonction principale pour extraire l'historique météo (Hourly et Daily) depuis Open-Meteo.
    
    Cette fonction :
    1. Calcule la date cible (J-2 par rapport à la date d'exécution Airflow).
    2. Interroge l'API 'Archive' d'Open-Meteo.
    3. Traite les données horaires (Hourly) et journalières (Daily).
    4. Sauvegarde les résultats au format Parquet dans MinIO avec une structure de dossiers précise.

    Args:
        latitude (float): Latitude du lieu.
        longitude (float): Longitude du lieu.
        **context: Contexte d'exécution Airflow (contient 'execution_date' et 'ds').
    """
    
    # -------------------------------------------------------------------------
    # 1. INITIALISATION ET CALCUL DES DATES
    # -------------------------------------------------------------------------
    
    # Récupération de la date d'exécution depuis le contexte Airflow
    # Si le contexte n'est pas fourni (ex: test manuel), on utilise une date par défaut ou on lève une erreur
    if 'execution_date' not in context:
        logger.error("Contexte Airflow manquant (execution_date requis).")
        raise ValueError("Le paramètre 'execution_date' est manquant dans le contexte.")

    execution_date = context['execution_date']
    
    # Log pour le suivi
    logger.info(f"Début de l'extraction historique OpenMeteo pour la date d'exécution : {execution_date}")

    # La date cible est définie comme 2 jours avant la date d'exécution (délai de l'archive Open-Meteo)
    target_date = execution_date - timedelta(days=2)
    start_date_str = target_date.strftime("%Y-%m-%d")
    end_date_str = target_date.strftime("%Y-%m-%d")
    
    logger.info(f"Date cible calculée (J-2) : {start_date_str}")

    # -------------------------------------------------------------------------
    # 2. CONFIGURATION DU CLIENT OPEN-METEO
    # -------------------------------------------------------------------------
    
    # Configuration du cache pour éviter les requêtes redondantes (expire après 1h)
    cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
    # Configuration du retry en cas d'erreur réseau (5 essais, backoff exponentiel)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    # Création du client
    openmeteo = openmeteo_requests.Client(session=retry_session)

    # URL de l'API Archive
    url = "https://archive-api.open-meteo.com/v1/archive"
    
    # Paramètres de la requête API
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date_str,
        "end_date": end_date_str,
        # Variables journalières demandées
        "daily": ["temperature_2m_mean", "temperature_2m_max", "temperature_2m_min", "precipitation_sum"],
        # Variables horaires demandées
        "hourly": "surface_pressure",
    }

    # -------------------------------------------------------------------------
    # 3. APPEL API ET RÉCUPÉRATION DES RÉPONSES
    # -------------------------------------------------------------------------
    try:
        logger.info(f"Appel de l'API Open-Meteo Archive : {url}")
        responses = openmeteo.weather_api(url, params=params)
    except Exception as e:
        logger.error(f"Erreur lors de l'appel API Open-Meteo : {e}")
        raise

    # Traitement de la première réponse (car nous ne demandons qu'une seule localisation)
    response = responses[0]
    
    # Affichage des métadonnées pour le debug
    logger.info(f"Coordonnées reçues : {response.Latitude()}°N {response.Longitude()}°E")
    logger.info(f"Altitude : {response.Elevation()} m asl")

    # -------------------------------------------------------------------------
    # 4. TRAITEMENT DES DONNÉES HORAIRES (HOURLY)
    # -------------------------------------------------------------------------
    logger.info("Traitement des données horaires (Hourly)...")
    
    hourly = response.Hourly()
    
    # Extraction des valeurs numpy
    # L'ordre doit correspondre exactement à la liste 'hourly' dans params
    hourly_surface_pressure = hourly.Variables(0).ValuesAsNumpy()

    # Création du dictionnaire de données horaires
    hourly_data = {
        "date": pd.date_range(
            start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
            end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=hourly.Interval()),
            inclusive="left"
        )
    }
    hourly_data["surface_pressure"] = hourly_surface_pressure

    # Conversion en DataFrame Pandas
    hourly_dataframe = pd.DataFrame(data=hourly_data)
    logger.info(f"Données horaires extraites : {len(hourly_dataframe)} lignes.")

    # -------------------------------------------------------------------------
    # 5. TRAITEMENT DES DONNÉES JOURNALIÈRES (DAILY)
    # -------------------------------------------------------------------------
    logger.info("Traitement des données journalières (Daily)...")
    
    daily = response.Daily()
    
    # Extraction des valeurs numpy
    # L'ordre doit correspondre exactement à la liste 'daily' dans params
    daily_temperature_2m_mean = daily.Variables(0).ValuesAsNumpy()
    daily_temperature_2m_max = daily.Variables(1).ValuesAsNumpy()
    daily_temperature_2m_min = daily.Variables(2).ValuesAsNumpy()
    daily_precipitation_sum = daily.Variables(3).ValuesAsNumpy()

    # Création du dictionnaire de données journalières
    daily_data = {
        "date": pd.date_range(
            start=pd.to_datetime(daily.Time(), unit="s", utc=True),
            end=pd.to_datetime(daily.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=daily.Interval()),
            inclusive="left"
        )
    }
    
    daily_data["temperature_2m_mean"] = daily_temperature_2m_mean
    daily_data["temperature_2m_max"] = daily_temperature_2m_max
    daily_data["temperature_2m_min"] = daily_temperature_2m_min
    daily_data["precipitation_sum"] = daily_precipitation_sum

    # Conversion en DataFrame Pandas
    daily_dataframe = pd.DataFrame(data=daily_data)
    logger.info(f"Données journalières extraites : {len(daily_dataframe)} lignes.")

    # -------------------------------------------------------------------------
    # 6. SAUVEGARDE DANS MINIO
    # -------------------------------------------------------------------------
    
    # Connexion au système de fichiers MinIO via le module commun
    try:
        fs = get_fs()
    except Exception as e:
        logger.critical(f"Erreur de connexion MinIO : {e}")
        raise

    # Définition des chemins de sauvegarde
    # Structure : hourly/openmeteo/year=YYYY/month=MM/day=DD/data.parquet
    # Structure : daily/openmeteo/year=YYYY/month=MM/day=DD/data.parquet
    
    year = target_date.year
    month = f"{target_date.month:02d}"
    day = f"{target_date.day:02d}"

    path_hourly = f"raw/hourly/openmeteo/year={year}/month={month}/day={day}/data.parquet"
    path_daily = f"raw/daily/openmeteo/year={year}/month={month}/day={day}/data.parquet"

    # Sauvegarde Hourly
    try:
        logger.info(f"Sauvegarde des données horaires dans MinIO : {path_hourly}")
        table_hourly = pa.Table.from_pandas(hourly_dataframe)
        with fs.open(path_hourly, "wb") as f:
            pq.write_table(table_hourly, f)
        logger.info("Sauvegarde Hourly terminée avec succès.")
    except Exception as e:
        logger.error(f"Echec sauvegarde Hourly MinIO : {e}")
        raise

    # Sauvegarde Daily
    try:
        logger.info(f"Sauvegarde des données journalières dans MinIO : {path_daily}")
        table_daily = pa.Table.from_pandas(daily_dataframe)
        with fs.open(path_daily, "wb") as f:
            pq.write_table(table_daily, f)
        logger.info("Sauvegarde Daily terminée avec succès.")
    except Exception as e:
        logger.error(f"Echec sauvegarde Daily MinIO : {e}")
        raise

    logger.info("Extraction historique OpenMeteo terminée globalement.")
