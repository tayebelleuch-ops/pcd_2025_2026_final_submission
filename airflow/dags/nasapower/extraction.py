
import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timedelta
from commun import get_fs
import logging

# Configuration du Logging pour ce module
# Cela permet d'avoir des logs structurés (Heure - Niveau - Message)
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def fetch(latitude, longitude, **context):
    """
    Extrait les données météorologiques depuis l'API NASA POWER.
    Gère les erreurs de connexion et de format de données.
    
    Args:
        latitude: Latitude de la localisation
        longitude: Longitude de la localisation
        **context: Contexte Airflow contenant la date d'exécution
    """
    try:
        # Get execution date from Airflow context
        ds = context["ds"]
        execution_date = datetime.strptime(ds, "%Y-%m-%d")
        
        logger.info(f"Début de l'extraction NasaPower pour la date d'exécution : {ds}")
        
        # Target date is 2 days ago
        target_date = execution_date - timedelta(days=2)
        start_str = target_date.strftime("%Y%m%d")
        end_str = target_date.strftime("%Y%m%d")

        url = "https://power.larc.nasa.gov/api/temporal/daily/point"
        params = {
            "start": start_str,
            "end": end_str,
            "latitude": latitude,
            "longitude": longitude,
            "community": "RE",
            "parameters": "T2M,PRECTOTCORR,RH2M,WS2M,ALLSKY_SFC_SW_DWN,CLRSKY_SFC_SW_DWN",
            "format": "JSON",
            "header": "true"
        }

        logger.info(f"Appel API NasaPower : {url} (Params masqués)")
        
        # Appel API avec gestion des Timeouts pour éviter de bloquer indéfiniment
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status() # Lève une exception si le code HTTP est >= 400
        data = response.json()

        logger.info("Données reçues de l'API avec succès.")

        # Parse NASA POWER results
        # Attention : structure JSON complexe de NasaPower
        try:
            params_data = data["properties"]["parameter"]
        except KeyError as e:
            logger.error(f"Format de réponse API inattendu : clé manquante {e}")
            raise ValueError("Structure JSON invalide de l'API NasaPower")

        records = []
        
        # Invert long format (parameter -> date -> value)
        for param_name, date_values in params_data.items():
            for date_str, value in date_values.items():
                records.append({
                    "date": datetime.strptime(date_str, "%Y%m%d"),
                    "parameter": param_name,
                    "value": value
                })
                
        if not records:
            logger.warning("Aucune donnée trouvée pour cette date (records vide).")
            return

        df = pd.DataFrame(records)
        logger.info(f"Transformation en DataFrame terminée : {len(df)} enregistrements.")

        # Dedup: Ensure uniqueness per parameter/date
        df = df.drop_duplicates(subset=["date", "parameter"])

        # Connect to MinIO
        try:
            fs = get_fs()
        except Exception as e:
            logger.critical(f"Impossible de se connecter à MinIO : {e}")
            raise ConnectionError("Erreur critique MinIO")

        # Save partitioned
        unique_dates = df["date"].dt.date.unique()
        
        for single_date in unique_dates:
            daily_df = df[df["date"].dt.date == single_date]
            
            path = (
                f"raw/daily/nasapower/"
                f"year={single_date.year}/"
                f"month={single_date.month:02d}/"
                f"day={single_date.day:02d}/"
                f"data.parquet"
            )

            try:
                table = pa.Table.from_pandas(daily_df)
                with fs.open(path, "wb") as f:
                    pq.write_table(table, f)
                logger.info(f"Sauvegarde réussie dans MinIO : {path}")
            except Exception as e:
                logger.error(f"Erreur lors de l'écriture du fichier {path} dans MinIO : {e}")
                raise IOError(f"Echec écriture MinIO : {path}")
                
        logger.info(f"Extraction NasaPower terminée avec succès pour {start_str}")

    except requests.exceptions.HTTPError as e:
        logger.error(f"Erreur HTTP lors de l'appel API : {e}")
        raise # On relance l'erreur pour qu'Airflow marque la tâche en échec (et retente)
    except requests.exceptions.ConnectionError as e:
        logger.error(f"Erreur de connexion (Internet/DNS) : {e}")
        raise
    except Exception as e:
        logger.error(f"Erreur inattendue dans le script d'extraction : {e}", exc_info=True)
        raise
