
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timedelta
from commun import get_fs
import logging

# Configuration du Logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


def normalize_nasapower(**context):
    """
    Lit les données brutes NasaPower (format long), les pivote vers un format large,
    et écrit dans le bucket 'normalized/daily'.
    Intègre une gestion d'erreurs et des logs détaillés.
    
    Args:
        **context: Contexte Airflow contenant la date d'exécution
    """
    try:
        # Récupération et préparation de la date cible
        execution_date = context["ds"]
        logger.info(f"Début de la normalisation NasaPower pour {execution_date}")

        execution_dt = datetime.strptime(execution_date, "%Y-%m-%d")
        target_date = execution_dt - timedelta(days=2)
        
        # Connexion MinIO
        try:
            fs = get_fs()
        except Exception as e:
            logger.critical(f"Erreur connexion MinIO : {e}")
            raise ConnectionError("MinIO inaccessible")
        
        # Chemin source Raw pour NasaPower
        raw_path = (
            f"raw/daily/nasapower/"
            f"year={target_date.year}/"
            f"month={target_date.month:02d}/"
            f"day={target_date.day:02d}/"
            f"data.parquet"
        )
        
        # Vérification de l'existence du fichier
        if not fs.exists(raw_path):
            logger.warning(f"Fichier source non trouvé : {raw_path}. Arrêt de la normalisation.")
            return # Pas une erreur critique, juste pas de données à traiter
        
        logger.info(f"Lecture fichier source : {raw_path}")
        
        try:
            with fs.open(raw_path, "rb") as f:
                df = pd.read_parquet(f)
        except Exception as e:
            logger.error(f"Fichier Parquet corrompu ou illisible : {e}")
            raise IOError("Erreur lecture Parquet")
            
        if df.empty:
            logger.warning("Fichier source vide.")
            return

        # --- Logique de Normalisation ---
        
        # 1. Pivotement des données : transformation du format "Long" vers "Wide"
        try:
            df_pivot = df.pivot(index="date", columns="parameter", values="value").reset_index()
        except KeyError as e:
            logger.error(f"Colonnes manquantes pour le pivot (date/parameter/value) : {e}")
            raise ValueError("Structure données brutes invalide")
        
        # 2. Dictionnaire de mappage pour renommer les codes NASA cryptiques en noms lisibles
        column_mapping = {
            "T2M": "temperature_2m_mean",             # Température à 2m (Celsius)
            "PRECTOTCORR": "precipitation_sum",      # Précipitation corrigée (mm)
            "RH2M": "relative_humidity_2m",     # Humidité relative (%)
            "WS2M": "wind_speed_2m",             # Vitesse du vent (m/s)
            "ALLSKY_SFC_SW_DWN": "all_sky_surface_shortwave_radiation_downward", # Rayonnement solaire (All Sky)
            "CLRSKY_SFC_SW_DWN": "clear_sky_surface_shortwave_radiation_downward" # Rayonnement solaire (Ciel clair)
        }
        # Application du renommage
        df_pivot = df_pivot.rename(columns=column_mapping)
        
        # 2.5 Nettoyage des données
        
        # Remplacer les valeurs manquantes spécifiques à NASA (-999) par pd.NA
        df_pivot = df_pivot.replace(-999, pd.NA)

        # Conversion forcée de toutes les colonnes de mesures en type numérique (float)
        cols_to_numeric = [
            "temperature_2m_mean", "precipitation_sum", "relative_humidity_2m", 
            "wind_speed_2m", "all_sky_surface_shortwave_radiation_downward", 
            "clear_sky_surface_shortwave_radiation_downward"
        ]
        for col in cols_to_numeric:
            if col in df_pivot.columns:
                df_pivot[col] = pd.to_numeric(df_pivot[col], errors='coerce')
            else:
                logger.warning(f"Colonne attendue manquante : {col}")

        # --- Application des Règles de Nettoyage Avancées ---

        # 1. TEMPÉRATURE (T2M) - Bounds + Interpolation Intelligente
        if "temperature_2m_mean" in df_pivot.columns:
            # Bornes réalistes pour la Tunisie : [-50°C, +60°C]
            temp_col = "temperature_2m_mean"
            mask_out_of_bounds = ~df_pivot[temp_col].between(-50, 60)
            
            if mask_out_of_bounds.any():
                num_invalid = mask_out_of_bounds.sum()
                logger.warning(f"🌡️ {num_invalid} températures hors bornes [-50, 60]°C détectées. Application de l'interpolation temporelle.")
                # Marquer les valeurs aberrantes comme NaN pour imputation
                df_pivot.loc[mask_out_of_bounds, temp_col] = pd.NA
            
            # Imputation Intelligente : Interpolation temporelle (linéaire basée sur le temps)
            df_pivot[temp_col] = df_pivot[temp_col].interpolate(method='linear', limit_direction='both')
            
            # Si l'interpolation échoue (ex: premier/dernier point), utiliser Forward/Backward Fill
            df_pivot[temp_col] = df_pivot[temp_col].ffill().bfill()

        # 2. PRÉCIPITATIONS (PRECTOTCORR) - Bounds Strictes
        if "precipitation_sum" in df_pivot.columns:
            precip_col = "precipitation_sum"
            
            # Correction des valeurs négatives (impossible physiquement)
            mask_neg = df_pivot[precip_col] < 0
            if mask_neg.any():
                logger.info(f"🌧️ Correction de {mask_neg.sum()} précipitations négatives → 0")
                df_pivot.loc[mask_neg, precip_col] = 0
            
            # Gestion des aberrations > 500mm (outliers extrêmes)
            mask_outlier = df_pivot[precip_col] > 500
            if mask_outlier.any():
                logger.warning(f"⚠️ {mask_outlier.sum()} précipitations > 500mm détectées → Mise à 0")
                df_pivot.loc[mask_outlier, precip_col] = 0

        # 3. RAYONNEMENT SOLAIRE - Cohérence Physique (All Sky <= Clear Sky)
        if ("all_sky_surface_shortwave_radiation_downward" in df_pivot.columns and 
            "clear_sky_surface_shortwave_radiation_downward" in df_pivot.columns):
            
            all_sky = "all_sky_surface_shortwave_radiation_downward"
            clear_sky = "clear_sky_surface_shortwave_radiation_downward"
            
            # La radiation "All Sky" (réelle) ne peut jamais dépasser "Clear Sky" (théorique max)
            mask_invalid = df_pivot[all_sky] > df_pivot[clear_sky]
            
            if mask_invalid.any():
                num_invalid = mask_invalid.sum()
                logger.warning(f"☀️ {num_invalid} valeurs AllSky > ClearSky détectées. Plafonnement à ClearSky.")
                # Plafonner AllSky à la valeur de ClearSky (maximum théorique)
                df_pivot.loc[mask_invalid, all_sky] = df_pivot.loc[mask_invalid, clear_sky]

        # 4. HUMIDITÉ RELATIVE (RH2M) - Clipping [0, 100]%
        if "relative_humidity_2m" in df_pivot.columns:
            humidity_col = "relative_humidity_2m"
            
            # L'humidité est strictement comprise entre 0% et 100%
            original_min = df_pivot[humidity_col].min()
            original_max = df_pivot[humidity_col].max()
            
            df_pivot[humidity_col] = df_pivot[humidity_col].clip(lower=0, upper=100)
            
            if original_min < 0 or original_max > 100:
                logger.info(f"💧 Humidité clippée : [{original_min:.1f}, {original_max:.1f}] → [0, 100]")

        # 5. VITESSE DU VENT & RAYONNEMENT - Non-Négativité
        for col in ["wind_speed_2m", all_sky, clear_sky]:
            if col in df_pivot.columns:
                mask_neg = df_pivot[col] < 0
                if mask_neg.any():
                    logger.info(f"🌬️ Correction de {mask_neg.sum()} valeurs négatives dans {col} → 0")
                    df_pivot.loc[mask_neg, col] = 0


        # --- Ajout des Métadonnées (Traçabilité) ---
        df_pivot["extraction_date"] = datetime.now().date()
        df_pivot["source"] = "nasapower"


        # Chemin de destination Normalisé
        dest_path = (
            f"normalized/daily/nasapower/"
            f"year={target_date.year}/"
            f"month={target_date.month:02d}/"
            f"day={target_date.day:02d}/"
            f"data.parquet"
        )
        
        logger.info(f"Écriture des données normalisées vers {dest_path}...")
        
        # Écriture finale
        try:
            table = pa.Table.from_pandas(df_pivot)
            with fs.open(dest_path, "wb") as f:
                pq.write_table(table, f)
            logger.info("Normalisation NasaPower terminée avec succès.")
        except Exception as e:
            logger.error(f"Erreur écriture fichier normalisé MinIO : {e}")
            raise IOError("Echec sauvegarde normalisation")

    except Exception as e:
        logger.error(f"Erreur critique Normalisation NasaPower : {e}", exc_info=True)
        raise
