
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timedelta, date
from commun import get_fs
import logging

# Configuration du Logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


def _get_path_dates(execution_date_str, type_data="history"):
    """
    Calcule les dates et l'année/mois/jour pour les chemins MinIO.
    Pour l'historique (daily/hourly) : cible = J-2
    Pour la prévision (forecast) : cible = date d'exécution
    """
    execution_dt = datetime.strptime(execution_date_str, "%Y-%m-%d")
    
    if type_data == "history":
        target_date = execution_dt - timedelta(days=2)
    else: # forecast
        target_date = execution_dt

    return target_date, target_date.year, f"{target_date.month:02d}", f"{target_date.day:02d}"


def normalize_openmeteo_daily(**context):
    """
    Normalise les données OpenMeteo Daily (Historique).
    Source: raw/daily/openmeteo/
    Destination: normalized/daily/openmeteo/
    """
    try:
        ds = context["ds"]
        logger.info(f"Début normalisation OpenMeteo DAILY pour {ds}")
        
        target_date, year, month, day = _get_path_dates(ds, "history")
        
        fs = get_fs()
        
        # Chemins
        raw_path = f"raw/daily/openmeteo/year={year}/month={month}/day={day}/data.parquet"
        dest_path = f"normalized/daily/openmeteo/year={year}/month={month}/day={day}/data.parquet"
        
        if not fs.exists(raw_path):
            logger.warning(f"Fichier source absent: {raw_path}")
            return

        with fs.open(raw_path, "rb") as f:
            df = pd.read_parquet(f)
            
        if df.empty:
            logger.warning("Fichier source vide.")
            return

        # --- Transformation ---
        # Renommage
        rename_map = {
            "temperature_2m_mean": "temperature_2m_mean", # Déjà bon, mais explicite
            "temperature_2m_max": "temperature_2m_max",
            "temperature_2m_min": "temperature_2m_min",
            "precipitation_sum": "precipitation_sum"
        }
        df = df.rename(columns=rename_map)
        
        # Nettoyage - Conversion en numérique
        cols_numeric = ["temperature_2m_mean", "temperature_2m_max", "temperature_2m_min", "precipitation_sum"]
        for col in cols_numeric:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # --- Application des Règles de Nettoyage Avancées ---
        
        # 1. COHÉRENCE PHYSIQUE DES TEMPÉRATURES (Min <= Mean <= Max)
        has_temps = all(col in df.columns for col in ["temperature_2m_min", "temperature_2m_mean", "temperature_2m_max"])
        
        if has_temps:
            # Détection et correction : Min > Max (erreur capteur)
            mask_swap = df["temperature_2m_min"] > df["temperature_2m_max"]
            if mask_swap.any():
                num_swaps = mask_swap.sum()
                logger.warning(f"🔄 {num_swaps} incohérences Min > Max détectées. Swap automatique des valeurs.")
                # Échange des colonnes Min et Max
                df.loc[mask_swap, ["temperature_2m_min", "temperature_2m_max"]] = \
                    df.loc[mask_swap, ["temperature_2m_max", "temperature_2m_min"]].values
            
            # Recalcul de Mean si elle est incohérente (Mean < Min ou Mean > Max)
            mask_mean_invalid = (df["temperature_2m_mean"] < df["temperature_2m_min"]) | \
                                (df["temperature_2m_mean"] > df["temperature_2m_max"])
            
            if mask_mean_invalid.any():
                num_recalc = mask_mean_invalid.sum()
                logger.info(f"🧮 {num_recalc} valeurs Mean incohérentes. Recalcul : Mean = (Min + Max) / 2")
                df.loc[mask_mean_invalid, "temperature_2m_mean"] = \
                    (df.loc[mask_mean_invalid, "temperature_2m_min"] + 
                     df.loc[mask_mean_invalid, "temperature_2m_max"]) / 2
        
        # 2. BORNES TEMPÉRATURES + IMPUTATION INTELLIGENTE
        temp_cols = ["temperature_2m_mean", "temperature_2m_max", "temperature_2m_min"]
        for temp_col in temp_cols:
            if temp_col in df.columns:
                # Bornes réalistes : [-50°C, +60°C]
                mask_out_of_bounds = ~df[temp_col].between(-50, 60)
                
                if mask_out_of_bounds.any():
                    num_invalid = mask_out_of_bounds.sum()
                    logger.warning(f"🌡️ {num_invalid} valeurs {temp_col} hors bornes [-50, 60]°C. Interpolation appliquée.")
                    df.loc[mask_out_of_bounds, temp_col] = pd.NA
                
                # Interpolation temporelle
                df[temp_col] = df[temp_col].interpolate(method='linear', limit_direction='both')
                df[temp_col] = df[temp_col].ffill().bfill()

        # 3. PRÉCIPITATIONS - Bounds [0, 500] mm
        if "precipitation_sum" in df.columns:
            precip_col = "precipitation_sum"
            
            # Aucune précipitation négative possible
            mask_neg = df[precip_col] < 0
            if mask_neg.any():
                logger.info(f"🌧️ {mask_neg.sum()} précipitations négatives → 0")
                df.loc[mask_neg, precip_col] = 0
            
            # Outliers extrêmes (> 500mm)
            mask_outlier = df[precip_col] > 500
            if mask_outlier.any():
                logger.warning(f"⚠️ {mask_outlier.sum()} précipitations > 500mm → 0")
                df.loc[mask_outlier, precip_col] = 0


        # Métadonnées
        df["extraction_date"] = datetime.now().date()
        df["source"] = "openmeteo"
        
        # Sauvegarde
        table = pa.Table.from_pandas(df)
        with fs.open(dest_path, "wb") as f:
            pq.write_table(table, f)
            
        logger.info(f"Normalisation OpenMeteo DAILY terminée : {dest_path}")

    except Exception as e:
        logger.error(f"Erreur Normalize OpenMeteo DAILY: {e}", exc_info=True)
        raise


def normalize_openmeteo_hourly(**context):
    """
    Normalise les données OpenMeteo Hourly (Historique).
    Source: raw/hourly/openmeteo/
    Destination: normalized/hourly/openmeteo/
    """
    try:
        ds = context["ds"]
        logger.info(f"Début normalisation OpenMeteo HOURLY pour {ds}")
        
        target_date, year, month, day = _get_path_dates(ds, "history")
        fs = get_fs()
        
        raw_path = f"raw/hourly/openmeteo/year={year}/month={month}/day={day}/data.parquet"
        dest_path = f"normalized/hourly/openmeteo/year={year}/month={month}/day={day}/data.parquet"
        
        if not fs.exists(raw_path):
            logger.warning(f"Fichier source absent: {raw_path}")
            return

        with fs.open(raw_path, "rb") as f:
            df = pd.read_parquet(f)
            
        if df.empty: return

        # --- Transformation ---
        # Renommage (si besoin)
        rename_map = {
            "surface_pressure": "surface_pressure"
        }
        df = df.rename(columns=rename_map)
        # surface_pressure est déjà correct

        # Nettoyage - Conversion en numérique
        if "surface_pressure" in df.columns:
            df["surface_pressure"] = pd.to_numeric(df["surface_pressure"], errors='coerce')
        
        # --- Application des Règles de Nettoyage Avancées ---
        
        # PRESSION ATMOSPHÉRIQUE - Bounds [870, 1090] hPa + Forward Fill
        if "surface_pressure" in df.columns:
            pressure_col = "surface_pressure"
            
            # Bornes basées sur les records mondiaux de pression atmosphérique
            # Min: 870 hPa (Typhon Tip 1979), Max: 1090 hPa (Sibérie)
            mask_out_of_bounds = ~df[pressure_col].between(870, 1090)
            
            if mask_out_of_bounds.any():
                num_invalid = mask_out_of_bounds.sum()
                logger.warning(f"🌀 {num_invalid} valeurs de pression hors bornes [870, 1090] hPa. Mise à NaN → Forward Fill.")
                df.loc[mask_out_of_bounds, pressure_col] = pd.NA
            
            # Forward Fill pour combler les trous
            df[pressure_col] = df[pressure_col].ffill()

        # Métadonnées
        df["extraction_date"] = datetime.now().date()
        df["source"] = "openmeteo"
        
        # Sauvegarde
        table = pa.Table.from_pandas(df)
        with fs.open(dest_path, "wb") as f:
            pq.write_table(table, f)
            
        logger.info(f"Normalisation OpenMeteo HOURLY terminée : {dest_path}")

    except Exception as e:
        logger.error(f"Erreur Normalize OpenMeteo HOURLY: {e}", exc_info=True)
        raise


def normalize_openmeteo_forecast(**context):
    """
    Normalise les données OpenMeteo Forecast (Prédiction).
    Source: raw/prediction/openmeteo/
    Destination: normalized/prediction/openmeteo/
    """
    try:
        ds = context["ds"]
        logger.info(f"Début normalisation OpenMeteo FORECAST pour {ds}")
        
        # Pour les prévisions, on utilise la date d'exécution directement (pas de J-2)
        target_date, year, month, day = _get_path_dates(ds, "forecast")
        fs = get_fs()
        
        raw_path = f"raw/prediction/openmeteo/year={year}/month={month}/day={day}/data.parquet"
        dest_path = f"normalized/prediction/openmeteo/year={year}/month={month}/day={day}/data.parquet"
        
        if not fs.exists(raw_path):
            logger.warning(f"Fichier source absent: {raw_path}")
            return

        with fs.open(raw_path, "rb") as f:
            df = pd.read_parquet(f)
            
        if df.empty: return

        # --- Transformation ---
        # Colonnes attendues : weather_code, temperature_2m_max, temperature_2m_min,
        # precipitation_sum, wind_speed_10m_max, soil_moisture_0_to_100cm_mean
        
        rename_map = {
            "precipitation_sum": "precipitation_sum",
            "wind_speed_10m_max": "wind_speed_10m_max",
            "soil_moisture_0_to_100cm_mean": "soil_moisture_0_to_100cm_mean",
            "weather_code": "weather_code",
            "temperature_2m_max": "temperature_2m_max",
            "temperature_2m_min": "temperature_2m_min"
        }
        df = df.rename(columns=rename_map)
        
        # Nettoyage - Conversion en numérique
        cols_numeric = [
            "temperature_2m_max",
            "temperature_2m_min",
            "precipitation_sum",
            "wind_speed_10m_max",
            "soil_moisture_0_to_100cm_mean",
            "weather_code",
        ]
        for col in cols_numeric:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # --- Application des Règles de Nettoyage Avancées ---
        
        # 1. TEMPÉRATURES (Min/Max) - Bounds + Interpolation
        temp_cols = ["temperature_2m_max", "temperature_2m_min"]
        for temp_col in temp_cols:
            if temp_col in df.columns:
                # Bornes réalistes : [-50°C, +60°C]
                mask_out_of_bounds = ~df[temp_col].between(-50, 60)
                
                if mask_out_of_bounds.any():
                    num_invalid = mask_out_of_bounds.sum()
                    logger.warning(f"🌡️ {num_invalid} valeurs {temp_col} hors bornes [-50, 60]°C. Interpolation appliquée.")
                    df.loc[mask_out_of_bounds, temp_col] = pd.NA
                
                # Interpolation temporelle
                df[temp_col] = df[temp_col].interpolate(method='linear', limit_direction='both')
                df[temp_col] = df[temp_col].ffill().bfill()
        
        # 2. PRÉCIPITATIONS - Bounds [0, 500] mm
        if "precipitation_sum" in df.columns:
            precip_col = "precipitation_sum"
            
            # Correction des valeurs négatives
            mask_neg = df[precip_col] < 0
            if mask_neg.any():
                logger.info(f"🌧️ {mask_neg.sum()} précipitations négatives → 0")
                df.loc[mask_neg, precip_col] = 0
            
            # Outliers extrêmes (> 500mm) -> Mise à 0
            mask_outlier = df[precip_col] > 500
            if mask_outlier.any():
                logger.warning(f"⚠️ {mask_outlier.sum()} précipitations > 500mm → 0")
                df.loc[mask_outlier, precip_col] = 0
        
        # 3. VITESSE DU VENT - Bounds [0, 150] km/h
        if "wind_speed_10m_max" in df.columns:
            wind_col = "wind_speed_10m_max"
            
            # Correction des valeurs négatives
            mask_neg = df[wind_col] < 0
            if mask_neg.any():
                logger.info(f"🌬️ {mask_neg.sum()} vitesses de vent négatives → 0")
                df.loc[mask_neg, wind_col] = 0
            
            # Plafonnement à 150 km/h (Rafales extrêmes pour région standard)
            mask_extreme = df[wind_col] > 150
            if mask_extreme.any():
                logger.warning(f"💨 {mask_extreme.sum()} rafales > 150 km/h → Plafonnement à 150")
                df.loc[mask_extreme, wind_col] = 150
        
        # 4. WEATHER CODE (WMO) - Validation + Mapping au plus proche
        if "weather_code" in df.columns:
            # Liste complète des codes WMO valides
            valid_wmo_codes = [0, 1, 2, 3, 45, 48, 51, 53, 55, 56, 57, 61, 63, 65, 66, 67, 
                               71, 73, 75, 77, 80, 81, 82, 85, 86, 95, 96, 99]
            
            def map_to_nearest_wmo(code):
                """Mappe un code invalide au code WMO valide le plus proche."""
                if pd.isna(code):
                    return code
                code = int(round(code))
                if code in valid_wmo_codes:
                    return code
                # Trouver le code valide le plus proche
                return min(valid_wmo_codes, key=lambda x: abs(x - code))
            
            # Appliquer le mapping
            df["weather_code"] = df["weather_code"].apply(map_to_nearest_wmo)
            
            # Logging des corrections
            num_mapped = df["weather_code"].notna().sum()
            logger.info(f"🌦️ Weather codes validés/mappés : {num_mapped} codes")

        # Métadonnées
        df["extraction_date"] = datetime.now().date()
        df["source"] = "openmeteo"
        
        # Sauvegarde
        table = pa.Table.from_pandas(df)
        with fs.open(dest_path, "wb") as f:
            pq.write_table(table, f)
            
        logger.info(f"Normalisation OpenMeteo FORECAST terminée : {dest_path}")

    except Exception as e:
        logger.error(f"Erreur Normalize OpenMeteo FORECAST: {e}", exc_info=True)
        raise
