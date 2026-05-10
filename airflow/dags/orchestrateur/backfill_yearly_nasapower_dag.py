# ============================================================================
# IMPORTS - Bibliotheques necessaires pour le DAG
# ============================================================================
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import time
import logging
import json
from openmeteo import normalize_openmeteo_forecast, fetch_forecast as openmeteo_fetch_forecast
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from sqlalchemy import text
import os

# Import des modules reorganises selon la nouvelle architecture
# On importe les fonctions depuis les packages (via leur __init__.py respectif)
# Cela permet de garder le code de l'orchestrateur propre et lisible.
from commun import get_fs
from loading.loading import get_pg_engine, get_ch_client, get_or_create_location

logger = logging.getLogger(__name__)

NASA_POWER_URL = "https://power.larc.nasa.gov/api/temporal/daily/point"
NASA_POWER_PARAMETERS = [
    "T2M",
    "PRECTOTCORR",
    "RH2M",
    "WS2M",
    "ALLSKY_SFC_SW_DWN",
    "CLRSKY_SFC_SW_DWN",
]
OPENMETEO_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
OPENMETEO_DAILY_PARAMETERS = [
    "temperature_2m_mean",
    "temperature_2m_max",
    "temperature_2m_min",
    "precipitation_sum",
]
OPENMETEO_HOURLY_PARAMETERS = ["surface_pressure"]
SEASONALITY_DATE_COLUMNS = {
    "daily": "date_mesure",
    "hourly": "date_mesure",
}
POSTGRES_TABLES = {
    "daily": "weather_daily",
    "hourly": "weather_hourly",
    "prediction": "weather_prediction",
}
CLICKHOUSE_TABLES = {
    "daily": "fact_weather_daily",
    "hourly": "fact_weather_hourly",
    "prediction": "fact_weather_prediction",
}
SOURCE_PRIORITY = ["openmeteo", "nasapower"]
BACKFILL_N_DAYS = int(365.25 * 30)
BACKFILL_END_DATE = datetime(2026, 1, 31)
BACKFILL_TARGET_CITY = "sousse"
PREDICTION_METRIC_COLUMNS = [
    "weather_code",
    "temperature_2m_max",
    "temperature_2m_min",
    "precipitation_sum",
    "wind_speed_10m_max",
    "soil_moisture_0_to_100cm_mean",
]
NASA_POWER_MAX_RETRIES = 5
NASA_POWER_RETRY_BASE_DELAY_SECONDS = 5
NASA_POWER_INTER_BATCH_DELAY_SECONDS = 1


# ============================================================================
# REFERENTIEL DES PLANTES (A MODIFIER MANUELLEMENT PAR L'AGRICULTEUR)
# ============================================================================
PLANTS_DATA = [
    {
        "nom_du_plante": "Tomate",
        "temp_min_opt": 20.0,
        "temp_max_opt": 27.0,
        "temp_min_abs": 7.0,
        "temp_max_abs": 35.0,
        "precipitation_min_opt": 1.64,
        "precipitation_max_opt": 3.56,
        "nature_des_sols": ["Fluvisols", "Cambisols", "Luvisols", "Calcisols"],
        "crop_cycle_min": 60,
        "crop_cycle_max": 90
    },
    {
        "nom_du_plante": "Olivier",
        "temp_min_opt": 20.0,
        "temp_max_opt": 34.0,
        "temp_min_abs": 5.0,
        "temp_max_abs": 40.0,
        "precipitation_min_opt": 1.09,
        "precipitation_max_opt": 1.92,
        "nature_des_sols": ["Calcisols", "Cambisols", "Luvisols", "Regosols"],
        "crop_cycle_min": 150,
        "crop_cycle_max": 180
    },
    {
        "nom_du_plante": "Blé",
        "temp_min_opt": 15.0,
        "temp_max_opt": 23.0,
        "temp_min_abs": 5.0,
        "temp_max_abs": 27.0,
        "precipitation_min_opt": 2.05,
        "precipitation_max_opt": 2.46,
        "nature_des_sols": ["Luvisols", "Cambisols", "Calcisols", "Vertisols"],
        "crop_cycle_min": 120,
        "crop_cycle_max": 150
    },
    {
        "nom_du_plante": "Pomme de terre",
        "temp_min_opt": 15.0,
        "temp_max_opt": 25.0,
        "temp_min_abs": 7.0,
        "temp_max_abs": 30.0,
        "precipitation_min_opt": 1.37,
        "precipitation_max_opt": 2.19,
        "nature_des_sols": ["Cambisols", "Fluvisols", "Luvisols"],
        "crop_cycle_min": 70,
        "crop_cycle_max": 120
    },
    {
        "nom_du_plante": "Oignon",
        "temp_min_opt": 12.0,
        "temp_max_opt": 25.0,
        "temp_min_abs": 4.0,
        "temp_max_abs": 30.0,
        "precipitation_min_opt": 350.0,
        "precipitation_max_opt": 550.0,
        "nature_des_sols": ["Fluvisols", "Cambisols", "Luvisols"],
        "crop_cycle_min": 100,
        "crop_cycle_max": 150
    },
    {
        "nom_du_plante": "Piment",
        "temp_min_opt": 17.0,
        "temp_max_opt": 30.0,
        "temp_min_abs": 8.0,
        "temp_max_abs": 35.0,
        "precipitation_min_opt": 1.64,
        "precipitation_max_opt": 3.42,
        "nature_des_sols": ["Fluvisols", "Cambisols", "Luvisols", "Calcisols"],
        "crop_cycle_min": 90,
        "crop_cycle_max": 120
    },
    {
        "nom_du_plante": "Orange",
        "temp_min_opt": 20.0,
        "temp_max_opt": 30.0,
        "temp_min_abs": 13.0,
        "temp_max_abs": 38.0,
        "precipitation_min_opt": 3.28,
        "precipitation_max_opt": 5.48,
        "nature_des_sols": ["Fluvisols", "Luvisols", "Cambisols"],
        "crop_cycle_min": 240,
        "crop_cycle_max": 300
    },
    {
        "nom_du_plante": "Vigne",
        "temp_min_opt": 18.0,
        "temp_max_opt": 28.0,
        "temp_min_abs": 9.0,
        "temp_max_abs": 32.0,
        "precipitation_min_opt": 3.29,
        "precipitation_max_opt": 3.83,
        "nature_des_sols": ["Calcisols", "Luvisols", "Cambisols"],
        "crop_cycle_min": 150,
        "crop_cycle_max": 210
    },
    {
        "nom_du_plante": "pastèque",
        "temp_min_opt": 20.0,
        "temp_max_opt": 35.0,
        "temp_min_abs": 15.0,
        "temp_max_abs": 40.0,
        "precipitation_min_opt": 1.37,
        "precipitation_max_opt": 1.92,
        "nature_des_sols": ["Regosols", "Fluvisols", "Calcisols"],
        "crop_cycle_min": 70,
        "crop_cycle_max": 90
    }
]


def load_plants_requirements(**context):
    """
    Cree la table plant_requirements si elle n'existe pas et insere
    les donnees manuellement modifiees par l'agriculteur dans PostgreSQL.
    """
    from loading import get_pg_engine
    from sqlalchemy import text

    engine = get_pg_engine()

    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS plant_requirements (
                nom_du_plante VARCHAR(255) PRIMARY KEY,
                temp_min_opt FLOAT,
                temp_max_opt FLOAT,
                temp_min_abs FLOAT,
                temp_max_abs FLOAT,
                precipitation_min_opt FLOAT,
                precipitation_max_opt FLOAT,
                nature_des_sols TEXT[],
                crop_cycle_min INT,
                crop_cycle_max INT
            );
        """))

        # Migration : ajouter les colonnes si la table existe deja sans elles
        conn.execute(text("""
            ALTER TABLE plant_requirements
            ADD COLUMN IF NOT EXISTS crop_cycle_min INT,
            ADD COLUMN IF NOT EXISTS crop_cycle_max INT;
        """))

        for plant in PLANTS_DATA:
            conn.execute(text("""
                INSERT INTO plant_requirements
                (nom_du_plante, temp_min_opt, temp_max_opt, temp_min_abs, temp_max_abs, precipitation_min_opt, precipitation_max_opt, nature_des_sols, crop_cycle_min, crop_cycle_max)
                VALUES
                (:nom, :t_min, :t_max, :t_min_abs, :t_max_abs, :p_min, :p_max, :n_sols, :cc_min, :cc_max)
                ON CONFLICT (nom_du_plante) DO UPDATE SET
                    crop_cycle_min = EXCLUDED.crop_cycle_min,
                    crop_cycle_max = EXCLUDED.crop_cycle_max;
            """), {
                "nom": plant["nom_du_plante"],
                "t_min": plant["temp_min_opt"],
                "t_max": plant["temp_max_opt"],
                "t_min_abs": plant["temp_min_abs"],
                "t_max_abs": plant["temp_max_abs"],
                "p_min": plant["precipitation_min_opt"],
                "p_max": plant["precipitation_max_opt"],
                "n_sols": plant["nature_des_sols"],
                "cc_min": plant["crop_cycle_min"],
                "cc_max": plant["crop_cycle_max"]
            })

    print(f"{len(PLANTS_DATA)} plantes inserees/mises a jour dans la table plant_requirements.")


# ============================================================================
# OUTILS NASA POWER - APPELS BATCHES PAR ANNEE
# ============================================================================
def group_dates_by_year(target_dates):
    """
    Regroupe les dates raw NASA POWER par annee.

    Le backfill journalier garde la regle historique existante : chaque contexte
    Airflow cible les donnees J-2. Cette fonction recoit donc directement les
    vraies dates raw a extraire et cree un appel API par annee concernee.
    """
    dates = sorted({d.date() if hasattr(d, "date") else d for d in target_dates})
    batches = []

    for year in sorted({d.year for d in dates}):
        year_dates = [d for d in dates if d.year == year]
        batches.append({
            "year": year,
            "start_date": year_dates[0],
            "end_date": year_dates[-1],
        })

    return batches


def write_yearly_parquet(df, fs, layer, seasonality, source, year):
    """
    Ecrit un dataframe annuel dans le layout :
    {layer}/{seasonality}/{source}/year=YYYY/data.parquet
    """
    path = f"{layer}/{seasonality}/{source}/year={year}/data.parquet"
    table = pa.Table.from_pandas(df)

    with fs.open(path, "wb") as f:
        pq.write_table(table, f)

    print(f"{source} {seasonality} {layer} sauvegarde : {path} ({len(df)} lignes)")


def read_yearly_parquet(fs, layer, seasonality, source, year):
    """
    Lit un dataframe annuel depuis MinIO.
    """
    path = f"{layer}/{seasonality}/{source}/year={year}/data.parquet"

    if not fs.exists(path):
        raise FileNotFoundError(f"Fichier annuel introuvable : {path}")

    with fs.open(path, "rb") as f:
        return pd.read_parquet(f)


def fetch_nasapower_by_year(latitude, longitude, target_dates):
    """
    Extrait NASA POWER en un appel API par annee, puis ecrit raw/year.

    Ancien comportement couteux :
        1 appel API NASA POWER par jour.

    Nouveau comportement :
        1 appel API NASA POWER par annee concernee par le backfill.
        Les metriques deja fournies par OpenMeteo ne sont pas demandees.
    """
    batches = group_dates_by_year(target_dates)
    fs = get_fs()
    total_days = 0
    total_records = 0

    print(f"Demarrage extraction NASA Power batchee : {len(batches)} appel(s) API annuel(s).")

    for batch_index, batch in enumerate(batches):
        start_str = batch["start_date"].strftime("%Y%m%d")
        end_str = batch["end_date"].strftime("%Y%m%d")

        print(f"NASA Power annee {batch['year']} : {start_str} -> {end_str}")

        params = {
            "start": start_str,
            "end": end_str,
            "latitude": latitude,
            "longitude": longitude,
            "community": "RE",
            "parameters": ",".join(NASA_POWER_PARAMETERS),
            "format": "JSON",
            "header": "true",
        }

        payload = None
        last_error = None
        for attempt in range(1, NASA_POWER_MAX_RETRIES + 1):
            try:
                response = requests.get(NASA_POWER_URL, params=params, timeout=300)
                response.raise_for_status()
                payload = response.json()
                break
            except requests.RequestException as exc:
                last_error = exc
                if attempt == NASA_POWER_MAX_RETRIES:
                    break

                wait_seconds = NASA_POWER_RETRY_BASE_DELAY_SECONDS * (2 ** (attempt - 1))
                print(
                    f"NASA Power annee {batch['year']} : tentative {attempt}/{NASA_POWER_MAX_RETRIES} "
                    f"echouee ({exc}). Nouvelle tentative dans {wait_seconds} seconde(s)."
                )
                time.sleep(wait_seconds)

        if payload is None:
            raise RuntimeError(
                f"Echec NASA POWER apres {NASA_POWER_MAX_RETRIES} tentative(s) pour l'annee {batch['year']}"
            ) from last_error

        try:
            params_data = payload["properties"]["parameter"]
        except KeyError as exc:
            raise ValueError("Structure JSON invalide de l'API NasaPower") from exc

        expected_dates = pd.date_range(
            start=batch["start_date"],
            end=batch["end_date"],
            freq="D",
        ).strftime("%Y%m%d").tolist()
        expected_date_set = set(expected_dates)

        missing_parameters = set(NASA_POWER_PARAMETERS) - set(params_data)
        if missing_parameters:
            raise ValueError(f"Parametres NASA POWER manquants : {sorted(missing_parameters)}")

        records = []
        for parameter in NASA_POWER_PARAMETERS:
            returned_dates = set(params_data[parameter])
            missing_dates = expected_date_set - returned_dates
            if missing_dates:
                raise ValueError(
                    f"Dates NASA POWER manquantes pour {parameter}: {sorted(missing_dates)}"
                )

            for date_str, value in params_data[parameter].items():
                if date_str in expected_date_set:
                    records.append({
                        "date": datetime.strptime(date_str, "%Y%m%d"),
                        "parameter": parameter,
                        "value": value,
                    })

        records_df = pd.DataFrame(records).drop_duplicates(subset=["date", "parameter"])
        unique_dates = records_df["date"].dt.strftime("%Y%m%d").sort_values().unique().tolist()
        expected_record_count = len(expected_dates) * len(NASA_POWER_PARAMETERS)

        if unique_dates != expected_dates:
            raise ValueError(f"Dates NASA POWER inattendues pour {batch['year']}: {unique_dates}")

        if len(records_df) != expected_record_count:
            raise ValueError(
                f"Nombre d'enregistrements NASA POWER invalide pour {batch['year']}: "
                f"{len(records_df)} au lieu de {expected_record_count}"
            )

        write_yearly_parquet(records_df, fs, "raw", "daily", "nasapower", batch["year"])

        total_days += len(expected_dates)
        total_records += len(records_df)

        if batch_index < len(batches) - 1:
            time.sleep(NASA_POWER_INTER_BATCH_DELAY_SECONDS)

    print(
        f"Extraction NASA Power terminee : {len(batches)} appel(s) API, "
        f"{total_days} jour(s), {total_records} enregistrement(s)."
    )
    time.sleep(10)  # Attente de 10 secondes pour eviter les problemes de throttling


# ============================================================================
# OUTILS OPENMETEO - APPELS BATCHES PAR ANNEE
# ============================================================================
def _fetch_openmeteo_history_by_year(latitude, longitude, target_dates, include_daily, include_hourly):
    """
    Extrait OpenMeteo Archive en un appel API par annee, puis ecrit raw/year
    pour les seasonalites demandees.
    """
    if not include_daily and not include_hourly:
        raise ValueError("Au moins une saisonnalite OpenMeteo doit etre demandee.")

    batches = group_dates_by_year(target_dates)
    fs = get_fs()
    total_days = 0
    total_hourly_records = 0
    total_daily_records = 0

    requested_seasonalities = []
    if include_daily:
        requested_seasonalities.append("daily")
    if include_hourly:
        requested_seasonalities.append("hourly")

    print(
        "Demarrage extraction OpenMeteo batchee "
        f"({', '.join(requested_seasonalities)}) : {len(batches)} appel(s) API annuel(s)."
    )

    for batch in batches:
        start_str = batch["start_date"].strftime("%Y-%m-%d")
        end_str = batch["end_date"].strftime("%Y-%m-%d")

        print(f"OpenMeteo annee {batch['year']} : {start_str} -> {end_str}")

        params = {
            "latitude": latitude,
            "longitude": longitude,
            "start_date": start_str,
            "end_date": end_str,
            "timezone": "GMT",
        }
        if include_daily:
            params["daily"] = ",".join(OPENMETEO_DAILY_PARAMETERS)
        if include_hourly:
            params["hourly"] = ",".join(OPENMETEO_HOURLY_PARAMETERS)

        response = requests.get(OPENMETEO_ARCHIVE_URL, params=params, timeout=300)
        response.raise_for_status()
        payload = response.json()

        try:
            daily_payload = payload["daily"] if include_daily else None
            hourly_payload = payload["hourly"] if include_hourly else None
        except KeyError as exc:
            raise ValueError("Structure JSON invalide de l'API OpenMeteo Archive") from exc

        expected_dates = pd.date_range(
            start=batch["start_date"],
            end=batch["end_date"],
            freq="D",
        ).strftime("%Y-%m-%d").tolist()

        if include_daily:
            daily_df = pd.DataFrame({
                "date": pd.to_datetime(daily_payload["time"], utc=True),
            })
            for parameter in OPENMETEO_DAILY_PARAMETERS:
                if parameter not in daily_payload:
                    raise ValueError(f"Parametre OpenMeteo daily manquant : {parameter}")
                daily_df[parameter] = daily_payload[parameter]

            daily_dates = daily_df["date"].dt.strftime("%Y-%m-%d").sort_values().unique().tolist()
            if daily_dates != expected_dates:
                raise ValueError(f"Dates daily OpenMeteo inattendues pour {batch['year']}: {daily_dates}")

            expected_daily_records = len(expected_dates)
            if len(daily_df) != expected_daily_records:
                raise ValueError(
                    f"Nombre d'enregistrements daily OpenMeteo invalide pour {batch['year']}: "
                    f"{len(daily_df)} au lieu de {expected_daily_records}"
                )

            write_yearly_parquet(daily_df, fs, "raw", "daily", "openmeteo", batch["year"])
            total_daily_records += len(daily_df)

        if include_hourly:
            hourly_df = pd.DataFrame({
                "date": pd.to_datetime(hourly_payload["time"], utc=True),
            })
            for parameter in OPENMETEO_HOURLY_PARAMETERS:
                if parameter not in hourly_payload:
                    raise ValueError(f"Parametre OpenMeteo hourly manquant : {parameter}")
                hourly_df[parameter] = hourly_payload[parameter]

            hourly_dates = hourly_df["date"].dt.strftime("%Y-%m-%d").sort_values().unique().tolist()
            if hourly_dates != expected_dates:
                raise ValueError(f"Dates hourly OpenMeteo inattendues pour {batch['year']}: {hourly_dates}")

            expected_hourly_records = len(expected_dates) * 24
            if len(hourly_df) != expected_hourly_records:
                raise ValueError(
                    f"Nombre d'enregistrements hourly OpenMeteo invalide pour {batch['year']}: "
                    f"{len(hourly_df)} au lieu de {expected_hourly_records}"
                )

            write_yearly_parquet(hourly_df, fs, "raw", "hourly", "openmeteo", batch["year"])
            total_hourly_records += len(hourly_df)

        total_days += len(expected_dates)

    print(
        "Extraction OpenMeteo terminee : "
        f"{len(batches)} appel(s) API, {total_days} jour(s), "
        f"{total_daily_records} daily, {total_hourly_records} hourly."
    )


def fetch_openmeteo_daily_by_year(latitude, longitude, target_dates):
    """
    Extrait uniquement OpenMeteo daily vers raw/daily/openmeteo/year=YYYY.
    """
    _fetch_openmeteo_history_by_year(
        latitude,
        longitude,
        target_dates,
        include_daily=True,
        include_hourly=False,
    )


def fetch_openmeteo_hourly_by_year(latitude, longitude, target_dates):
    """
    Extrait uniquement OpenMeteo hourly vers raw/hourly/openmeteo/year=YYYY.
    """
    _fetch_openmeteo_history_by_year(
        latitude,
        longitude,
        target_dates,
        include_daily=False,
        include_hourly=True,
    )


def fetch_openmeteo_by_year(latitude, longitude, target_dates):
    """
    Compatibilite historique : extrait daily et hourly.
    """
    _fetch_openmeteo_history_by_year(
        latitude,
        longitude,
        target_dates,
        include_daily=True,
        include_hourly=True,
    )
    time.sleep(10)  # Attente de 10 secondes pour eviter les problemes de throttling



# ============================================================================
# NORMALISATION ANNUELLE
# ============================================================================
def normalize_openmeteo_daily_year(raw_df):
    """
    Normalise une annee OpenMeteo daily complete.
    """
    df = raw_df.copy()
    df["date"] = pd.to_datetime(df["date"], utc=True)

    cols_numeric = [
        "temperature_2m_mean",
        "temperature_2m_max",
        "temperature_2m_min",
        "precipitation_sum",
    ]
    for col in cols_numeric:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    has_temps = all(col in df.columns for col in [
        "temperature_2m_min",
        "temperature_2m_mean",
        "temperature_2m_max",
    ])

    if has_temps:
        mask_swap = df["temperature_2m_min"] > df["temperature_2m_max"]
        df.loc[mask_swap, ["temperature_2m_min", "temperature_2m_max"]] = (
            df.loc[mask_swap, ["temperature_2m_max", "temperature_2m_min"]].values
        )

        mask_mean_invalid = (
            (df["temperature_2m_mean"] < df["temperature_2m_min"]) |
            (df["temperature_2m_mean"] > df["temperature_2m_max"])
        )
        df.loc[mask_mean_invalid, "temperature_2m_mean"] = (
            df.loc[mask_mean_invalid, "temperature_2m_min"] +
            df.loc[mask_mean_invalid, "temperature_2m_max"]
        ) / 2

    for temp_col in ["temperature_2m_mean", "temperature_2m_max", "temperature_2m_min"]:
        if temp_col in df.columns:
            mask_out_of_bounds = ~df[temp_col].between(-50, 60)
            df.loc[mask_out_of_bounds, temp_col] = pd.NA
            df[temp_col] = df[temp_col].interpolate(method="linear", limit_direction="both")
            df[temp_col] = df[temp_col].ffill().bfill()

    if "precipitation_sum" in df.columns:
        df.loc[df["precipitation_sum"] < 0, "precipitation_sum"] = 0
        df.loc[df["precipitation_sum"] > 500, "precipitation_sum"] = 0

    df["extraction_date"] = datetime.now().date()
    df["source"] = "openmeteo"
    return df.sort_values("date").reset_index(drop=True)


def normalize_openmeteo_hourly_year(raw_df):
    """
    Normalise une annee OpenMeteo hourly complete.
    """
    df = raw_df.copy()
    df["date"] = pd.to_datetime(df["date"], utc=True)

    if "surface_pressure" in df.columns:
        df["surface_pressure"] = pd.to_numeric(df["surface_pressure"], errors="coerce")
        mask_out_of_bounds = ~df["surface_pressure"].between(870, 1090)
        df.loc[mask_out_of_bounds, "surface_pressure"] = pd.NA
        df["surface_pressure"] = df["surface_pressure"].ffill()

    df["extraction_date"] = datetime.now().date()
    df["source"] = "openmeteo"
    return df.sort_values("date").reset_index(drop=True)


def normalize_nasapower_daily_year(raw_df):
    """
    Normalise une annee NASA POWER daily complete.
    """
    df = raw_df.copy()
    df["date"] = pd.to_datetime(df["date"])
    df = df.drop_duplicates(subset=["date", "parameter"])

    df_pivot = df.pivot(index="date", columns="parameter", values="value").reset_index()
    column_mapping = {
        "T2M": "temperature_2m_mean",
        "PRECTOTCORR": "precipitation_sum",
        "RH2M": "relative_humidity_2m",
        "WS2M": "wind_speed_2m",
        "ALLSKY_SFC_SW_DWN": "all_sky_surface_shortwave_radiation_downward",
        "CLRSKY_SFC_SW_DWN": "clear_sky_surface_shortwave_radiation_downward",
    }
    df_pivot = df_pivot.rename(columns=column_mapping)
    df_pivot = df_pivot.replace(-999, pd.NA)

    cols_to_numeric = [
        "temperature_2m_mean",
        "precipitation_sum",
        "relative_humidity_2m",
        "wind_speed_2m",
        "all_sky_surface_shortwave_radiation_downward",
        "clear_sky_surface_shortwave_radiation_downward",
    ]
    for col in cols_to_numeric:
        if col in df_pivot.columns:
            df_pivot[col] = pd.to_numeric(df_pivot[col], errors="coerce")

    if "temperature_2m_mean" in df_pivot.columns:
        mask_out_of_bounds = ~df_pivot["temperature_2m_mean"].between(-50, 60)
        df_pivot.loc[mask_out_of_bounds, "temperature_2m_mean"] = pd.NA
        df_pivot["temperature_2m_mean"] = df_pivot["temperature_2m_mean"].interpolate(
            method="linear",
            limit_direction="both",
        )
        df_pivot["temperature_2m_mean"] = df_pivot["temperature_2m_mean"].ffill().bfill()

    if "precipitation_sum" in df_pivot.columns:
        df_pivot.loc[df_pivot["precipitation_sum"] < 0, "precipitation_sum"] = 0
        df_pivot.loc[df_pivot["precipitation_sum"] > 500, "precipitation_sum"] = 0

    all_sky = "all_sky_surface_shortwave_radiation_downward"
    clear_sky = "clear_sky_surface_shortwave_radiation_downward"
    if all_sky in df_pivot.columns and clear_sky in df_pivot.columns:
        mask_invalid = df_pivot[all_sky] > df_pivot[clear_sky]
        df_pivot.loc[mask_invalid, all_sky] = df_pivot.loc[mask_invalid, clear_sky]

    if "relative_humidity_2m" in df_pivot.columns:
        df_pivot["relative_humidity_2m"] = df_pivot["relative_humidity_2m"].clip(lower=0, upper=100)

    for col in ["wind_speed_2m", all_sky, clear_sky]:
        if col in df_pivot.columns:
            df_pivot.loc[df_pivot[col] < 0, col] = 0

    df_pivot["extraction_date"] = datetime.now().date()
    df_pivot["source"] = "nasapower"
    return df_pivot.sort_values("date").reset_index(drop=True)


def normalize_yearly_datasets(years):
    """
    Transforme tous les fichiers raw/year en normalized/year.
    """
    fs = get_fs()

    for year in years:
        print(f"Normalisation annuelle {year}...")

        openmeteo_daily = read_yearly_parquet(fs, "raw", "daily", "openmeteo", year)
        openmeteo_hourly = read_yearly_parquet(fs, "raw", "hourly", "openmeteo", year)
        nasapower_daily = read_yearly_parquet(fs, "raw", "daily", "nasapower", year)

        write_yearly_parquet(
            normalize_openmeteo_daily_year(openmeteo_daily),
            fs,
            "normalized",
            "daily",
            "openmeteo",
            year,
        )
        write_yearly_parquet(
            normalize_openmeteo_hourly_year(openmeteo_hourly),
            fs,
            "normalized",
            "hourly",
            "openmeteo",
            year,
        )
        write_yearly_parquet(
            normalize_nasapower_daily_year(nasapower_daily),
            fs,
            "normalized",
            "daily",
            "nasapower",
            year,
        )


def normalize_historical_daily_years(years):
    """
    Normalise uniquement le flux historique daily OpenMeteo.
    """
    fs = get_fs()

    for year in years:
        print(f"Normalisation historique daily {year}...")
        openmeteo_daily = read_yearly_parquet(fs, "raw", "daily", "openmeteo", year)
        write_yearly_parquet(
            normalize_openmeteo_daily_year(openmeteo_daily),
            fs,
            "normalized",
            "daily",
            "openmeteo",
            year,
        )


def normalize_historical_hourly_years(years):
    """
    Normalise le flux historique hourly :
      - OpenMeteo hourly
      - NASA POWER daily (diffuse ensuite sur l'hourly final)
    """
    fs = get_fs()

    for year in years:
        print(f"Normalisation historique hourly {year}...")
        openmeteo_hourly = read_yearly_parquet(fs, "raw", "hourly", "openmeteo", year)
        nasapower_daily = read_yearly_parquet(fs, "raw", "daily", "nasapower", year)

        write_yearly_parquet(
            normalize_openmeteo_hourly_year(openmeteo_hourly),
            fs,
            "normalized",
            "hourly",
            "openmeteo",
            year,
        )
        write_yearly_parquet(
            normalize_nasapower_daily_year(nasapower_daily),
            fs,
            "normalized",
            "daily",
            "nasapower",
            year,
        )


# ============================================================================
# DATAFRAMES GEANTS, MERGE PAR SAISONNALITE, CHARGEMENT DB
# ============================================================================
def load_source_seasonality_dataframes(years):
    """
    Charge tous les normalized/year en dataframes geants, un par couple
    (source, seasonality).
    """
    fs = get_fs()
    pair_definitions = [
        ("openmeteo", "daily"),
        ("openmeteo", "hourly"),
        ("nasapower", "daily"),
    ]
    pair_dfs = {}

    for source, seasonality in pair_definitions:
        frames = []

        for year in years:
            frames.append(read_yearly_parquet(fs, "normalized", seasonality, source, year))

        df = pd.concat(frames, ignore_index=True)
        df["date"] = pd.to_datetime(df["date"], utc=True)

        if seasonality == "daily":
            df["date"] = df["date"].dt.floor("D")

        df = df.sort_values("date").reset_index(drop=True)
        pair_dfs[(source, seasonality)] = df

        print(f"Dataframe geant ({source}, {seasonality}) : {len(df)} lignes")

    return pair_dfs


def load_normalized_source_dataframe(years, source, seasonality):
    """
    Charge un dataframe geant pour un unique couple (source, seasonality).
    """
    fs = get_fs()
    frames = []

    for year in years:
        frames.append(read_yearly_parquet(fs, "normalized", seasonality, source, year))

    df = pd.concat(frames, ignore_index=True)
    df["date"] = pd.to_datetime(df["date"], utc=True)

    if seasonality == "daily":
        df["date"] = df["date"].dt.floor("D")

    df = df.sort_values("date").reset_index(drop=True)
    print(f"Dataframe geant ({source}, {seasonality}) : {len(df)} lignes")
    return df


def prepare_seasonality_frame(df, source, existing_metric_cols):
    """
    Prepare un dataframe pour une saisonnalite donnee.

    Les metriques gardent leur nom canonique. Si une metrique existe deja
    depuis une source prioritaire, elle est ignoree dans la source courante.
    """
    metadata_cols = {"date", "source", "extraction_date"}
    metric_cols = [col for col in df.columns if col not in metadata_cols]
    collision_cols = sorted(set(metric_cols) & existing_metric_cols)

    if collision_cols:
        print(
            f"Metriques ignorees depuis {source} car deja presentes "
            f"depuis une source prioritaire: {collision_cols}"
        )
        metric_cols = [col for col in metric_cols if col not in collision_cols]

    prepared = df[["date", "extraction_date"] + metric_cols].copy()
    prepared[f"{source}_extraction_date"] = pd.to_datetime(
        prepared.pop("extraction_date"),
        errors="coerce",
    )
    return prepared, set(metric_cols)


def finalize_single_source_dataframe(df, source):
    """
    Finalise un dataframe d'une seule source pour un chargement direct.
    """
    prepared, _ = prepare_seasonality_frame(df, source, set())
    extraction_cols = [col for col in prepared.columns if col.endswith("_extraction_date")]
    prepared["extraction_date"] = prepared[extraction_cols].max(axis=1)
    prepared = prepared.drop(columns=extraction_cols)
    return prepared.sort_values("date").reset_index(drop=True)


def merge_nasapower_daily_into_hourly(openmeteo_hourly_df, nasapower_daily_df):
    """
    Diffuse les metriques NASA POWER journalieres sur chaque ligne horaire du jour.
    """
    hourly_df = openmeteo_hourly_df.copy()
    hourly_df["join_day"] = hourly_df["date"].dt.floor("D")

    hourly_metric_cols = [
        col for col in hourly_df.columns
        if col not in {"date", "join_day", "source", "extraction_date"}
    ]
    hourly_prepared = hourly_df[["date", "join_day"] + hourly_metric_cols].copy()
    hourly_prepared["openmeteo_extraction_date"] = pd.to_datetime(
        hourly_df["extraction_date"],
        errors="coerce",
    )

    nasa_df = nasapower_daily_df.copy()
    nasa_df["join_day"] = nasa_df["date"].dt.floor("D")
    nasa_metric_cols = [
        col for col in nasa_df.columns
        if col not in {"date", "join_day", "source", "extraction_date"}
    ]
    nasa_prepared = nasa_df[["join_day"] + nasa_metric_cols].copy()
    nasa_prepared["nasapower_extraction_date"] = pd.to_datetime(
        nasa_df["extraction_date"],
        errors="coerce",
    )

    merged_df = hourly_prepared.merge(nasa_prepared, on="join_day", how="left")
    extraction_cols = [col for col in merged_df.columns if col.endswith("_extraction_date")]
    merged_df["extraction_date"] = merged_df[extraction_cols].max(axis=1)
    merged_df = merged_df.drop(columns=extraction_cols + ["join_day"])
    return merged_df.sort_values("date").reset_index(drop=True)


def build_dataframes_by_seasonality(pair_dfs):
    """
    Produit un dataframe final par saisonnalite.

    weather_daily :
        - OpenMeteo daily uniquement

    weather_hourly :
        - OpenMeteo hourly
        - NASA POWER daily diffuse sur chaque heure du jour
    """
    openmeteo_daily_df = pair_dfs[("openmeteo", "daily")]
    openmeteo_hourly_df = pair_dfs[("openmeteo", "hourly")]
    nasapower_daily_df = pair_dfs[("nasapower", "daily")]

    daily_df = finalize_single_source_dataframe(openmeteo_daily_df, "openmeteo")
    hourly_df = merge_nasapower_daily_into_hourly(openmeteo_hourly_df, nasapower_daily_df)

    print(f"Dataframe final (daily) : {len(daily_df)} lignes")
    print(f"Dataframe final (hourly) : {len(hourly_df)} lignes")

    return {
        "daily": daily_df,
        "hourly": hourly_df,
    }


def remove_timezone(series):
    """
    Convertit une serie datetime vers un format compatible PostgreSQL/ClickHouse.
    """
    series = pd.to_datetime(series, errors="coerce")
    if getattr(series.dt, "tz", None) is not None:
        series = series.dt.tz_convert(None)
    return series


def json_safe_value(value):
    """
    Convertit les valeurs pandas/numpy en valeurs JSON simples.
    """
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if hasattr(value, "item"):
        return value.item()
    return value


def load_seasonality_dataframe_to_postgres(df, seasonality, latitude, longitude, city_name):
    """
    Charge un dataframe final par saisonnalite dans PostgreSQL.
    """
    table_name = POSTGRES_TABLES[seasonality]
    date_col = SEASONALITY_DATE_COLUMNS[seasonality]
    df_pg = df.copy()
    df_pg[date_col] = remove_timezone(df_pg.pop("date"))
    df_pg["extraction_date"] = remove_timezone(df_pg["extraction_date"])

    metric_cols = [
        col for col in df_pg.columns
        if col not in {date_col, "extraction_date"}
    ]

    engine = get_pg_engine()
    location_id = get_or_create_location(engine, city_name, latitude, longitude)

    with engine.begin() as conn:
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id BIGSERIAL PRIMARY KEY,
                location_id INT REFERENCES location(id),
                {date_col} TIMESTAMP NOT NULL,
                extraction_date TIMESTAMP NOT NULL,
                metrics JSONB NOT NULL,
                UNIQUE(location_id, {date_col})
            );
        """))

        records = []
        for _, row in df_pg.iterrows():
            metrics = {
                col: json_safe_value(row[col])
                for col in metric_cols
                if json_safe_value(row[col]) is not None
            }
            records.append({
                "location_id": location_id,
                "date_val": row[date_col],
                "extraction_date": row["extraction_date"],
                "metrics": json.dumps(metrics),
            })

        if records:
            conn.execute(text(f"""
                INSERT INTO {table_name} (location_id, {date_col}, extraction_date, metrics)
                VALUES (:location_id, :date_val, :extraction_date, CAST(:metrics AS JSONB))
                ON CONFLICT (location_id, {date_col})
                DO UPDATE SET
                    extraction_date = EXCLUDED.extraction_date,
                    metrics = EXCLUDED.metrics;
            """), records)

    print(f"PostgreSQL charge : {table_name} ({len(df_pg)} lignes)")


def load_seasonality_dataframe_to_clickhouse(df, seasonality, latitude, longitude, city_name):
    """
    Charge un dataframe final par saisonnalite dans ClickHouse.
    """
    table_name = CLICKHOUSE_TABLES[seasonality]
    date_col = SEASONALITY_DATE_COLUMNS[seasonality]
    df_ch = df.copy()
    df_ch[date_col] = remove_timezone(df_ch.pop("date"))
    df_ch["extraction_date"] = remove_timezone(df_ch["extraction_date"])
    df_ch["city_name"] = str(city_name)
    df_ch["latitude"] = float(latitude)
    df_ch["longitude"] = float(longitude)

    metric_cols = [
        col for col in df_ch.columns
        if col not in {date_col, "extraction_date", "city_name", "latitude", "longitude"}
    ]
    for col in metric_cols:
        df_ch[col] = pd.to_numeric(df_ch[col], errors="coerce")

    columns = [date_col, "extraction_date", "city_name", "latitude", "longitude"] + metric_cols
    df_ch = df_ch[columns].copy()

    if seasonality == "daily":
        date_type = "Date"
        df_ch[date_col] = df_ch[date_col].dt.date
        partition_sql = f"\nPARTITION BY toYear({date_col})"
    else:
        date_type = "DateTime"
        partition_sql = f"\nPARTITION BY toYYYYMM({date_col})"

    metric_sql = ",\n    ".join(f"{col} Nullable(Float64)" for col in metric_cols)
    schema_sql = f"""
CREATE TABLE IF NOT EXISTS {table_name} (
    {date_col} {date_type},
    extraction_date DateTime,
    city_name String,
    latitude Float64,
    longitude Float64{',' if metric_sql else ''}
    {metric_sql}
) ENGINE = MergeTree(){partition_sql}
ORDER BY (city_name, {date_col});
"""

    client = get_ch_client()
    client.command(schema_sql)
    for col in metric_cols:
        client.command(
            f"ALTER TABLE {table_name} "
            f"ADD COLUMN IF NOT EXISTS {col} Nullable(Float64)"
        )
    client.insert_df(
        table=table_name,
        df=df_ch,
        settings={"max_partitions_per_insert_block": 0},
    )

    print(f"ClickHouse charge : {table_name} ({len(df_ch)} lignes)")


def load_dataframes_by_seasonality(dataframes_by_seasonality, latitude, longitude, city_name):
    """
    Charge tous les dataframes finaux dans PostgreSQL puis ClickHouse.
    """
    for seasonality, df in dataframes_by_seasonality.items():
        load_seasonality_dataframe_to_postgres(df, seasonality, latitude, longitude, city_name)
        load_seasonality_dataframe_to_clickhouse(df, seasonality, latitude, longitude, city_name)


# ============================================================================
# TABLE FINALE PREDICTION OPENMETEO
# ============================================================================
def prediction_bucket_path(layer, execution_date):
    """
    Construit le chemin bucket pour une prediction OpenMeteo.
    """
    execution_dt = pd.to_datetime(execution_date).to_pydatetime()
    year = execution_dt.year
    month = f"{execution_dt.month:02d}"
    day = f"{execution_dt.day:02d}"
    return f"{layer}/prediction/openmeteo/year={year}/month={month}/day={day}/data.parquet"


def validate_prediction_metrics(df, stage_name, path=None):
    """
    Verifie que le dataframe prediction contient de vraies metriques.
    """
    if df.empty:
        raise ValueError(f"{stage_name} ne contient aucune ligne.")

    missing_columns = sorted(set(PREDICTION_METRIC_COLUMNS) - set(df.columns))
    if missing_columns:
        raise ValueError(f"{stage_name} colonnes prediction manquantes : {missing_columns}")

    metric_frame = df[PREDICTION_METRIC_COLUMNS]
    non_null_counts = metric_frame.notna().sum().to_dict()
    empty_metric_rows = int((metric_frame.notna().sum(axis=1) == 0).sum())
    path_label = f" ({path})" if path else ""

    print(
        f"{stage_name}{path_label}: {len(df)} ligne(s), "
        f"valeurs metriques non nulles {non_null_counts}"
    )

    if empty_metric_rows:
        raise ValueError(
            f"{stage_name} contient {empty_metric_rows}/{len(df)} ligne(s) "
            "sans aucune metrique. Refus de charger metrics={}"
        )


def read_prediction_bucket_dataframe(layer, execution_date):
    """
    Lit un dataframe OpenMeteo prediction depuis raw/ ou normalized/.
    """
    path = prediction_bucket_path(layer, execution_date)
    fs = get_fs()

    if not fs.exists(path):
        raise FileNotFoundError(f"Fichier prediction OpenMeteo introuvable : {path}")

    with fs.open(path, "rb") as f:
        df = pd.read_parquet(f)

    if df.empty:
        raise ValueError(f"Fichier prediction OpenMeteo vide : {path}")

    return df, path


def read_openmeteo_prediction_dataframe(execution_date):
    """
    Lit le fichier OpenMeteo prediction normalise pour la date d'execution.
    """
    df, path = read_prediction_bucket_dataframe("normalized", execution_date)

    if "date" not in df.columns:
        raise ValueError(f"Colonne date manquante dans la prediction normalisee : {path}")

    validate_prediction_metrics(df, "Prediction normalisee", path)

    if "extraction_date" not in df.columns:
        df["extraction_date"] = datetime.now()
    if "source" not in df.columns:
        df["source"] = "openmeteo"

    df["date"] = pd.to_datetime(df["date"], utc=True)
    df["extraction_date"] = pd.to_datetime(df["extraction_date"], errors="coerce")
    df["source"] = "openmeteo"
    return df.sort_values("date").reset_index(drop=True)


def ensure_weather_prediction_pg_schema(conn, table_name):
    """
    Cree ou migre weather_prediction vers la contrainte finale attendue.
    """
    conn.execute(text(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id BIGSERIAL PRIMARY KEY,
            location_id INT REFERENCES location(id),
            prediction_target_date TIMESTAMP NOT NULL,
            extraction_date TIMESTAMP NOT NULL,
            metrics JSONB NOT NULL,
            CONSTRAINT weather_prediction_location_target_extraction_key
                UNIQUE(location_id, prediction_target_date, extraction_date)
        );
    """))

    conn.execute(text(f"""
        ALTER TABLE {table_name}
        DROP CONSTRAINT IF EXISTS weather_prediction_location_source_target_extraction_key;
    """))
    conn.execute(text(f"""
        ALTER TABLE {table_name}
        DROP COLUMN IF EXISTS source_id;
    """))

    conn.execute(text(f"""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1
                FROM pg_constraint con
                JOIN pg_class rel ON rel.oid = con.conrelid
                WHERE rel.relname = '{table_name}'
                  AND con.contype = 'u'
                  AND (
                      SELECT array_agg(att.attname::text ORDER BY cols.ordinality)
                      FROM unnest(con.conkey) WITH ORDINALITY AS cols(attnum, ordinality)
                      JOIN pg_attribute att
                        ON att.attrelid = con.conrelid
                       AND att.attnum = cols.attnum
                  ) = ARRAY[
                      'location_id',
                      'prediction_target_date',
                      'extraction_date'
                  ]
            ) THEN
                ALTER TABLE {table_name}
                ADD CONSTRAINT weather_prediction_location_target_extraction_key
                UNIQUE(location_id, prediction_target_date, extraction_date);
            END IF;
        END $$;
    """))


def load_prediction_dataframe_to_postgres(df, latitude, longitude, city_name):
    """
    Charge la table finale weather_prediction depuis OpenMeteo.
    """
    table_name = POSTGRES_TABLES["prediction"]
    date_col = "prediction_target_date"
    df_pg = df.copy()
    df_pg[date_col] = remove_timezone(df_pg.pop("date"))
    df_pg["extraction_date"] = remove_timezone(df_pg["extraction_date"])
    df_pg["extraction_date"] = df_pg["extraction_date"].fillna(pd.Timestamp.now())

    if df_pg[date_col].isna().any():
        raise ValueError("Impossible de charger weather_prediction avec une prediction_target_date vide.")

    engine = get_pg_engine()
    location_id = get_or_create_location(engine, city_name, latitude, longitude)

    records = []
    empty_metric_rows = 0
    for _, row in df_pg.iterrows():
        metrics = {}
        for col in PREDICTION_METRIC_COLUMNS:
            value = json_safe_value(row[col])
            if value is not None:
                metrics[col] = value

        if not metrics:
            empty_metric_rows += 1

        records.append({
            "location_id": location_id,
            "prediction_target_date": row[date_col],
            "extraction_date": row["extraction_date"],
            "metrics": json.dumps(metrics),
        })

    if empty_metric_rows:
        raise ValueError(
            f"Refus de charger weather_prediction : {empty_metric_rows} ligne(s) "
            "auraient metrics={}"
        )

    with engine.begin() as conn:
        ensure_weather_prediction_pg_schema(conn, table_name)

        if records:
            conn.execute(text(f"""
                INSERT INTO {table_name}
                    (location_id, prediction_target_date, extraction_date, metrics)
                VALUES
                    (:location_id, :prediction_target_date, :extraction_date, CAST(:metrics AS JSONB))
                ON CONFLICT (location_id, prediction_target_date, extraction_date)
                DO UPDATE SET metrics = EXCLUDED.metrics;
            """), records)

    print(f"PostgreSQL charge : {table_name} ({len(df_pg)} lignes)")


def load_prediction_dataframe_to_clickhouse(df, latitude, longitude, city_name):
    """
    Charge la table finale fact_weather_prediction depuis OpenMeteo.
    """
    table_name = CLICKHOUSE_TABLES["prediction"]
    date_col = "prediction_target_date"
    df_ch = df.copy()
    df_ch[date_col] = remove_timezone(df_ch.pop("date"))
    df_ch["extraction_date"] = remove_timezone(df_ch["extraction_date"])
    df_ch["extraction_date"] = df_ch["extraction_date"].fillna(pd.Timestamp.now())
    df_ch["city_name"] = str(city_name)
    df_ch["latitude"] = float(latitude)
    df_ch["longitude"] = float(longitude)

    if df_ch[date_col].isna().any():
        raise ValueError("Impossible de charger fact_weather_prediction avec une prediction_target_date vide.")

    for col in PREDICTION_METRIC_COLUMNS:
        if col not in df_ch.columns:
            df_ch[col] = pd.NA
        df_ch[col] = pd.to_numeric(df_ch[col], errors="coerce")

    df_ch = df_ch[
        [date_col, "extraction_date", "city_name", "latitude", "longitude"] +
        PREDICTION_METRIC_COLUMNS
    ].copy()

    schema_sql = f"""
CREATE TABLE IF NOT EXISTS {table_name} (
    prediction_target_date DateTime,
    extraction_date DateTime,
    city_name String,
    latitude Float64,
    longitude Float64,
    weather_code Nullable(Float32),
    temperature_2m_max Nullable(Float64),
    temperature_2m_min Nullable(Float64),
    precipitation_sum Nullable(Float64),
    wind_speed_10m_max Nullable(Float64),
    soil_moisture_0_to_100cm_mean Nullable(Float64)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(prediction_target_date)
ORDER BY (latitude, longitude, prediction_target_date);
"""

    client = get_ch_client()
    client.command(schema_sql)
    for col in PREDICTION_METRIC_COLUMNS:
        col_type = "Nullable(Float32)" if col == "weather_code" else "Nullable(Float64)"
        client.command(
            f"ALTER TABLE {table_name} "
            f"ADD COLUMN IF NOT EXISTS {col} {col_type}"
        )
    client.insert_df(
        table=table_name,
        df=df_ch,
        settings={"max_partitions_per_insert_block": 0},
    )

    print(f"ClickHouse charge : {table_name} ({len(df_ch)} lignes)")


def load_openmeteo_prediction_final_table(pred_context):
    """
    Extrait, normalise et charge la troisieme table finale OpenMeteo prediction.
    """
    print("Prediction etape 1/4 : appel API OpenMeteo")
    openmeteo_fetch_forecast(**pred_context)

    raw_df, raw_path = read_prediction_bucket_dataframe("raw", pred_context["execution_date"])
    validate_prediction_metrics(raw_df, "Prediction raw", raw_path)

    print("Prediction etape 2/4 : normalisation raw -> normalized")
    normalize_openmeteo_forecast(**pred_context)

    print("Prediction etape 3/4 : lecture et validation normalized")
    prediction_df = read_openmeteo_prediction_dataframe(pred_context["execution_date"])

    print("Prediction etape 4/4 : chargement PostgreSQL et ClickHouse")
    load_prediction_dataframe_to_postgres(
        prediction_df,
        pred_context["latitude"],
        pred_context["longitude"],
        pred_context["city_name"],
    )
    load_prediction_dataframe_to_clickhouse(
        prediction_df,
        pred_context["latitude"],
        pred_context["longitude"],
        pred_context["city_name"],
    )


# ============================================================================
# CONFIGURATION PARTAGEE DES BACKFILLS
# ============================================================================
def get_backfill_config():
    """
    Retourne la configuration manuelle du backfill.
    """
    return {
        "n_days": BACKFILL_N_DAYS,
        "end_date": BACKFILL_END_DATE,
        "target_city": BACKFILL_TARGET_CITY,
    }


def resolve_target_location(target_city):
    """
    Geocode la ville cible du backfill.
    """
    print(f"Recherche des coordonnees pour la ville de: {target_city}...")

    try:
        url = (
            "https://geocoding-api.open-meteo.com/v1/search"
            f"?name={target_city}&count=1&language=fr&format=json"
        )
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()

        if "results" in data and len(data["results"]) > 0:
            latitude = data["results"][0]["latitude"]
            longitude = data["results"][0]["longitude"]
            city_name = data["results"][0].get("name", target_city)
            print(f"Coordonnees trouvees : {latitude}, {longitude} ({city_name})")
            return latitude, longitude, city_name

        raise ValueError(f"Impossible de geocoder {target_city}")
    except Exception as e:
        print(f"Erreur critique lors du geocodage de {target_city} : {e}")
        raise


def build_historical_backfill_context():
    """
    Construit le contexte partage par les DAGs historiques.
    """
    config = get_backfill_config()
    print(
        f"Preparation du contexte historique pour les {config['n_days']} jours "
        f"precedant le {config['end_date'].strftime('%Y-%m-%d')}"
    )
    latitude, longitude, city_name = resolve_target_location(config["target_city"])

    historical_target_dates = [
        config["end_date"] - timedelta(days=day_offset)
        for day_offset in range(config["n_days"])
    ]
    years = [batch["year"] for batch in group_dates_by_year(historical_target_dates)]

    return {
        **config,
        "latitude": latitude,
        "longitude": longitude,
        "city_name": city_name,
        "historical_target_dates": historical_target_dates,
        "years": years,
    }


def build_prediction_backfill_context():
    """
    Construit le contexte partage par le DAG prediction.
    """
    config = get_backfill_config()
    print(
        f"Preparation du contexte prediction pour le "
        f"{config['end_date'].strftime('%Y-%m-%d')}"
    )
    latitude, longitude, city_name = resolve_target_location(config["target_city"])

    return {
        "ds": config["end_date"].strftime("%Y-%m-%d"),
        "execution_date": config["end_date"],
        "latitude": latitude,
        "longitude": longitude,
        "city_name": city_name,
    }


def fetch_historical_daily_to_bucket(**context):
    """
    DAG historique daily - etape fetch -> raw bucket.
    """
    historical_context = build_historical_backfill_context()
    print("HISTORICAL DAILY - FETCH -> BUCKET")
    fetch_openmeteo_daily_by_year(
        historical_context["latitude"],
        historical_context["longitude"],
        historical_context["historical_target_dates"],
    )


def normalize_historical_daily_to_bucket(**context):
    """
    DAG historique daily - etape raw -> normalized bucket.
    """
    historical_context = build_historical_backfill_context()
    print("HISTORICAL DAILY - NORMALIZE -> BUCKET")
    normalize_historical_daily_years(historical_context["years"])


def load_historical_daily_to_databases(**context):
    """
    DAG historique daily - etape normalized -> databases.
    """
    historical_context = build_historical_backfill_context()
    print("HISTORICAL DAILY - LOAD -> DATABASES")

    openmeteo_daily_df = load_normalized_source_dataframe(
        historical_context["years"],
        "openmeteo",
        "daily",
    )
    daily_df = finalize_single_source_dataframe(openmeteo_daily_df, "openmeteo")

    load_seasonality_dataframe_to_postgres(
        daily_df,
        "daily",
        historical_context["latitude"],
        historical_context["longitude"],
        historical_context["city_name"],
    )
    load_seasonality_dataframe_to_clickhouse(
        daily_df,
        "daily",
        historical_context["latitude"],
        historical_context["longitude"],
        historical_context["city_name"],
    )


def fetch_historical_hourly_to_bucket(**context):
    """
    DAG historique hourly - etape fetch -> raw bucket.
    """
    historical_context = build_historical_backfill_context()
    print("HISTORICAL HOURLY - FETCH -> BUCKET")

    fetch_openmeteo_hourly_by_year(
        historical_context["latitude"],
        historical_context["longitude"],
        historical_context["historical_target_dates"],
    )
    fetch_nasapower_by_year(
        historical_context["latitude"],
        historical_context["longitude"],
        historical_context["historical_target_dates"],
    )


def normalize_historical_hourly_to_bucket(**context):
    """
    DAG historique hourly - etape raw -> normalized bucket.
    """
    historical_context = build_historical_backfill_context()
    print("HISTORICAL HOURLY - NORMALIZE -> BUCKET")
    normalize_historical_hourly_years(historical_context["years"])


def load_historical_hourly_to_databases(**context):
    """
    DAG historique hourly - etape normalized -> databases.
    """
    historical_context = build_historical_backfill_context()
    print("HISTORICAL HOURLY - LOAD -> DATABASES")

    openmeteo_hourly_df = load_normalized_source_dataframe(
        historical_context["years"],
        "openmeteo",
        "hourly",
    )
    nasapower_daily_df = load_normalized_source_dataframe(
        historical_context["years"],
        "nasapower",
        "daily",
    )
    hourly_df = merge_nasapower_daily_into_hourly(openmeteo_hourly_df, nasapower_daily_df)

    load_seasonality_dataframe_to_postgres(
        hourly_df,
        "hourly",
        historical_context["latitude"],
        historical_context["longitude"],
        historical_context["city_name"],
    )
    load_seasonality_dataframe_to_clickhouse(
        hourly_df,
        "hourly",
        historical_context["latitude"],
        historical_context["longitude"],
        historical_context["city_name"],
    )


def fetch_prediction_to_bucket(**context):
    """
    DAG prediction - etape fetch -> raw bucket.
    """
    pred_context = build_prediction_backfill_context()
    print("PREDICTION - FETCH -> BUCKET")
    openmeteo_fetch_forecast(**pred_context)
    raw_df, raw_path = read_prediction_bucket_dataframe("raw", pred_context["execution_date"])
    validate_prediction_metrics(raw_df, "Prediction raw", raw_path)


def normalize_prediction_to_bucket(**context):
    """
    DAG prediction - etape raw -> normalized bucket.
    """
    pred_context = build_prediction_backfill_context()
    print("PREDICTION - NORMALIZE -> BUCKET")
    normalize_openmeteo_forecast(**pred_context)
    read_openmeteo_prediction_dataframe(pred_context["execution_date"])


def load_prediction_to_databases(**context):
    """
    DAG prediction - etape normalized -> databases.
    """
    pred_context = build_prediction_backfill_context()
    print("PREDICTION - LOAD -> DATABASES")
    prediction_df = read_openmeteo_prediction_dataframe(pred_context["execution_date"])

    load_prediction_dataframe_to_postgres(
        prediction_df,
        pred_context["latitude"],
        pred_context["longitude"],
        pred_context["city_name"],
    )
    load_prediction_dataframe_to_clickhouse(
        prediction_df,
        pred_context["latitude"],
        pred_context["longitude"],
        pred_context["city_name"],
    )


def load_kc_table_to_postgres(**context):
        """
        DAG de chargement de la table de kc.
        """
        print("CHARGEMENT TABLE KC - LOAD -> POSTGRES")
        
        # Determine the path to the CSV file next to this DAG file
        dag_dir = os.path.dirname(__file__)
        csv_path = os.path.join(dag_dir, "kc.csv")

        
        # Read the CSV file into a DataFrame
        df = pd.read_csv(csv_path)
        
        # Get the PostgreSQL engine
        engine = get_pg_engine()
        
        # Load the DataFrame into the PostgreSQL table 'kc', creating it if it doesn't exist
        df.to_sql('kc', engine, if_exists='replace', index=False)
        
        print(f"Table 'kc' loaded with {len(df)} rows from {csv_path}")
    

# ============================================================================
# CONFIGURATION DES DAGS AIRFLOW
# ============================================================================
default_args = {
    "owner": "airflow",
    "retries": 0,
}


def create_pipeline_dag(dag_id, description, fetch_callable, normalize_callable, load_callable, tags):
    """
    Cree un DAG simple fetch -> normalize -> load.
    """
    with DAG(
        dag_id=dag_id,
        default_args=default_args,
        description=description,
        schedule=None,
        start_date=datetime(2024, 1, 1),
        catchup=False,
        tags=tags,
    ) as dag:
        fetch_task = PythonOperator(
            task_id="fetch_to_bucket",
            python_callable=fetch_callable,
        )
        normalize_task = PythonOperator(
            task_id="normalize_to_bucket",
            python_callable=normalize_callable,
        )
        load_task = PythonOperator(
            task_id="load_to_databases",
            python_callable=load_callable,
        )

        fetch_task >> normalize_task >> load_task

    return dag


historical_daily_dag = create_pipeline_dag(
    dag_id="backfill_weather_historical_daily_v1",
    description="Backfill historique daily : fetch -> bucket -> normalize -> databases",
    fetch_callable=fetch_historical_daily_to_bucket,
    normalize_callable=normalize_historical_daily_to_bucket,
    load_callable=load_historical_daily_to_databases,
    tags=["pcd", "backfill", "historical-daily", "yearly-dataframe-flow"],
)

historical_hourly_dag = create_pipeline_dag(
    dag_id="backfill_weather_historical_hourly_v1",
    description="Backfill historique hourly : fetch -> bucket -> normalize -> databases",
    fetch_callable=fetch_historical_hourly_to_bucket,
    normalize_callable=normalize_historical_hourly_to_bucket,
    load_callable=load_historical_hourly_to_databases,
    tags=["pcd", "backfill", "historical-hourly", "yearly-dataframe-flow"],
)

prediction_dag = create_pipeline_dag(
    dag_id="backfill_weather_prediction_v1",
    description="Backfill prediction : fetch -> bucket -> normalize -> databases",
    fetch_callable=fetch_prediction_to_bucket,
    normalize_callable=normalize_prediction_to_bucket,
    load_callable=load_prediction_to_databases,
    tags=["pcd", "backfill", "prediction", "yearly-dataframe-flow"],
)


with DAG(
    dag_id="load_plant_requirements_v1",
    default_args=default_args,
    description="Charge le referentiel plant_requirements dans PostgreSQL",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["pcd", "plants", "reference-data"],
) as plant_requirements_dag:
    load_plant_requirements_task = PythonOperator(
        task_id="fill_plant_requirements_table",
        python_callable=load_plants_requirements,
    )
