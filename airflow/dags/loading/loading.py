import pandas as pd
import clickhouse_connect
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
from commun import get_fs
import logging
import os
import json

# ============================================================================
# INITIALISATION
# ============================================================================
# Initialisation du module de journalisation (logger) pour pister les étapes dans Airflow
logger = logging.getLogger(__name__)

# ============================================================================
# CONNEXIONS AUX BASES DE DONNÉES
# ============================================================================

def get_pg_engine():
    """
    Crée et retourne le moteur de connexion (Engine) SQLAlchemy pour PostgreSQL.
    """
    # Récupération des paramètres de connexion depuis les variables d'environnement
    user = os.getenv("OP_DB_USER", "op_user")
    password = os.getenv("OP_DB_PASS", "op_pass")
    host = os.getenv("OP_DB_HOST", "op-db") # Nom interne du conteneur Docker PostgreSQL
    db_name = os.getenv("OP_DB_NAME", "op_db")
    port = os.getenv("OP_DB_PORT", "5432")
    
    # Construction de l'URL de connexion standard pour SQLAlchemy
    connection_string = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db_name}"
    
    # Renvoie le connecteur prêt à échanger des requêtes avec PostgreSQL
    return create_engine(connection_string)


def get_ch_client():
    """
    Crée et retourne le client de connexion (driver) pour la base ClickHouse.
    """
    # Identifiants lus depuis l'écosystème Docker/Airflow
    host = os.getenv("CLICKHOUSE_HOST", "analytics-db") # Conteneur Docker ClickHouse
    port = int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123"))
    user = os.getenv("CLICKHOUSE_USER", "analytics_user")
    password = os.getenv("CLICKHOUSE_PASSWORD", "analytics_pass")
    database = os.getenv("CLICKHOUSE_DB", "analytics_db")
    
    # Renvoie l'objet client optimisé pour l'injection analytique rapide
    return clickhouse_connect.get_client(
        host=host,
        port=port,
        username=user,
        password=password,
        database=database
    )


# ============================================================================
# EXTRACTION DES DONNÉES DEPUIS MINIO
# ============================================================================

def read_normalized_data(context, data_type="daily"):
    """
    Lit et importe le fichier de données formaté (Parquet) situé sur le bucket MinIO 'normalized'.
    """
    try:
        # 1. Airflow nous informe de la date "logique" ciblée pour l'extraction via la variable 'ds'
        ds = context["ds"]
        execution_dt = datetime.strptime(ds, "%Y-%m-%d") # Transforme le texte en vraie date
        
        # 2. Règle métier temporelle : 
        # Si c'est pour la prédiction on veut les données enregistrées "hier", 
        # mais pour le climat historique consolidé (daily/hourly), on cible les données d'il y a 2 jours.
        if data_type == "prediction":
            target_date = execution_dt
        else:
            target_date = execution_dt - timedelta(days=2)
            
        fs = get_fs() # Connexion au MinIO
        
        # 3. Construction du chemin exact vers le fichier (Architecture partitionnée par date/source)
        path = (
            f"normalized/{data_type}/openmeteo/"
            f"year={target_date.year}/"
            f"month={target_date.month:02d}/"
            f"day={target_date.day:02d}/"
            f"data.parquet"
        )
        
        # Protection : Si le fichier Minio n'existe pas, on abandonne au lieu de forcer une erreur
        if not fs.exists(path):
            logger.warning(f"📂 Aucun fichier trouvé à : {path}")
            return None
            
        logger.info(f"📖 Lecture de : {path}")
        
        # 4. Ouverture du fichier via flux binaire binaire ('rb') -> Traduction en tableau (DataFrame)
        with fs.open(path, "rb") as f:
            df = pd.read_parquet(f)
            return df
            
    except Exception as e:
        logger.error(f"❌ Erreur lecture des données normalisées ({data_type}) : {e}")
        raise


# ============================================================================
# UTILS POSTGRESQL (Gestion des Clés Étrangères)
# ============================================================================

def get_or_create_location(engine, city_name, lat, lon):
    """
    Enregistre les coordonnées géographiques dans la table "location" et renvoie l'ID créé,
    ou récupère l'ID existant si les coordonnées y sont déjà.
    """
    with engine.begin() as conn: # Transaction sécurisée
        # Vérification et création si nécessaire
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS location (
                id SERIAL PRIMARY KEY,
                city_name VARCHAR(100) NOT NULL,
                latitude DECIMAL(9,6) NOT NULL,
                longitude DECIMAL(9,6) NOT NULL
            );
        """))
        
        # Vu que la contrainte d'unicité (latitude, longitude) a été retirée à votre demande,
        # on ne peut plus se reposer sur `ON CONFLICT DO NOTHING`.
        # On procède donc en deux temps :
        res = conn.execute(text("SELECT id FROM location WHERE latitude = :lat AND longitude = :lon"), {"lat": lat, "lon": lon}).scalar()
        if res:
            return res
            
        # Si ça n'existe pas, on insère la nouvelle ville et coordonnée
        query = text("""
            INSERT INTO location (city_name, latitude, longitude) 
            VALUES (:city_name, :lat, :lon) 
            RETURNING id;
        """)
        return conn.execute(query, {"city_name": city_name, "lat": lat, "lon": lon}).scalar()


def get_or_create_source(engine, source_name):
    """
    Même principe que location, mais pour la source de données (ex: "openmeteo").
    Retourne la clé étrangère (ID) associée à ce nom de fournisseur.
    """
    if not source_name:
        source_name = "unknown"
        
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS data_source (
                id SERIAL PRIMARY KEY,
                name VARCHAR(50) UNIQUE NOT NULL
            );
        """))
        
        query = text("""
            INSERT INTO data_source (name) 
            VALUES (:name) 
            ON CONFLICT (name) DO NOTHING;
        """)
        conn.execute(query, {"name": source_name})
        
        res = conn.execute(text("SELECT id FROM data_source WHERE name = :name"), {"name": source_name})
        return res.scalar()


# ============================================================================
# CHARGEMENT POSTGRESQL (Faits JSONB Evolutifs)
# ============================================================================

def load_pg_generic(context, data_type, table_name, date_col, metrics_cols):
    """
    Mécanique principale d'ingestion relationnelle.
    Au lieu de créer 50 colonnes statiques, cette fonction stocke toutes les valeurs
    météorologiques dans une seule supercolonne "metrics", au format paramétrable (JSONB).
    """
    # 1. Récupération des données extraites sous forme de gros tableau
    df = read_normalized_data(context, data_type=data_type)
    if df is None or df.empty:
        logger.warning(f"⚠️ Aucune donnée à charger dans {table_name} (PostgreSQL)")
        return
        
    # 2. Lecture DYNAMIQUE des paramètres lat et lon, propulsés depuis le script DAG
    lat = context.get("latitude")
    lon = context.get("longitude")
    if lat is None or lon is None:
        raise ValueError("Latitude et Longitude doivent être fournis dynamiquement dans le context Airflow.")
        
    # Extraction délicate du nom du fournisseur (ex: openmeteo) depuis les colonnes du Parquet
    source_name = getattr(df, 'source', None)
    if source_name is not None and "source" in df.columns:
        source_name = str(df["source"].iloc[0]) # On prend la toute première ligne comme source
    else:
        source_name = "openmeteo"
    
    engine = get_pg_engine()
    
    city_name = context.get("city_name", "Inconnu")
    
    # 3. Récupération instantanée des clés numérotées étrangères de notre référentiel Postgres
    location_id = get_or_create_location(engine, city_name, lat, lon)
    source_id = get_or_create_source(engine, source_name)
    
    # 4. S'assurer que le container de facturation des données (la table finale) existe bien
    with engine.begin() as conn:
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id BIGSERIAL PRIMARY KEY,
                location_id INT REFERENCES location(id),
                source_id INT REFERENCES data_source(id),
                {date_col} TIMESTAMP NOT NULL,
                extraction_date TIMESTAMP NOT NULL,
                metrics JSONB NOT NULL,
                UNIQUE(location_id, source_id, {date_col})
            );
        """))
        
    # 5. Conversion du tableau horizontal vers la structure orientée dictionnaire JSON
    records = []
    # Pour chaque heure / chaque ligne mesurée
    for _, row in df.iterrows():
        # Transformation des "colonnes ciblées" ('metrics_cols') en simple format texte pour le JSON
        metrics_dict = {col: str(row[col]) if isinstance(row[col], pd.Timestamp) else row[col] 
                        for col in metrics_cols if col in df.columns and pd.notnull(row[col])}
        
        if "extraction_date" in df.columns and pd.notnull(row["extraction_date"]):
            ext_date = row["extraction_date"]
        else:
            ext_date = datetime.now()
            
        # Fabrication de la ligne d'insertion telle qu'elle doit atterir dans la Base de Données
        record = {
            "location_id": location_id,
            "source_id": source_id,
            date_col: row["date"],
            "extraction_date": ext_date,
            "metrics": json.dumps(metrics_dict) # Clé de la réussite : on transforme dict() en chaîne de caractères JSON
        }
        records.append(record)
        
    df_pg = pd.DataFrame(records)
    
    # 6. Dernière opération : l'Insertion ou la Synchronisation ("UPSERT")
    with engine.begin() as conn:
        for r in records:
            # Même protection que pour les IDs : 'ON CONFLICT DO UPDATE'
            # Signifie : Si on a de nouvelles données pour un MÊME JOUR, MÊME VILLE, MÊME FOURNISSEUR,
            # Alors n'ajoute pas une nouvelle ligne, mets simplement le JSON metrics à jour.
            conn.execute(text(f"""
                INSERT INTO {table_name} (location_id, source_id, {date_col}, extraction_date, metrics)
                VALUES (:location_id, :source_id, :date_val, :extraction_date, CAST(:metrics AS JSONB))
                ON CONFLICT (location_id, source_id, {date_col}) 
                DO UPDATE SET metrics = EXCLUDED.metrics, extraction_date = EXCLUDED.extraction_date;
            """), {
                "location_id": r["location_id"],
                "source_id": r["source_id"],
                "date_val": r[date_col],
                "extraction_date": r["extraction_date"],
                "metrics": r["metrics"]
            })

    logger.info(f"✅ {len(df_pg)} lignes insérées dans PostgreSQL : {table_name}")

# --- Fonctions appelables via l'opérateur Python Airflow ---

def load_daily_pg(**context):
    # Les colonnes vitales que nous voulons capturer et pousser dans le JSON
    metrics_cols = ["temperature_2m_mean", "temperature_2m_max", "temperature_2m_min", "precipitation_sum"]
    load_pg_generic(context, "daily", "weather_daily", "date_mesure", metrics_cols)

def load_hourly_pg(**context):
    metrics_cols = ["surface_pressure"]
    load_pg_generic(context, "hourly", "weather_hourly", "date_mesure", metrics_cols)

def load_prediction_pg(**context):
    metrics_cols = [
        "weather_code",
        "temperature_2m_max",
        "temperature_2m_min",
        "precipitation_sum",
        "wind_speed_10m_max",
        "soil_moisture_0_to_100cm_mean",
    ]
    load_pg_generic(context, "prediction", "weather_prediction", "prediction_target_date", metrics_cols)


# ============================================================================
# CHARGEMENT VERS CLICKHOUSE (Colonnes Simples - OBT)
# ============================================================================

# Dans le Big Data analytique comme Clickhouse, les "clés étrangères" ralentissent les jointures.
# La philosophie "One Big Table (OBT)" veut que toutes les données (Lat, Lon, Météo explose) 
# soient juxtaposées physiquement sur chaque ligne.

SCHEMA_DAILY_CH = """
CREATE TABLE IF NOT EXISTS fact_weather_daily (
    date_mesure Date,
    extraction_date DateTime,
    city_name String,
    latitude Float64,
    longitude Float64,
    temperature_2m_mean Nullable(Float64),
    temperature_2m_max Nullable(Float64),
    temperature_2m_min Nullable(Float64),
    precipitation_sum Nullable(Float64)
) ENGINE = ReplacingMergeTree()
ORDER BY (latitude, longitude, date_mesure);
"""

SCHEMA_HOURLY_CH = """
CREATE TABLE IF NOT EXISTS fact_weather_hourly (
    date_mesure DateTime,
    extraction_date DateTime,
    city_name String,
    latitude Float64,
    longitude Float64,
    surface_pressure Nullable(Float64)
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(date_mesure)
ORDER BY (latitude, longitude, date_mesure);
"""

SCHEMA_PREDICTION_CH = """
CREATE TABLE IF NOT EXISTS fact_weather_prediction (
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
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(prediction_target_date)
ORDER BY (latitude, longitude, prediction_target_date);
"""

def load_ch_generic(context, data_type, table_name, schema_sql, date_col, cols_to_keep):
    """
    Insère massivement les informations modélisées (toutes alignées) dans le bloc Clickhouse.
    """
    df = read_normalized_data(context, data_type=data_type)
    if df is None or df.empty:
        logger.warning(f"⚠️ Aucune donnée à charger dans {table_name} (ClickHouse)")
        return
        
    lat = context.get("latitude")
    lon = context.get("longitude")
    if lat is None or lon is None:
        raise ValueError("Latitude et Longitude doivent être fournis dans le context Airflow.")
        
    # Ajustement cosmétique du nom temporel ("date_mesure" etc...)
    if "date" in df.columns:
        df = df.rename(columns={"date": date_col})
        
    # Ici, au lieu d'une Foreign Key pointant vers location_id, on clone les points Lat / Lon brute.
    df["latitude"] = float(lat)
    df["longitude"] = float(lon)
    df["city_name"] = str(context.get("city_name", "Inconnu"))
    
    if "extraction_date" not in df.columns:
        df["extraction_date"] = pd.Timestamp.now()
        
    # Cadrage Strcit: La liste complète de tout ce que réclament la création du DDL de cette table
    columns = [date_col, "extraction_date", "city_name", "latitude", "longitude"] + cols_to_keep
    
    # Comble les trous si l'API externe (MinIo) ne fournit exceptionnellement pas la colonne voulue
    for c in columns:
        if c not in df.columns:
            df[c] = None
            
    # Purge pure et propre : abandonner ou désolidariser toutes variables inexploitées
    # Cela évite les bugs ClickHouse ("Mismatch column structure..") 
    df_ch = df[columns].copy()
    
    # Précautions techniques des timezones: ClickHouse Connect nécessite que l'on nettoie
    # le marquage de fuseau (UTC etc.). Mais avant d'appliquer '.dt', on doit s'assurer
    # que Pandas comprend bien que ce sont des dates et pas du texte !
    
    # Étape A: Forcer la conversion en vrai format datetime Pandas
    df_ch[date_col] = pd.to_datetime(df_ch[date_col])
    df_ch["extraction_date"] = pd.to_datetime(df_ch["extraction_date"])
    
    # Étape B: Suppression du fuseau horaire (tz_convert(None)) s'il existe
    if df_ch[date_col].dt.tz is not None:
        df_ch[date_col] = df_ch[date_col].dt.tz_convert(None)
    if df_ch["extraction_date"].dt.tz is not None:
        df_ch["extraction_date"] = df_ch["extraction_date"].dt.tz_convert(None)

    # L'offensive finale
    client = get_ch_client()
    client.command(schema_sql) # Créé table si inexistante.
    for col in cols_to_keep:
        col_type = "Nullable(Float32)" if col == "weather_code" else "Nullable(Float64)"
        client.command(
            f"ALTER TABLE {table_name} "
            f"ADD COLUMN IF NOT EXISTS {col} {col_type}"
        )
    client.insert_df(table_name, df_ch) # Expédition totale du Dataframe en ultra haut-débit.
    logger.info(f"✅ {len(df_ch)} lignes insérées dans ClickHouse : {table_name}")


def load_daily_ch(**context):
    cols = ["temperature_2m_mean", "temperature_2m_max", "temperature_2m_min", "precipitation_sum"]
    load_ch_generic(context, "daily", "fact_weather_daily", SCHEMA_DAILY_CH, "date_mesure", cols)

def load_hourly_ch(**context):
    cols = ["surface_pressure"]
    load_ch_generic(context, "hourly", "fact_weather_hourly", SCHEMA_HOURLY_CH, "date_mesure", cols)

def load_prediction_ch(**context):
    cols = [
        "weather_code",
        "temperature_2m_max",
        "temperature_2m_min",
        "precipitation_sum",
        "wind_speed_10m_max",
        "soil_moisture_0_to_100cm_mean",
    ]
    load_ch_generic(context, "prediction", "fact_weather_prediction", SCHEMA_PREDICTION_CH, "prediction_target_date", cols)

# -- FORCE DOCKER/AIRFLOW CACHE RELOAD --
