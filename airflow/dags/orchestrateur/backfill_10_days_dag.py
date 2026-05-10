# ============================================================================
# IMPORTS - Bibliothèques nécessaires pour le DAG
# ============================================================================
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Import des modules réorganisés selon la nouvelle architecture
# On importe les fonctions depuis les packages (via leur __init__.py respectif)
# Cela permet de garder le code de l'orchestrateur propre et lisible.
from openmeteo import fetch as openmeteo_fetch, normalize_openmeteo_daily, normalize_openmeteo_hourly, normalize_openmeteo_forecast, fetch_forecast as openmeteo_fetch_forecast
from nasapower import fetch as nasapower_fetch, normalize_nasapower
from loading import (
    load_daily_pg,
    load_hourly_pg,
    load_prediction_pg,
    load_daily_ch,
    load_hourly_ch,
    load_prediction_ch
)

# ============================================================================
# RÉFÉRENTIEL DES PLANTES (À MODIFIER MANUELLEMENT PAR L'AGRICULTEUR)
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
        "nature_des_sols": ["Fluvisols","Cambisols","Luvisols","Calcisols"],
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
        "nature_des_sols": ["Calcisols","Cambisols","Luvisols","Regosols"],
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
        "nature_des_sols": ["Luvisols","Cambisols","Calcisols","Vertisols"],
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
        "nature_des_sols": ["Cambisols","Fluvisols","Luvisols"],
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
        "nature_des_sols": ["Fluvisols","Cambisols","Luvisols"],
        "crop_cycle_min": 100,
        "crop_cycle_max": 150
    },
    {
        "nom_du_plante": "Piment",#Capsicum annuum
        "temp_min_opt": 17.0,
        "temp_max_opt": 30.0,
        "temp_min_abs": 8.0,
        "temp_max_abs": 35.0,
        "precipitation_min_opt": 1.64,
        "precipitation_max_opt": 3.42,
        "nature_des_sols": ["Fluvisols","Cambisols","Luvisols","Calcisols"],
        "crop_cycle_min": 90,
        "crop_cycle_max": 120
    },
    {
        "nom_du_plante": "Orange",#Citrus sinensis
        "temp_min_opt": 20.0,
        "temp_max_opt": 30.0,
        "temp_min_abs": 13.0,
        "temp_max_abs": 38.0,
        "precipitation_min_opt": 3.28,
        "precipitation_max_opt": 5.48,
        "nature_des_sols": ["Fluvisols","Luvisols","Cambisols"],
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
        "nature_des_sols": ["Calcisols","Luvisols","Cambisols"],
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
        "nature_des_sols": ["Regosols","Fluvisols","Calcisols"],
        "crop_cycle_min": 70,
        "crop_cycle_max": 90
    }
]

def load_plants_requirements(**context):
    """
    Crée la table plant_requirements si elle n'existe pas et insère
    les données manuellement modifiées par l'agriculteur dans la base PostgreSQL.
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
        
        # Migration : ajouter les colonnes si la table existe déjà sans elles
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
            
    print(f"✅ {len(PLANTS_DATA)} plantes insérées/mises à jour dans la table plant_requirements.")


# ============================================================================
# FONCTION PRINCIPALE DE BACKFILL
# ============================================================================
def backfill_last_n_days(**context):
    """
    Cette fonction effectue le backfill (remplissage historique) des N jours.
    
    STRATÉGIE :
    1. Boucle sur les N jours passés pour extraire l'historique (OpenMeteo + NasaPower)
       et les charger dans les bases de données.
    2. Exécute UNE SEULE FOIS la prédiction (OpenMeteo Forecast) basée sur la date de fin.
    """
    
    # -------------------------------------------------------------------------
    # CONFIGURATION MANUELLE DU BACKFILL
    # Modifiez ces variables directement ici pour lancer un backfill spécifique
    # -------------------------------------------------------------------------
    n_days = 3700                         # Nombre de jours à remonter
    end_date = datetime.now()     # Date de fin (ex: datetime(2024, 1, 31))
                                        # Par défaut : Aujourd'hui (datetime.now())
    
    target_city = "sousse"               # Nom de la ville à requêter (sera géocodée automatiquement)
    
    # -------------------------------------------------------------------------
    # INITIALISATION ET GÉOCODAGE
    # -------------------------------------------------------------------------
    print(f"🚀 Démarrage du backfill pour les {n_days} jours précédant le {end_date.strftime('%Y-%m-%d')}")
    print(f"🌍 Recherche des coordonnées pour la ville de: {target_city}...")
    
    import requests
    try:
        url = f"https://geocoding-api.open-meteo.com/v1/search?name={target_city}&count=1&language=fr&format=json"
        response = requests.get(url, timeout=10)
        data = response.json()
        if "results" in data and len(data["results"]) > 0:
            latitude = data["results"][0]["latitude"]
            longitude = data["results"][0]["longitude"]
            city_name = data["results"][0].get("name", target_city)
            print(f"✅ Coordonnées trouvées : {latitude}, {longitude} ({city_name})")
        else:
            raise ValueError(f"Impossible de géocoder {target_city}")
    except Exception as e:
        print(f"❌ Erreur critique lors du géocodage de {target_city} : {e}")
        return
    
    # -------------------------------------------------------------------------
    # PHASE 1 : BOUCLE HISTORIQUE (De J-1 à J-n_days)
    # -------------------------------------------------------------------------
    print(f"\n{'#'*60}")
    print(f"PHASE 1 : EXTRACTION & CHARGEMENT DE L'HISTORIQUE ({n_days} JOURS)")
    print(f"{'#'*60}")

    for day_offset in range(n_days):
        # Calcul de la date cible pour cette itération
        target_date = end_date - timedelta(days=day_offset)
        date_str = target_date.strftime('%Y-%m-%d')
        
        print(f"\n{'-'*60}")
        print(f"📆 Traitement du jour : {date_str} (Décalage : J-{day_offset})")
        print(f"{'-'*60}")
        
        # --- SIMULATION DU CONTEXTE AIRFLOW ---
        fake_context = {
            "ds": date_str,
            "execution_date": target_date,
            "latitude": latitude,
            "longitude": longitude,
            "city_name": city_name,
        }
        
        try:
            # --- ÉTAPE 1 : EXTRACTION (RAW DATA) ---
            print(f"🌍 [1/8] Extraction Historique OpenMeteo (J-2)...")
            openmeteo_fetch(**fake_context)
            
            print(f"🛰️  [2/8] Extraction NASA Power (J-2)...")
            nasapower_fetch(**fake_context)
            
            # --- ÉTAPE 2 : NORMALISATION (CLEANED DATA) ---
            print(f"🔧 [3/8] Normalisation OpenMeteo (Daily & Hourly)...")
            normalize_openmeteo_daily(**fake_context)
            normalize_openmeteo_hourly(**fake_context)
            
            print(f"🔧 [4/8] Normalisation NASA Power...")
            normalize_nasapower(**fake_context)
            
            # --- ÉTAPE 3 : CHARGEMENT (DATA WAREHOUSE / DB) ---
            
            # --- ÉTAPE 5 : CHARGEMENT POSTGRES (DAILY UNIFIÉ) ---
            print(f"💾 [5/8] Chargement PostgreSQL weather_daily (Fusion OpenMeteo+NasaPower)...")
            load_daily_pg(**fake_context)
            
            # --- ÉTAPE 6 : CHARGEMENT CLICKHOUSE (DAILY UNIFIÉ) ---
            print(f"💾 [6/8] Chargement ClickHouse weather_daily (Fusion OpenMeteo+NasaPower)...")
            load_daily_ch(**fake_context)
            
            # --- ÉTAPE 7 : CHARGEMENT POSTGRES (HOURLY) ---
            print(f"💾 [7/8] Chargement PostgreSQL weather_hourly...")
            load_hourly_pg(**fake_context)
            
            # --- ÉTAPE 8 : CHARGEMENT CLICKHOUSE (HOURLY) ---
            print(f"💾 [8/8] Chargement ClickHouse weather_hourly...")
            load_hourly_ch(**fake_context)
            
            print(f"✅ Jour {date_str} traité avec succès !")
            
        except Exception as e:
            print(f"❌ ERREUR lors du traitement du jour {date_str} : {e}")
            print(f"⚠️  Passage au jour suivant...")
            continue 

    # -------------------------------------------------------------------------
    # PHASE 2 : PRÉDICTION (UNE SEULE FOIS)
    # -------------------------------------------------------------------------
    print(f"\n{'#'*60}")
    print(f"PHASE 2 : EXTRACTION DE LA PRÉDICTION (7 JOURS SUIVANTS)")
    print(f"{'#'*60}")
    
    try:
        # La prédiction se base sur la date de fin du backfill (end_date)
        # Elle va extraire les prévisions du end_date à end_date + 7 jours
        # Si end_date est dans le passé, elle utilisera l'API Archive (Hindcast)
        
        pred_context = {
            "ds": end_date.strftime('%Y-%m-%d'),
            "execution_date": end_date,
            "latitude": latitude,
            "longitude": longitude,
            "city_name": city_name,
        }
        
        print(f"🔮 Lancement de la prédiction pour le {end_date.strftime('%Y-%m-%d')}...")
        openmeteo_fetch_forecast(**pred_context)
        
        print(f"🔧 Normalisation Prédiction...")
        normalize_openmeteo_forecast(**pred_context)
        
        print(f"💾 [4/5] Chargement PostgreSQL weather_prediction...")
        load_prediction_pg(**pred_context)
        
        print(f"💾 [5/5] Chargement ClickHouse weather_prediction...")
        load_prediction_ch(**pred_context)

        print(f"✅ Prédiction traitée (Extraite -> Normalisée -> Chargée).")
        
    except Exception as e:
        print(f"❌ ERREUR lors de l'étape de prédiction : {e}")

    print(f"\n{'='*60}")
    print(f"🎉 Le processus de Backfill est terminé !")
    print(f"{'='*60}")


# ============================================================================
# CONFIGURATION DU DAG AIRFLOW
# ============================================================================
default_args = {
    "owner": "airflow",
    "retries": 0,
}

with DAG(
    dag_id="backfill_weather_10_days_v2",
    default_args=default_args,
    description="Backfill manuel configurable via code (Historique + Prédiction unique)",
    schedule=None,        # Ce DAG ne s'exécute pas tout seul (manuel uniquement)
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["pcd", "backfill", "modular"],
) as dag:
    
    # Task pour charger les besoins des plantes (s'exécute en premier)
    plant_req_task = PythonOperator(
        task_id="load_plants_requirements",
        python_callable=load_plants_requirements,
    )

    # Task unique qui appelle la fonction python de backfill
    backfill_task = PythonOperator(
        task_id="run_backfill",
        python_callable=backfill_last_n_days,
    )
    
    plant_req_task >> backfill_task
