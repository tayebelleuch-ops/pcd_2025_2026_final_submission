from __future__ import annotations

import json
import logging
from contextvars import ContextVar, Token
from typing import Any, Optional

from google.adk.tools import FunctionTool

from app.repositories.postgres import fetch_data
from app.repositories.clickhouse import execute_parameterized_query

logger = logging.getLogger(__name__)

_graph_data_buffer: ContextVar[Optional[list[dict[str, Any]]]] = ContextVar(
    "graph_data_buffer",
    default=None,
)
_specialist_execution_state: ContextVar[Optional[dict[str, Any]]] = ContextVar(
    "specialist_execution_state",
    default=None,
)

GRAPH_SUCCESS_RESPONSE = (
    '{"status": "success", "message": "Graph data prepared. Tell the user the chart is ready."}'
)

POSTGRES_TOOL_DESCRIPTION = (
    "Execute a SQL SELECT query against the PostgreSQL relational database. "
    "Use this tool when the user wants to FETCH/EXTRACT specific rows, look up records, "
    "retrieve a single entry, or query normalized/relational data (JOINs between tables). "
    "For crop-specific requirements, thresholds, and compatibility questions, prefer this tool first because plant_requirements is the authoritative source. "
    "DO NOT use for aggregations (MIN, MAX, AVG, SUM, COUNT over large datasets) - use query_clickhouse instead.\n\n"
    "SCHEMA:\n"
    "  Table: location          -- Dimension: id (PK), city_name VARCHAR, latitude DECIMAL, longitude DECIMAL\n"
    "  Table: data_source       -- Dimension: id (PK), name VARCHAR (source identifier, e.g. 'open-meteo')\n"
    "  Table: weather_daily     -- Fact: id, location_id FK, source_id FK, date_mesure DATE, extraction_date TIMESTAMP, metrics JSONB\n"
    "  Table: weather_hourly    -- Fact: id, location_id FK, source_id FK, date_mesure TIMESTAMP, extraction_date TIMESTAMP, metrics JSONB\n"
    "  Table: weather_prediction-- Fact: id, location_id FK, source_id FK, prediction_target_date TIMESTAMP, extraction_date TIMESTAMP, metrics JSONB\n"
    "  Table: plant_requirements-- Dimension: nom_du_plante VARCHAR PK, temp_min_opt FLOAT, temp_max_opt FLOAT, "
    "temp_min_abs FLOAT, temp_max_abs FLOAT, precipitation_min_opt FLOAT, precipitation_max_opt FLOAT, nature_des_sols TEXT[], "
    "crop_cycle_min INT (duree min du cycle cultural en jours), crop_cycle_max INT (duree max du cycle cultural en jours)\n\n"
    "NOTE: weather metrics (temperature, precipitation, etc.) are stored inside the JSONB 'metrics' column. "
    "Example: metrics->>'temperature_2m_mean', metrics->>'precipitation_sum'\n"
    "JOINs: weather_daily JOIN location ON weather_daily.location_id = location.id\n"
    "IMPORTANT: plant_requirements is the preferred source for crop thresholds, soil compatibility, crop-cycle duration, and precipitation/temperature bounds."
)

CLICKHOUSE_TOOL_DESCRIPTION = (
    "Execute a SQL SELECT query against the ClickHouse analytical (columnar) database. "
    "Use this tool for AGGREGATIONS: MIN, MAX, AVG, SUM, COUNT, GROUP BY, time-series analysis, "
    "historical trends, or any computation over large volumes of data. "
    "DO NOT use for simple single-row lookups - use query_postgres instead.\n\n"
    "SCHEMA:\n"
    "  Table: fact_weather_daily\n"
    "    Engine: MergeTree() ORDER BY (latitude, longitude, date_mesure)\n"
    "    Columns: date_mesure Date, extraction_date DateTime, city_name String, "
    "latitude Float64, longitude Float64, temperature_2m_mean Nullable(Float64), "
    "temperature_2m_max Nullable(Float64), temperature_2m_min Nullable(Float64), precipitation_sum Nullable(Float64)\n\n"
    "  Table: fact_weather_hourly\n"
    "    Engine: MergeTree() ORDER BY (latitude, longitude, date_mesure) PARTITION BY toYYYYMM(date_mesure)\n"
    "    Columns: date_mesure DateTime, extraction_date DateTime, city_name String, "
    "latitude Float64, longitude Float64, surface_pressure Nullable(Float64)\n\n"
    "  Table: fact_weather_prediction\n"
    "    Engine: MergeTree() ORDER BY (latitude, longitude, prediction_target_date) PARTITION BY toYYYYMM(prediction_target_date)\n"
    "    Columns: prediction_target_date DateTime, extraction_date DateTime, city_name String, "
    "latitude Float64, longitude Float64, weather_code Nullable(Float32), "
    "temperature_2m_max Nullable(Float64), temperature_2m_min Nullable(Float64), "
    "precipitation_sum Nullable(Float64), wind_speed_10m_max Nullable(Float64), "
    "soil_moisture_0_to_100cm_mean Nullable(Float64)\n\n"
    "NOTE: Use ClickHouse SQL syntax (toYear(), toMonth(), toDate(), formatDateTime(), etc.). "
    "Metrics are direct columns, NO JSONB here."
)


def _truncate_text(value: Any, limit: int = 4000) -> str:
    text = str(value)
    if len(text) <= limit:
        return text
    return f"{text[:limit]}... [truncated]"


def start_specialist_execution_state(agent_name: str, request: str) -> Token[Optional[dict[str, Any]]]:
    """Start request-scoped state for an iterative specialist execution."""
    return _specialist_execution_state.set(
        {
            "agent_name": agent_name,
            "request": request,
            "history": [],
        }
    )


def get_specialist_execution_state() -> Optional[dict[str, Any]]:
    """Return the current specialist execution state, if any."""
    return _specialist_execution_state.get()


def reset_specialist_execution_state(token: Token[Optional[dict[str, Any]]]) -> None:
    """Reset the request-scoped specialist execution state."""
    _specialist_execution_state.reset(token)


def append_specialist_observation(
    tool_name: str,
    tool_input: Any,
    result: Any,
    *,
    is_error: bool = False,
) -> None:
    """Append a tool observation to the active specialist execution state."""
    state = _specialist_execution_state.get()
    if state is None:
        return

    history = state.setdefault("history", [])
    history.append(
        {
            "tool_name": tool_name,
            "tool_input": _truncate_text(tool_input),
            "result": _truncate_text(result),
            "is_error": is_error,
        }
    )


def start_graph_buffer() -> Token[Optional[list[dict[str, Any]]]]:
    """Start a request-scoped chart buffer for intercepted graph data."""
    return _graph_data_buffer.set(None)


def get_graph_buffer() -> Optional[list[dict[str, Any]]]:
    """Return the current request's intercepted graph data, if any."""
    return _graph_data_buffer.get()


def reset_graph_buffer(token: Token[Optional[list[dict[str, Any]]]]) -> None:
    """Reset the request-scoped chart buffer after the response is assembled."""
    _graph_data_buffer.reset(token)


async def query_postgres(query: str) -> str:
    """Execute a PostgreSQL SELECT query and return rows as a JSON string.
    SCHEMA:
location   
    id        | integer               
    city_name | character varying(100) 
    latitude  | numeric(9,6)           
    longitude | numeric(9,6)          
plant_requirements
    nom_du_plante         | character varying(255)  
    temp_min_opt          | double precision       
    temp_max_opt          | double precision       
    temp_min_abs          | double precision       
    temp_max_abs          | double precision       
    precipitation_min_opt | double precision       
    precipitation_max_opt | double precision       
    nature_des_sols       | text[]                 
    crop_cycle_min        | integer                
    crop_cycle_max        | integer                
weather_daily
    id              | bigint                      
    location_id     | integer                     
    date_mesure     | timestamp without time zone  
    extraction_date | timestamp without time zone  
    metrics         | jsonb {"precipitation_sum", "temperature_2m_max", "temperature_2m_min","temperature_2m_mean"}                     

weather_hourly
    id              | bigint                      
    location_id     | integer                     
    date_mesure     | timestamp without time zone 
    extraction_date | timestamp without time zone 
    metrics         | jsonb {"wind_speed_2m", "surface_pressure", "precipitation_sum", "temperature_2m_mean", "relative_humidity_2m", "all_sky_surface_shortwave_radiation_downward", "clear_sky_surface_shortwave_radiation_downward"}                    

weather_prediction
    id                     | bigint                      
    location_id            | integer                     
    prediction_target_date | timestamp without time zone 
    extraction_date        | timestamp without time zone 
    metrics                | jsonb {"weather_code", "precipitation_sum", "temperature_2m_max", "temperature_2m_min", "wind_speed_10m_max", "soil_moisture_0_to_100cm_mean"}                    

    """
    logger.info("Executing PostgreSQL agent tool query: %s", query.strip())
    try:
        data = await fetch_data(query)
        result = json.dumps(data, default=str)
        append_specialist_observation("query_postgres", query, result)
        return result
    except Exception as e:
        error_msg = f"PostgreSQL query failed: {str(e)}. Review your SQL syntax and table schema, then retry."
        logger.warning("PostgreSQL execution error: %s", error_msg)
        append_specialist_observation("query_postgres", query, error_msg, is_error=True)
        return error_msg


def query_clickhouse(query: str) -> str:
    """Execute a ClickHouse SELECT query and return rows as a JSON string.
    SCHEMA:
    fact_weather_prediction (`prediction_target_date` DateTime, `extraction_date` DateTime, `city_name` String, `latitude` Float64, `longitude` Float64, `weather_code` Nullable(Float32), `temperature_2m_max` Nullable(Float64), `temperature_2m_min` Nullable(Float64), `precipitation_sum` Nullable(Float64), `wind_speed_10m_max` Nullable(Float64), `soil_moisture_0_to_100cm_mean` Nullable(Float64))
    fact_weather_hourly (`date_mesure` DateTime, `extraction_date` DateTime, `city_name` String, `latitude` Float64, `longitude` Float64, `surface_pressure` Nullable(Float64), `all_sky_surface_shortwave_radiation_downward` Nullable(Float64), `clear_sky_surface_shortwave_radiation_downward` Nullable(Float64), `precipitation_sum` Nullable(Float64), `relative_humidity_2m` Nullable(Float64), `temperature_2m_mean` Nullable(Float64), `wind_speed_2m` Nullable(Float64)) 
    fact_weather_daily (`date_mesure` Date, `extraction_date` DateTime, `city_name` String, `latitude` Float64, `longitude` Float64, `temperature_2m_mean` Nullable(Float64), `temperature_2m_max` Nullable(Float64), `temperature_2m_min` Nullable(Float64), `precipitation_sum` Nullable(Float64))
    """
    logger.info("Executing ClickHouse agent tool query: %s", query.strip())
    try:
        data = execute_parameterized_query(query)
        result = json.dumps(data, default=str)
        append_specialist_observation("query_clickhouse", query, result)
        return result
    except Exception as e:
        error_msg = f"ClickHouse query failed: {str(e)}. Review your SQL syntax and table schema, then retry."
        logger.warning("ClickHouse execution error: %s", error_msg)
        append_specialist_observation("query_clickhouse", query, error_msg, is_error=True)
        return error_msg


async def check_soil_compatibility(farmer_soil_type: str, nom_du_plante: str) -> str:
    """
    Check whether the farmer's soil type is compatible with a plant's requirements.

    Args:
        farmer_soil_type: Soil type injected by the frontend context.
        nom_du_plante: Plant name to look up in plant_requirements.
    """
    query = "SELECT nature_des_sols FROM plant_requirements WHERE nom_du_plante = %(nom_du_plante)s;"

    try:
        asyncpg_query = query.replace("%(nom_du_plante)s", "$1")
        rows = await fetch_data(asyncpg_query, nom_du_plante)

        if not rows:
            return json.dumps(
                {
                    "compatible": False,
                    "message": f"Aucune exigence de sol trouvée pour {nom_du_plante}.",
                },
                default=str,
            )

        allowed_soils = rows[0].get("nature_des_sols") or []
        normalized_allowed = {str(soil).strip().lower() for soil in allowed_soils}
        normalized_farmer_soil = farmer_soil_type.strip().lower()
        compatible = normalized_farmer_soil in normalized_allowed

        if compatible:
            message = f"Le sol '{farmer_soil_type}' est compatible avec {nom_du_plante}."
        else:
            message = (
                f"Le sol '{farmer_soil_type}' n'est pas listé comme compatible avec "
                f"{nom_du_plante}. Sols compatibles: {', '.join(map(str, allowed_soils)) or 'aucun'}."
            )

        result = json.dumps({"compatible": compatible, "message": message}, default=str)
        append_specialist_observation(
            "check_soil_compatibility",
            {"farmer_soil_type": farmer_soil_type, "nom_du_plante": nom_du_plante},
            result,
        )
        return result
    except Exception as e:
        error_msg = f"PostgreSQL soil compatibility check failed: {str(e)}"
        logger.warning("Soil compatibility tool error: %s", error_msg)
        append_specialist_observation(
            "check_soil_compatibility",
            {"farmer_soil_type": farmer_soil_type, "nom_du_plante": nom_du_plante},
            error_msg,
            is_error=True,
        )
        return error_msg


async def get_optimal_planting_window(location: str, nom_du_plante: str) -> str:
    """
    Find optimal planting start dates from plant requirements and ClickHouse weather history.

    Args:
        location: City or location name to match against fact_weather_daily.city_name.
        nom_du_plante: Plant name to look up in plant_requirements.
    """
    plant_query = """
    SELECT temp_min_opt, temp_max_opt, temp_min_abs, temp_max_abs, crop_cycle_min, precipitation_min_opt,precipitation_max_opt
    FROM plant_requirements
    WHERE nom_du_plante ILIKE %(nom_du_plante)s;
    """

    clickhouse_query = """
    WITH daily_rollup AS
    (
        SELECT
            toDate(date_mesure) AS day_date,
            avgIf(temperature_2m_mean, (toHour(date_mesure) >= 6) AND (toHour(date_mesure) <= 18)) AS daytime_temp_avg,
            min(temperature_2m_mean) AS daily_temp_min,
            max(temperature_2m_mean) AS daily_temp_max,
            sum(precipitation_sum) AS daily_rain
        FROM fact_weather_hourly
        WHERE (city_name ILIKE 'Sousse') AND (date_mesure >= subtractYears('2026-01-31', 30))
        GROUP BY day_date
    ),daily_flags AS( SELECT
            day_date AS date_mesure,
            (daytime_temp_avg >= %(c_min)s) AND (daytime_temp_avg <= %(c_max)s) AS is_perfect,
            (daily_temp_min < %(abs_min)s) OR (daily_temp_max > %(abs_max)s) AS is_fatal,
            ifNull(daily_rain, 0) AS daily_rain
        FROM daily_rollup
        ORDER BY date_mesure ASC
    ),
    windows AS
    (
        SELECT
            date_mesure,
            toMonth(date_mesure) AS month,
            toDayOfMonth(date_mesure) AS day,
            toYear(date_mesure) AS record_year,
            sum(is_perfect) OVER w_germ AS germ_days,
            sum(is_perfect) OVER w_cycle AS perfect_days,
            sum(is_fatal) OVER w_cycle AS fatal_days,
            sum(daily_rain) OVER w_cycle AS cycle_total_rain
        FROM daily_flags
        WINDOW
            w_germ AS (ORDER BY date_mesure ASC ROWS BETWEEN CURRENT ROW AND 6 FOLLOWING),
            w_cycle AS (ORDER BY date_mesure ASC ROWS BETWEEN CURRENT ROW AND %(cycle_len)s FOLLOWING)
    ),
    daily_success AS (
        SELECT 
            date_mesure,
            month,
            day,
            (germ_days >= 5 AND perfect_days >= (%(cycle_len)s * 0.4) AND fatal_days = 0) AND (cycle_total_rain <= 1500) AS is_success,
            cycle_total_rain,
            if((2026 - record_year) <= 5, 1., 1. / sqrt(2026 - record_year)) AS year_weight
        FROM windows
        WHERE date_mesure <= subtractDays('2026-01-31', %(cycle_len)s) 
    )
    SELECT
        month,
        day,
        count() AS years_analyzed,
        round(sum(year_weight), 2) AS weighted_years_analyzed,
        round(sum(is_success * year_weight), 2) AS weighted_successful_years,
        round((sum(is_success * year_weight) / sum(year_weight)) * 100, 1) AS success_probability_pct,
        round(sum(cycle_total_rain * year_weight) / sum(year_weight), 1) AS expected_rainfall_mm
    FROM daily_success
    GROUP BY
        month,
        day
    HAVING years_analyzed >= 5
    ORDER BY
        success_probability_pct DESC,
        weighted_successful_years DESC,
        month ASC,
        day ASC
    LIMIT 30; 
    """
    logger.info("Executing optimal planting window tool for plant '%s' in location '%s'", nom_du_plante, location)
    try:
        asyncpg_query = plant_query.replace("%(nom_du_plante)s", "$1")
        plant_rows = await fetch_data(asyncpg_query, nom_du_plante)

        if not plant_rows:
            return json.dumps(
                {"optimal_start_dates": [], "message": f"Aucune exigence trouvée pour {nom_du_plante}."},
                default=str,
            )

        plant = plant_rows[0]
        params = {
            "c_min": plant["temp_min_opt"],
            "c_max": plant["temp_max_opt"],
            "abs_min": plant["temp_min_abs"],
            "abs_max": plant["temp_max_abs"],
            "cycle_len": plant["crop_cycle_min"],
            "p_max": plant["precipitation_max_opt"],
            "location": location,
        }
        logger.info(params)
        rows = execute_parameterized_query(clickhouse_query, params)
        windows = [
            {
                "month": row.get("month"),
                "day": row.get("day"),
                "success_probability_pct": row.get("success_probability_pct"),
                "expected_rainfall_mm": row.get("expected_rainfall_mm"),
            }
            for row in rows
        ]
        result = json.dumps(
            {
                "location": location,
                "nom_du_plante": nom_du_plante,
                "precipitation_min_opt": plant.get("precipitation_min_opt"),
                "planting_windows": windows,
            },
            default=str,
        )
        append_specialist_observation(
            "get_optimal_planting_window",
            {"location": location, "nom_du_plante": nom_du_plante},
            result,
        )
        return result
    except Exception as e:
        error_msg = f"Optimal planting window lookup failed: {str(e)}"
        logger.warning("Planting window tool error: %s", error_msg)
        append_specialist_observation(
            "get_optimal_planting_window",
            {"location": location, "nom_du_plante": nom_du_plante},
            error_msg,
            is_error=True,
        )
        return error_msg


async def fetch_graph(clickhouse_sql: str) -> str:
    """
    Execute ClickHouse SQL and intercept the result into the request-scoped chart buffer.

    Args:
        clickhouse_sql: SELECT query that returns chart-ready rows.
    """
    try:
        data = execute_parameterized_query(clickhouse_sql)
        _graph_data_buffer.set(data)
        print("Graph data intercepted and stored in buffer:", data)
        append_specialist_observation("fetch_graph", clickhouse_sql, GRAPH_SUCCESS_RESPONSE)
        return GRAPH_SUCCESS_RESPONSE
    except Exception as e:
        error_msg = f"ClickHouse graph query failed: {str(e)}"
        logger.warning("Graph fetch tool error: %s", error_msg)
        append_specialist_observation("fetch_graph", clickhouse_sql, error_msg, is_error=True)
        return error_msg


def _build_tool(
    func,
    *,
    name: Optional[str] = None,
    description: Optional[str] = None,
) -> FunctionTool:
    tool = FunctionTool(func)
    if name is not None:
        tool.name = name
    if description is not None:
        tool.description = description
    return tool


query_postgres_tool = _build_tool(
    query_postgres,
    name="query_postgres",
    description=POSTGRES_TOOL_DESCRIPTION,
)

query_clickhouse_tool = _build_tool(
    query_clickhouse,
    name="query_clickhouse",
    description=CLICKHOUSE_TOOL_DESCRIPTION,
)

check_soil_compatibility_tool = _build_tool(check_soil_compatibility)
get_optimal_planting_window_tool = _build_tool(get_optimal_planting_window)
fetch_graph_tool = _build_tool(fetch_graph)
