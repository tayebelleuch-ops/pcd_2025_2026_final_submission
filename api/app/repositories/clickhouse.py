import logging
from typing import Any, Dict, List, Optional
from clickhouse_driver import Client
from app.config import settings

# Setup basic logging
logging.basicConfig(level=settings.log_level)
logger = logging.getLogger(__name__)

# Global variable to hold our connection pool
_client: Optional[Client] = None

def get_client() -> Client:
    """Initialize and return the ClickHouse client using Pydantic settings."""
    global _client
    
    if _client is None:
        try:
            _client = Client(
                host=settings.clickhouse_host,
                port=settings.clickhouse_tcp_port,
                database=settings.clickhouse_database,
                user=settings.clickhouse_user,
                password=settings.clickhouse_password,
                # Optional: connection timeouts to prevent hanging API requests
                connect_timeout=10, 
                send_receive_timeout=30
            )
            logger.info("✅ ClickHouse native client initialized successfully.")
        except Exception as e:
            logger.error(f"❌ Failed to connect to ClickHouse: {e}")
            raise RuntimeError("Database connection failed on startup.")
            
    return _client

def execute_parameterized_query(query: str, params: Dict[str, Any] = None) -> List[Dict[str, Any]]:
    """
    Securely execute a SQL query using ClickHouse parameter binding.
    Returns a list of dictionaries mapping column names to row values.
    """
    client = get_client()
    if params is None:
        params = {}
        
    logger.info(f"Executing Query: {query.strip()} | Params: {params}")
    
    try:
        # with_column_types=True returns the schema so we can map column names
        rows, columns = client.execute(query, params, with_column_types=True)
        
        # Extract the column names from the schema tuple
        col_names = [col[0] for col in columns]
        
        # Zip the column names together with the row data
        return [dict(zip(col_names, row)) for row in rows]
        
    except Exception as e:
        logger.error(f"ClickHouse execution error: {e}")
        # We raise the error so the AI Agent service can catch it and tell the user politely
        raise RuntimeError(f"Database query failed: {str(e)}")