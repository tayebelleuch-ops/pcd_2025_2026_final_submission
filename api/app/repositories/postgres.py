import logging
from typing import Any, Dict, List, Optional
import asyncpg
from app.config import settings

# Setup basic logging
logging.basicConfig(level=settings.log_level)
logger = logging.getLogger(__name__)

# Global variable to hold our connection pool
_pg_pool: Optional[asyncpg.Pool] = None

async def get_pg_pool() -> asyncpg.Pool:
    """Initialize and return the asyncpg connection pool."""
    global _pg_pool
    
    if _pg_pool is None:
        try:
            # Clean the SQLAlchemy-style URL format if necessary
            clean_url = settings.postgres_url.replace("+asyncpg", "")
            logger.info(f"Connecting to PostgreSQL at {clean_url}")
            _pg_pool = await asyncpg.create_pool(
                dsn=clean_url,
                min_size=1,   # Minimum connections to keep open
                max_size=10,  # Maximum concurrent connections
                command_timeout=60
            )
            logger.info("✅ PostgreSQL async pool initialized successfully.")
        except Exception as e:
            logger.error(f"❌ Failed to connect to PostgreSQL: {e}")
            raise RuntimeError("PostgreSQL connection failed on startup.")
            
    return _pg_pool

async def fetch_data(query: str, *args) -> List[Dict[str, Any]]:
    """
    Securely execute a SELECT query using asyncpg parameter binding.
    Example usage: await fetch_data("SELECT * FROM farms WHERE region = $1", "North")
    """
    pool = await get_pg_pool()
    
    logger.info(f"Executing PG Query: {query.strip()} | Args: {args}")
    
    try:
        # asyncpg's fetch() returns a list of Record objects. 
        # We use a connection from the pool to execute the query.
        async with pool.acquire() as connection:
            records = await connection.fetch(query, *args)
            
            # Convert asyncpg Record objects to standard Python dictionaries
            return [dict(record) for record in records]
            
    except Exception as e:
        logger.error(f"PostgreSQL execution error: {e}")
        raise RuntimeError(f"Database query failed: {str(e)}")

async def close_pg_pool():
    """Gracefully close the connection pool on app shutdown."""
    global _pg_pool
    if _pg_pool is not None:
        await _pg_pool.close()
        logger.info("🔌 PostgreSQL connection pool closed.")