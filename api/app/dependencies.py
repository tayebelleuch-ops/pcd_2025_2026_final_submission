"""Dependency injection for database connections and services."""

import asyncpg
from clickhouse_driver import Client
from fastapi import Depends


async def get_pg_pool() -> asyncpg.Pool:
    """
    FastAPI dependency: return the PostgreSQL async connection pool.
    The pool is lazily initialized on first use.
    """
    from app.repositories.postgres import get_pg_pool as get_pool
    pool = await get_pool()
    return pool


def get_clickhouse_client() -> Client:
    """
    FastAPI dependency: return the ClickHouse client.
    The client is lazily initialized on first use.
    """
    from app.repositories.clickhouse import get_client
    client = get_client()
    return client

