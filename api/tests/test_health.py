"""Tests for health endpoint."""

import pytest
from httpx import AsyncClient
from app.main import app


@pytest.mark.asyncio
async def test_health_check():
    """Test health endpoint returns 200 and expected data."""
    async with AsyncClient(app=app, base_url="http://test") as client:
        response = await client.get("/api/v1/health")

    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"
    assert "timestamp" in data


@pytest.mark.asyncio
async def test_health_check_no_auth_required():
    """Test health endpoint does not require authentication."""
    async with AsyncClient(app=app, base_url="http://test") as client:
        # Without API key
        response = await client.get("/api/v1/health")

    assert response.status_code == 200
