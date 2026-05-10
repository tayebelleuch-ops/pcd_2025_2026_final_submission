"""Tests for API key authentication."""

import pytest
from httpx import AsyncClient
from app.main import app


@pytest.mark.asyncio
async def test_meteorology_endpoint_requires_auth():
    """Test that meteorology endpoints require API key."""
    async with AsyncClient(app=app, base_url="http://test") as client:
        # Without API key
        response = await client.get("/api/v1/meteorology/daily")

    assert response.status_code == 422  # Missing header parameter


@pytest.mark.asyncio
async def test_invalid_api_key():
    """Test that invalid API key is rejected."""
    async with AsyncClient(app=app, base_url="http://test") as client:
        response = await client.get(
            "/api/v1/meteorology/daily",
            headers={"X-API-Key": "invalid_key"}
        )

    assert response.status_code == 401
    data = response.json()
    assert "Invalid API key" in data["detail"]


@pytest.mark.asyncio
async def test_valid_api_key():
    """Test that valid API key is accepted."""
    async with AsyncClient(app=app, base_url="http://test") as client:
        response = await client.get(
            "/api/v1/meteorology/daily",
            headers={"X-API-Key": "demo_key_12345"}
        )

    # Should not get 401, might get 500 if DB not available but auth passed
    assert response.status_code != 401
