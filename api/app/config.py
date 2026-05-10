"""Application configuration using Pydantic Settings."""

from typing import List
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # API metadata
    app_name: str = "PCD Agricultural AI Agent API"
    app_version: str = "1.1.0"
    app_description: str = "AI-powered Chat API with secure ClickHouse tool execution"


    openai_api_key: str = Field(
        default="", 
        description="OpenAI API Key"
    )
    gemini_api_key: str = Field(
        default="", 
        description="Google Gemini API Key"
    )

    # PostgreSQL configuration 
    postgres_url: str = Field(
        default="postgresql+asyncpg://op_user:op_pass@op-db:5432/op_db",
        description="Async PostgreSQL connection URL",
    )

    # ClickHouse configuration
    clickhouse_host: str = Field(default="analytics-db", description="ClickHouse host")
    clickhouse_tcp_port: int = Field(default=9000, description="ClickHouse Native/TCP port")
    clickhouse_http_port: int = Field(default=8123, description="ClickHouse HTTP port")
    clickhouse_database: str = Field(default="analytics_db", description="ClickHouse database name")
    clickhouse_user: str = Field(default="analytics_user", description="ClickHouse username")
    clickhouse_password: str = Field(default="analytics_pass", description="ClickHouse password")

    # Security
    api_keys: str = Field(
        default="demo_key_12345",
        description="Comma-separated list of valid API keys",
    )

    # CORS
    cors_origins: List[str] = Field(
        default=["http://localhost:5173", "http://localhost:3000"], # Added Vite's default port 5173
        description="Allowed CORS origins",
    )

    # Logging
    log_level: str = Field(default="INFO", description="Logging level")
    
    @property
    def api_keys_list(self) -> List[str]:
        """Parse API keys from comma-separated string."""
        return [key.strip() for key in self.api_keys.split(",") if key.strip()]

# Global settings instance
settings = Settings()