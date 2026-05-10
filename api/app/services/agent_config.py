import os
from functools import lru_cache

from google.adk.models.lite_llm import LiteLlm

from app.config import settings

MODEL_NAME = "openai/gpt-5.1"


def configure_openai_api_key() -> None:
    """Expose the configured API key to LiteLLM/OpenAI-compatible providers."""
    if settings.openai_api_key and not os.environ.get("OPENAI_API_KEY"):
        os.environ["OPENAI_API_KEY"] = settings.openai_api_key


def get_openai_api_key() -> str:
    """Resolve the OpenAI API key from settings first, then process env."""
    configure_openai_api_key()
    return settings.openai_api_key or os.environ.get("OPENAI_API_KEY", "")


@lru_cache(maxsize=1)
def get_model() -> LiteLlm:
    """Build the shared ADK model using LiteLLM with an OpenAI-compatible key."""
    api_key = get_openai_api_key()
    kwargs = {"api_key": api_key} if api_key else {}
    return LiteLlm(model=MODEL_NAME, **kwargs,max_tokens=1000,drop_params=True)
