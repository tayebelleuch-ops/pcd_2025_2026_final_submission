import logging
import asyncpg
from clickhouse_driver import Client
from fastapi import APIRouter, Request, HTTPException, Depends
from slowapi import Limiter
from slowapi.util import get_remote_address
import asyncio
from app.config import settings
from app.dependencies import get_pg_pool, get_clickhouse_client
from app.schemas.chat import ChatRequest, ChatResponse
from app.services.orchestrator import process_chat_message

logger = logging.getLogger(__name__)

# Initialize the Wallet Defender (Rate Limiter)
limiter = Limiter(key_func=get_remote_address)

# Create the router
router = APIRouter(prefix="/api/v1", tags=["AI Chat"])

@router.post("/chat", response_model=ChatResponse)
@limiter.limit("5/minute")  # Enforce strict rate limit per IP address
async def chat_endpoint(
    request: Request, 
    payload: ChatRequest,
    pg_pool: asyncpg.Pool = Depends(get_pg_pool),
    ch_client: Client = Depends(get_clickhouse_client)
):
    """
    The main endpoint for the AI Assistant. 
    Accepts a message, processes it via Google Gemini, and returns the response.
    """
    logger.info(f"📥 Received chat request: {payload.message[:50]}...")
    
    try:
        # Hand the validated text and new farm profile data to the AI Engine
        ai_response = await process_chat_message(
            payload.message,
            conversation_id=payload.conversation_id,
            governorate=payload.governorate,
            farm_size=payload.farm_size,
            farm_size_unit=payload.farm_size_unit,
            soil_type=payload.soil_type,
            pg_pool=pg_pool,
            ch_client=ch_client
        )
        logger.info(f"FASTAPI IS ABOUT TO RETURN: {ai_response}")
        
        # Return the strictly formatted Pydantic response
        return ChatResponse(
            response=ai_response.get("text"),
            chart_data=ai_response.get("chart_data")
        )
        
    except Exception as e:
        logger.exception("❌ Chat Endpoint Error: %s", e)
        # Never leak raw backend stack traces to the frontend
        raise HTTPException(
            status_code=500, 
            detail="The AI Agent encountered an error processing your request."
        )
