from pydantic import BaseModel
from typing import Any, Dict, List, Optional

class ChatRequest(BaseModel):
    message: str
    conversation_id: Optional[str] = None
    governorate: Optional[str] = None
    farm_size: Optional[float] = None
    farm_size_unit: Optional[str] = None
    soil_type: Optional[str] = None

class ChatResponse(BaseModel):
    response: str
    chart_data: Optional[Dict[str, Any]] = None # Kept as Dict to match your frontend payload expectation, or List if it's an array of dicts!
