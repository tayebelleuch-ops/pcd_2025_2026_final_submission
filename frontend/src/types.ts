// src/types.ts

// The new, static profile we collect from the farmer
export interface FarmProfile {
  governorate: string | null;
  farm_size: number | null;
  farm_size_unit: string | null;
  soil_type: string | null;
}

// The exact payload expected by your POST /api/v1/chat endpoint
export interface ChatRequestPayload extends FarmProfile {
  conversation_id: string;
  message: string;
}
