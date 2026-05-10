from __future__ import annotations

import logging
import uuid
from contextvars import ContextVar, Token
from functools import lru_cache
from typing import TYPE_CHECKING, Optional


import asyncpg
from clickhouse_driver import Client
from google.adk.agents import LlmAgent
from google.adk.tools import FunctionTool

from app.services.adk_runtime import run_agent_message, run_orchestrator_message
from app.services.agent_config import get_model
from app.services.agronomy_specialist import get_agronomy_specialist
from app.services.db_tools import get_graph_buffer, reset_graph_buffer, start_graph_buffer
from app.services.climate_specialist import climate_specialist

if TYPE_CHECKING:
    from google.adk.agents.callback_context import CallbackContext
    from google.adk.models.llm_request import LlmRequest


logger = logging.getLogger(__name__)
_delegation_trace: ContextVar[Optional[list[dict[str, str]]]] = ContextVar(
    "delegation_trace",
    default=None,
)

ROUTER_PROMPT = """
You are a Senior Agricultural Advisor. You are the user's primary point of contact.

Communication Rules:

Never mention your internal structure, 'routing', 'specialists', or 'agents'.

Never explain how you are processing the request. Just provide the answer or ask a follow-up question.

Tone: Professional, empathetic, and direct.

If you need information from your internal tools or specialists, do it silently in the background. The user should feel like they are talking to one single, highly-knowledgeable person who already knows their farm details .

Operational Context:
Always refer to the FARM CONTEXT block provided in the user message (Location, Soil Classification, and Farm Size) as your source of truth.

Routing Rules:

Climate Specialist: Delegate via the delegate_to_climate_specialist tool if the query involves weather history, rainfall, forecasts, or climate risks.

Agronomy Specialist: Delegate via the delegate_to_agronomy_specialist tool if the query involves planting dates, soil chemistry (e.g., Fluvisols), crop compatibility, fertilizers, irrigation advice or pest management.

Synthesis: If a query spans both domains, call both specialist delegation tools, then synthesize their outputs into a single, cohesive response.

Guidelines:

Always respond in French.

If the user's request is too vague to route, ask for clarification.

Interpret JSON data from tools into natural, actionable advice.
Keep every farmer-facing answer short and focused, ideally in 1-2 sentences.
Do not overgeneralize. Preserve important specifics, numbers, locations, crop names, timing, and caveats when they matter to the recommendation.
If the evidence is uncertain or conditional, state that uncertainty briefly instead of sounding more certain than the data allows.
Prefer database-grounded answers over broad background knowledge whenever the data is available.
For crop requirements, compatibility, planting windows, soil fit, and thresholds, treat database evidence as the source of truth and use general knowledge only as a light interpretive layer.

When delegating a task to a Specialist, you MUST include the relevant parts of the FARM CONTEXT (Governorate, Soil Type, Size...) in your instructions to them so they can provide accurate, tailored advice.
For greetings or general chat that does not require specialist expertise, answer directly without calling a specialist.
Use only the exact public tool names `delegate_to_climate_specialist` and `delegate_to_agronomy_specialist`. Never invent or prefix tool names with underscores.
"""
_SESSION_HISTORY_TURNS = 6


def start_delegation_trace() -> Token[Optional[list[dict[str, str]]]]:
    """Start request-scoped specialist delegation tracing."""
    return _delegation_trace.set([])


def get_delegation_trace() -> list[dict[str, str]]:
    """Return the current request's specialist delegation trace."""
    return list(_delegation_trace.get() or [])


def reset_delegation_trace(token: Token[Optional[list[dict[str, str]]]]) -> None:
    """Reset request-scoped specialist delegation tracing."""
    _delegation_trace.reset(token)


def append_delegation_trace(specialist: str, text: str) -> None:
    """Record a specialist answer for potential direct-return optimization."""
    trace = _delegation_trace.get()
    if trace is None:
        return
    trace.append({"specialist": specialist, "text": text})


def _extract_event_text(message: str) -> str:
    marker = "USER MESSAGE:"
    if marker in message:
        return message.split(marker, 1)[1].strip()
    return message.strip()


def _format_recent_session_history(callback_context: CallbackContext) -> str:
    current_invocation_id = callback_context.invocation_id
    turns: list[dict[str, str]] = []
    turns_by_invocation: dict[str, dict[str, str]] = {}

    for event in callback_context.session.events:
        if event.invocation_id == current_invocation_id:
            continue

        invocation_turn = turns_by_invocation.get(event.invocation_id)
        if invocation_turn is None:
            invocation_turn = {}
            turns_by_invocation[event.invocation_id] = invocation_turn
            turns.append(invocation_turn)

        text = ""
        if event.content and event.content.parts:
            text = "".join(part.text or "" for part in event.content.parts).strip()
        if not text:
            continue

        if event.author == "user":
            invocation_turn["user"] = _extract_event_text(text)
        else:
            invocation_turn["assistant"] = text

    visible_turns = [
        turn
        for turn in turns
        if turn.get("user") or turn.get("assistant")
    ][-_SESSION_HISTORY_TURNS:]

    if not visible_turns:
        return ""

    lines = ["Conversation history from this same session:"]
    for index, turn in enumerate(visible_turns, start=1):
        user_text = turn.get("user")
        assistant_text = turn.get("assistant")
        if user_text:
            lines.append(f"Turn {index} user: {user_text}")
        if assistant_text:
            lines.append(f"Turn {index} assistant: {assistant_text}")
    return "\n".join(lines)


def _inject_session_history(callback_context: CallbackContext, llm_request: LlmRequest):
    history_block = _format_recent_session_history(callback_context)
    if not history_block:
        return None

    system_instruction = llm_request.config.system_instruction or ""
    llm_request.config.system_instruction = (
        f"{system_instruction}\n\n{history_block}".strip()
    )
    return None


async def delegate_to_climate_specialist(request: str) -> str:
    logger.info("Routing current request to specialist: climate_specialist")
    result = await run_agent_message(climate_specialist, request)
    append_delegation_trace("climate_specialist", result["text"])
    return result["text"]


async def delegate_to_agronomy_specialist(request: str) -> str:
    logger.info("Routing current request to specialist: agronomy_specialist")
    result = await run_agent_message(get_agronomy_specialist(), request)
    append_delegation_trace("agronomy_specialist", result["text"])
    return result["text"]


def _build_delegate_tool(func, *, name: str, description: str) -> FunctionTool:
    tool = FunctionTool(func)
    tool.name = name
    tool.description = description
    return tool


delegate_to_climate_specialist_tool = _build_delegate_tool(
    delegate_to_climate_specialist,
    name="delegate_to_climate_specialist",
    description=(
        "Delegate a climate or forecast question to the iterative climate specialist. "
        "Pass the user's request together with any relevant FARM CONTEXT."
    ),
)


delegate_to_agronomy_specialist_tool = _build_delegate_tool(
    delegate_to_agronomy_specialist,
    name="delegate_to_agronomy_specialist",
    description=(
        "Delegate an agronomy, soil, irrigation, crop, or pest question to the iterative agronomy specialist. "
        "Pass the user's request together with any relevant FARM CONTEXT."
    ),
)



@lru_cache(maxsize=1)
def get_orchestrator_agent() -> LlmAgent:
    return LlmAgent(
        name="orchestrator_agent",
        description="Routeur agricole principal qui délègue aux agents spécialisés.",
        instruction=ROUTER_PROMPT,
        tools=[
            delegate_to_climate_specialist_tool,
            delegate_to_agronomy_specialist_tool,
        ],
        before_model_callback=_inject_session_history,
        model=get_model(),
    )


def _build_frontend_context(
    governorate: Optional[str],
    farm_size: Optional[float],
    farm_size_unit: Optional[str],
    soil_type: Optional[str],
) -> Optional[str]:
    has_context = any(
        value is not None and value != ""
        for value in (governorate, farm_size, farm_size_unit, soil_type)
    )
    if not has_context:
        return None

    governorate_value = governorate or "unknown"
    farm_size_value = "null" if farm_size is None else str(farm_size)
    farm_size_unit_value = farm_size_unit or "unknown"
    soil_type_value = soil_type or "unknown"

    return (
        "FARM CONTEXT:\n"
        f"- governorate: {governorate_value}\n"
        f"- farm_size: {farm_size_value}\n"
        f"- farm_size_unit: {farm_size_unit_value}\n"
        f"- soil_type: {soil_type_value}\n"
        "Use this structured context as authoritative when routing and answering."
    )


def _inject_frontend_context(
    message: str,
    governorate: Optional[str],
    farm_size: Optional[float],
    farm_size_unit: Optional[str],
    soil_type: Optional[str],
) -> str:
    context = _build_frontend_context(
        governorate,
        farm_size,
        farm_size_unit,
        soil_type,
    )
    if context is None:
        return message

    return f"[{context}]\n\nUSER MESSAGE: {message}"


async def process_chat_message(
    message: str,
    conversation_id: Optional[str] = None,
    governorate: Optional[str] = None,
    farm_size: Optional[float] = None,
    farm_size_unit: Optional[str] = None,
    soil_type: Optional[str] = None,
    pg_pool: Optional[asyncpg.Pool] = None,
    ch_client: Optional[Client] = None,
) -> dict:
    """
    Route a chat message through the triage agent and return the specialist response.
    
    Args:
        message: User's chat message.
        conversation_id: Optional conversation grouping ID for tracing.
        governorate: Optional frontend governorate context.
        farm_size: Optional frontend farm size context.
        farm_size_unit: Optional frontend farm size unit context.
        soil_type: Optional frontend soil type context.
        pg_pool: Optional PostgreSQL connection pool (for future use).
        ch_client: Optional ClickHouse client (for future use).
    """
    trace_group_id = conversation_id or uuid.uuid4().hex[:16]
    input_message = _inject_frontend_context(
        message,
        governorate,
        farm_size,
        farm_size_unit,
        soil_type,
    )
    logger.info("Routing agricultural chat message: %r", message)

    graph_buffer_token = start_graph_buffer()
    delegation_trace_token = start_delegation_trace()
    try:
        result = await run_orchestrator_message(input_message, conversation_id=trace_group_id)
        delegation_trace = get_delegation_trace()
        if len(delegation_trace) == 1:
            specialist_result = delegation_trace[0]
            logger.info(
                "Returning direct specialist answer from %s instead of orchestrator reformulation",
                specialist_result["specialist"],
            )
            result = {
                "text": specialist_result["text"],
                "agent": specialist_result["specialist"],
                "session_id": result["session_id"],
            }
        chart_data = get_graph_buffer()
    finally:
        reset_delegation_trace(delegation_trace_token)
        reset_graph_buffer(graph_buffer_token)

    logger.info("Agricultural chat handled by agent: %s", result["agent"])

    return {
        "text": result["text"],
        "chart_data": chart_data,
        "agent": result["agent"],
    }
