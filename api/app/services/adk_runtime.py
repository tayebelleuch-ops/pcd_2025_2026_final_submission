from __future__ import annotations

import logging
import uuid
from functools import lru_cache
from typing import Optional

from google.adk import Runner
from google.adk.agents import LlmAgent
from google.adk.events import Event
from google.adk.sessions import InMemorySessionService
from google.genai.types import Content, Part

from app.services.agent_config import get_model
from app.services.db_tools import (
    get_specialist_execution_state,
    reset_specialist_execution_state,
    start_specialist_execution_state,
)

logger = logging.getLogger(__name__)

ADK_APP_NAME = "pcd-agriculture-api"
ADK_USER_ID = "pcd-api"
_SESSION_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_URL, "pcd-agriculture-adk-session")
_SESSION_SERVICE = InMemorySessionService()
_RETRY_MARKERS = {"retry", "try again", "again", "reessaye", "reessaie", "réessaye", "réessaie"}
_MAX_SPECIALIST_ITERATIONS = 10
_FINAL_PREFIX = "FINAL:"
_CONTINUE_PREFIX = "CONTINUE:"
_TARGET_FINAL_SENTENCES = 2
_SHORTEN_TRIGGER_WORDS = 90


def _build_user_message(message: str) -> Content:
    return Content(role="user", parts=[Part.from_text(text=message)])


def _resolve_session_id(conversation_id: Optional[str]) -> str:
    if conversation_id:
        return uuid.uuid5(_SESSION_NAMESPACE, conversation_id).hex
    return uuid.uuid4().hex


def _extract_text(event: Event) -> str:
    if not event.content or not event.content.parts:
        return ""
    return "".join(part.text or "" for part in event.content.parts)


def _summarize_event(event: Event) -> dict:
    text = _extract_text(event).strip()
    part_count = len(event.content.parts) if event.content and event.content.parts else 0
    return {
        "author": event.author,
        "partial": event.partial,
        "is_final": event.is_final_response(),
        "part_count": part_count,
        "has_text": bool(text),
        "text_preview": text[:200],
    }


def _serialize_event(event: Event) -> object:
    if hasattr(event, "model_dump"):
        try:
            return event.model_dump()
        except Exception:
            pass
    if hasattr(event, "dict"):
        try:
            return event.dict()
        except Exception:
            pass
    return repr(event)


def _extract_user_message_body(message: str) -> str:
    marker = "USER MESSAGE:"
    if marker not in message:
        return message.strip()
    return message.split(marker, 1)[1].strip()


def _is_retry_request(message: str) -> bool:
    normalized = " ".join(_extract_user_message_body(message).strip().lower().split())
    return normalized in _RETRY_MARKERS


def _parse_control_response(text: str) -> tuple[Optional[str], str]:
    stripped = text.strip()
    upper = stripped.upper()
    if upper.startswith(_FINAL_PREFIX):
        return "final", stripped[len(_FINAL_PREFIX):].strip()
    if upper.startswith(_CONTINUE_PREFIX):
        return "continue", stripped[len(_CONTINUE_PREFIX):].strip()
    return None, stripped


def _count_sentences(text: str) -> int:
    normalized = " ".join(text.split()).strip()
    if not normalized:
        return 0

    sentence_endings = {".", "!", "?"}
    count = sum(1 for char in normalized if char in sentence_endings)
    if count > 0:
        return count
    return 1


def _should_nudge_shorter(text: str) -> bool:
    normalized = " ".join(text.split()).strip()
    if not normalized:
        return False
    if _looks_detail_dense(normalized):
        return False
    if _count_sentences(normalized) > 3:
        return True
    if len(normalized.split()) > _SHORTEN_TRIGGER_WORDS:
        return True
    return False


def _looks_detail_dense(text: str) -> bool:
    numeric_tokens = sum(1 for token in text.split() if any(char.isdigit() for char in token))
    unit_markers = ["%", "mm", "cm", "kg", "ha", "m2", "m²", "jours", "semaines", "mois", "ans"]
    unit_hits = sum(text.lower().count(marker) for marker in unit_markers)
    caution_markers = [
        "si",
        "sauf",
        "mais",
        "cependant",
        "en revanche",
        "selon",
        "probable",
        "risque",
        "incertain",
    ]
    caution_hits = sum(text.lower().count(marker) for marker in caution_markers)
    return numeric_tokens >= 3 or unit_hits >= 2 or caution_hits >= 3


@lru_cache(maxsize=1)
def get_response_shortener_runner() -> Runner:
    shortener_agent = LlmAgent(
        name="response_shortener",
        description="Compresses already-reasoned farmer responses without losing key meaning.",
        instruction=(
            "You rewrite a finished farmer-facing answer in French.\n"
            "Keep the meaning, concrete advice, and decision-critical details.\n"
            "Do not overgeneralize, soften, or simplify away important specifics.\n"
            "Preserve any numbers, dates, durations, quantities, locations, crop names, probabilities, thresholds, and caveats when present.\n"
            "Preserve uncertainty when the original answer is uncertain.\n"
            "Make it shorter only by removing redundancy and filler.\n"
            "Return only the rewritten answer, with no preface.\n"
            "Target 1-2 short sentences when possible, but prefer precision over brevity.\n"
            "Do not mention tools, reasoning steps, SQL, or internal processing."
        ),
        model=get_model(),
    )
    return Runner(
        app_name=f"{ADK_APP_NAME}-response-shortener",
        agent=shortener_agent,
        session_service=_SESSION_SERVICE,
        auto_create_session=True,
    )


async def _nudge_shorter_response(text: str, *, conversation_id: Optional[str] = None) -> str:
    normalized = " ".join(text.split()).strip()
    if not _should_nudge_shorter(normalized):
        return normalized

    session_id = _resolve_session_id(conversation_id)
    prompt = (
        "Rewrite this final answer for the farmer in shorter French.\n"
        "Keep the same meaning and practical advice.\n"
        "Do not overgeneralize or oversimplify.\n"
        "Preserve any numbers, dates, durations, locations, plant names, probabilities, thresholds, warnings, and conditions.\n"
        "If shortening would remove important nuance, keep the nuance.\n"
        "Target 1-2 short sentences when possible, but keep precision first.\n"
        "Return only the rewritten answer.\n\n"
        f"Answer to shorten:\n{normalized}"
    )
    events: list[Event] = []

    try:
        async for event in get_response_shortener_runner().run_async(
            user_id=ADK_USER_ID,
            session_id=session_id,
            new_message=_build_user_message(prompt),
        ):
            events.append(event)

        shorter_text, _ = _extract_response(events, default_agent="response_shortener")
        shorter_normalized = " ".join(shorter_text.split()).strip()
        return shorter_normalized or normalized
    except Exception:
        logger.exception("Failed to shorten final response; returning original answer.")
        return normalized


def _format_specialist_history() -> str:
    state = get_specialist_execution_state()
    if state is None:
        return "No prior observations."

    history = state.get("history") or []
    if not history:
        return "No prior observations."

    lines = ["Observed results so far:"]
    for index, item in enumerate(history, start=1):
        status = "ERROR" if item.get("is_error") else "OK"
        lines.append(
            f"{index}. Tool={item.get('tool_name')} Status={status}\n"
            f"Input={item.get('tool_input')}\n"
            f"Observation={item.get('result')}"
        )
    return "\n".join(lines)


def _build_iterative_specialist_message(original_message: str, iteration: int, max_iterations: int) -> str:
    final_pass = iteration >= max_iterations
    final_pass_rule = (
        "This is the last allowed pass. You must answer with FINAL using the observations you already have. "
        "Do not answer with CONTINUE on this pass."
        if final_pass
        else "If information is still missing after this pass, answer with CONTINUE."
    )
    return (
        f"Farmer request:\n{original_message}\n\n"
        "Execution rules:\n"
        f"- You are on pass {iteration} of {max_iterations}.\n"
        "- Work iteratively using Thought -> Action -> Observation, but keep your private reasoning hidden.\n"
        "- Use at most one tool in this pass.\n"
        "- Before acting, review the observation history below.\n"
        "- Treat tool observations and database results as your primary evidence.\n"
        "- Prefer concrete facts from PostgreSQL and ClickHouse over broad background knowledge whenever the data is available.\n"
        "- Use general domain knowledge only to interpret, connect, or qualify the observed data, not to replace it.\n"
        "- If a SQL/tool result failed earlier, use this pass to correct the syntax or simplify the request instead of surfacing the error.\n"
        f"- {final_pass_rule}\n"
        "- If you have enough information, answer exactly with `FINAL: ` followed by a concise farmer-facing answer in French.\n"
        "- The final answer should be short, ideally 1-2 sentences, and must not mention SQL, tools, iterations, or technical steps.\n"
        "- When the database does not contain enough evidence for a point, say the recommendation is based on general agronomic knowledge or likely conditions rather than presenting it as a database-backed fact.\n"
        "- If you still need one more observation, answer exactly with `CONTINUE: ` followed by a brief note in French about what is still missing.\n\n"
        f"{_format_specialist_history()}"
    )


async def _get_last_user_message(app_name: str, session_id: str) -> Optional[str]:
    session = await _SESSION_SERVICE.get_session(
        app_name=app_name,
        user_id=ADK_USER_ID,
        session_id=session_id,
    )
    if not session:
        return None

    for event in reversed(session.events):
        if event.author != "user":
            continue

        text = _extract_text(event).strip()
        if not text or _is_retry_request(text):
            continue
        return text

    return None


async def _prepare_message_for_retry(app_name: str, session_id: str, message: str) -> str:
    if not _is_retry_request(message):
        return message

    previous_user_message = await _get_last_user_message(app_name, session_id)
    if not previous_user_message:
        return message

    return (
        "The user asked you to retry your previous answer. "
        "Answer their previous request again using the existing conversation context. "
        "Do not say that the previous question is missing.\n\n"
        f"Previous user request:\n{previous_user_message}"
    )


def _extract_response(events: list[Event], default_agent: str) -> tuple[str, str]:
    fallback_text = ""
    fallback_agent = default_agent
    final_text = ""
    final_agent = default_agent
    final_event: Optional[Event] = None

    for event in events:
        if event.author == "user":
            continue

        if event.is_final_response() and not event.partial:
            final_event = event
            final_agent = event.author or default_agent

        text = _extract_text(event)
        if not text:
            continue

        fallback_text = text
        fallback_agent = event.author or default_agent

        if final_event is event:
            final_text = text

    if final_text:
        return final_text, final_agent
    if fallback_text:
        return fallback_text, fallback_agent

    logger.error(
        "ADK runner completed without text response. Event count=%s summaries=%s final_event=%s",
        len(events),
        [_summarize_event(event) for event in events],
        _serialize_event(final_event) if final_event is not None else None,
    )
    if final_event is not None:
        return (
            "Je n'ai pas pu produire une reponse exploitable pour cette demande. Merci de reessayer.",
            final_agent,
        )
    raise RuntimeError("ADK runner completed without producing a text response.")


@lru_cache(maxsize=1)
def get_orchestrator_runner() -> Runner:
    from app.services.orchestrator import get_orchestrator_agent

    return Runner(
        app_name=ADK_APP_NAME,
        agent=get_orchestrator_agent(),
        session_service=_SESSION_SERVICE,
        auto_create_session=True,
    )


async def run_orchestrator_message(message: str, conversation_id: Optional[str] = None) -> dict:
    """Run the shared orchestrator runner and extract the final text response."""
    session_id = _resolve_session_id(conversation_id)
    prepared_message = await _prepare_message_for_retry(ADK_APP_NAME, session_id, message)
    events: list[Event] = []

    logger.info("Launching orchestrator session %s", session_id)

    async for event in get_orchestrator_runner().run_async(
        user_id=ADK_USER_ID,
        session_id=session_id,
        new_message=_build_user_message(prepared_message),
    ):
        events.append(event)

    text, agent_name = _extract_response(events, default_agent="orchestrator_agent")
    text = await _nudge_shorter_response(text, conversation_id=conversation_id)
    logger.info("ADK orchestrator session %s completed via %s", session_id, agent_name)
    return {"text": text, "agent": agent_name, "session_id": session_id}


async def run_agent_message(
    agent: LlmAgent,
    message: str,
    *,
    conversation_id: Optional[str] = None,
) -> dict:
    """Run a specialist agent through a bounded iterative loop."""
    session_id = _resolve_session_id(conversation_id)
    app_name = f"{ADK_APP_NAME}-{agent.name}"
    runner = Runner(
        app_name=app_name,
        agent=agent,
        session_service=_SESSION_SERVICE,
        auto_create_session=True,
    )
    state_token = start_specialist_execution_state(agent.name, message)

    try:
        final_text = ""
        final_agent_name = agent.name

        for iteration in range(1, _MAX_SPECIALIST_ITERATIONS + 1):
            state = get_specialist_execution_state() or {}
            history_before = len(state.get("history") or [])
            iterative_message = _build_iterative_specialist_message(
                message,
                iteration,
                _MAX_SPECIALIST_ITERATIONS,
            )
            prepared_message = await _prepare_message_for_retry(app_name, session_id, iterative_message)
            events: list[Event] = []

            async for event in runner.run_async(
                user_id=ADK_USER_ID,
                session_id=session_id,
                new_message=_build_user_message(prepared_message),
            ):
                events.append(event)

            text, agent_name = _extract_response(events, default_agent=agent.name)
            final_agent_name = agent_name
            directive, payload = _parse_control_response(text)

            state = get_specialist_execution_state() or {}
            history = state.get("history") or []
            new_history = history[history_before:]
            has_new_observation = bool(new_history)
            has_new_error = any(item.get("is_error") for item in new_history)

            logger.info(
                "Specialist %s completed pass %s/%s with directive=%s observations=%s errors=%s",
                agent.name,
                iteration,
                _MAX_SPECIALIST_ITERATIONS,
                directive,
                len(new_history),
                has_new_error,
            )

            if directive == "final" and payload:
                final_text = await _nudge_shorter_response(payload, conversation_id=conversation_id)
                break

            if iteration == _MAX_SPECIALIST_ITERATIONS:
                final_text = await _nudge_shorter_response(payload or (
                    "Je n'ai pas pu confirmer davantage de details, mais voici la meilleure reponse concise possible "
                    "a partir des observations disponibles."
                ), conversation_id=conversation_id)
                break

            if directive == "continue":
                continue

            if has_new_error or has_new_observation:
                continue

            final_text = await _nudge_shorter_response(payload or text, conversation_id=conversation_id)
            break

        logger.info("ADK direct session %s completed via %s", session_id, final_agent_name)
        final_text = await _nudge_shorter_response(final_text, conversation_id=conversation_id)
        return {"text": final_text.strip(), "agent": final_agent_name, "session_id": session_id}
    finally:
        reset_specialist_execution_state(state_token)
