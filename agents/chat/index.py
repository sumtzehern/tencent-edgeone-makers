"""
Direct gateway chat handler — calls EdgeOne AI gateway (OpenAI-compatible) directly.

Route: POST /chat
Response: SSE stream (text/event-stream)

SSE event protocol:
  event: text_delta   data: {"delta": "..."}
  event: tool_called  data: {"tool": "ToolName"}
  event: token_usage  data: {"inputTokens": 123, "outputTokens": 456, "totalTokens": 579}
  event: ping         data: {"ts": 1710000000000}
  event: error        data: {"message": "..."}
  event: done         data: {"stopped": false}
"""

from __future__ import annotations

import json
import os
import time
from typing import Any, AsyncGenerator

import httpx
from dotenv import load_dotenv

from .._logger import create_logger
from .._model import collect_gateway_env, resolve_model_name

load_dotenv()

logger = create_logger("chat")
HEARTBEAT_INTERVAL_S = 5
SYSTEM_PROMPT = (
    "You are a helpful AI assistant built on EdgeOne Makers. "
    "You help developers quickly validate platform capabilities. "
    "Keep responses concise and useful."
)


def sse_event(event: str, data: Any) -> str:
    return f"event: {event}\ndata: {json.dumps(data)}\n\n"


async def handler(ctx: Any) -> AsyncGenerator[str, None]:
    """EdgeOne Makers entry point (async generator streaming)."""
    cid = ctx.conversation_id or ""
    body = ctx.request.body
    user_message: str = body.get("message", "") if isinstance(body, dict) else ""

    if not user_message.strip():
        yield sse_event("error", {"message": "'message' is required"})
        yield sse_event("done", {"stopped": False})
        return

    user_id = str((body.get("userId") or body.get("user_id") or "") if isinstance(body, dict) else "").strip() or None
    store_adapter = ctx.store

    # --- Load conversation history ---
    history = []
    if cid:
        try:
            past = await store_adapter.get_messages(conversation_id=cid, limit=50, order="asc")
            for m in past:
                role = getattr(m, "role", None)
                content = getattr(m, "content", "")
                if role in ("user", "assistant") and content:
                    history.append({"role": role, "content": content})
        except Exception as e:
            logger.error(f"[history] failed to load: {e}")

    # --- Save user message ---
    if cid:
        try:
            await store_adapter.append_message(
                conversation_id=cid,
                role="user",
                content=user_message,
                user_id=user_id,
            )
        except Exception as e:
            logger.error(f"[store] failed to save user message: {e}")

    # --- Build request ---
    messages = history + [{"role": "user", "content": user_message}]
    gateway_env = collect_gateway_env()
    api_key = gateway_env.get("ANTHROPIC_API_KEY") or os.environ.get("AI_GATEWAY_API_KEY", "")
    base_url = gateway_env.get("ANTHROPIC_BASE_URL") or os.environ.get("AI_GATEWAY_BASE_URL", "https://ai-gateway.edgeone.link/v1")
    model = resolve_model_name()

    # Strip trailing slash and ensure /messages endpoint
    base_url = base_url.rstrip("/")
    url = f"{base_url}/messages"

    headers = {
        "x-api-key": api_key,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json",
        "accept": "text/event-stream",
    }

    payload = {
        "model": model,
        "max_tokens": 4096,
        "system": SYSTEM_PROMPT,
        "messages": messages,
        "stream": True,
    }

    logger.log(f"[request] model={model} url={url} history_turns={len(history)}")

    # --- Stream response ---
    stopped = False
    full_response = ""
    last_ping = time.time()
    input_tokens = 0
    output_tokens = 0

    try:
        async with httpx.AsyncClient(timeout=120.0) as client:
            async with client.stream("POST", url, headers=headers, json=payload) as response:
                if response.status_code != 200:
                    error_body = await response.aread()
                    error_text = error_body.decode("utf-8", errors="replace")
                    logger.error(f"[error] status={response.status_code} body={error_text}")
                    yield sse_event("error", {"message": f"Gateway error {response.status_code}: {error_text}"})
                    yield sse_event("done", {"stopped": False})
                    return

                async for line in response.aiter_lines():
                    # Send heartbeat ping if needed
                    now = time.time()
                    if now - last_ping >= HEARTBEAT_INTERVAL_S:
                        yield sse_event("ping", {"ts": int(now * 1000)})
                        last_ping = now

                    # Check cancellation
                    cancel_signal = getattr(ctx.request, "signal", None)
                    if cancel_signal and getattr(cancel_signal, "aborted", False):
                        stopped = True
                        break

                    if not line.startswith("data:"):
                        continue

                    data_str = line[5:].strip()
                    if data_str == "[DONE]":
                        break

                    try:
                        chunk = json.loads(data_str)
                    except json.JSONDecodeError:
                        continue

                    # Anthropic SSE format
                    chunk_type = chunk.get("type", "")

                    if chunk_type == "content_block_delta":
                        delta = chunk.get("delta", {})
                        if delta.get("type") == "text_delta":
                            text = delta.get("text", "")
                            if text:
                                full_response += text
                                yield sse_event("text_delta", {"delta": text})

                    elif chunk_type == "message_delta":
                        # Capture token usage from final message_delta chunk
                        usage = chunk.get("usage", {})
                        if usage.get("output_tokens"):
                            output_tokens = usage["output_tokens"]

                    elif chunk_type == "message_start":
                        # Capture input token count from message_start
                        usage = chunk.get("message", {}).get("usage", {})
                        if usage.get("input_tokens"):
                            input_tokens = usage["input_tokens"]

                    elif chunk_type == "message_stop":
                        break

                    elif chunk_type == "error":
                        err = chunk.get("error", {})
                        yield sse_event("error", {"message": err.get("message", "Unknown error")})
                        yield sse_event("done", {"stopped": False})
                        return

                    # OpenAI-compatible format (fallback for gateway)
                    choices = chunk.get("choices", [])
                    if choices:
                        delta = choices[0].get("delta", {})
                        text = delta.get("content", "")
                        if text:
                            full_response += text
                            yield sse_event("text_delta", {"delta": text})
                        # Capture usage from OpenAI-style final chunk
                        usage = chunk.get("usage", {})
                        if usage:
                            input_tokens = usage.get("prompt_tokens", input_tokens)
                            output_tokens = usage.get("completion_tokens", output_tokens)

    except httpx.TimeoutException:
        logger.error("[error] request timed out")
        yield sse_event("error", {"message": "Request timed out"})
    except Exception as e:
        logger.error(f"[error] {e}")
        yield sse_event("error", {"message": str(e)})

    # Emit token usage for observability
    if input_tokens or output_tokens:
        yield sse_event("token_usage", {
            "inputTokens": input_tokens,
            "outputTokens": output_tokens,
            "totalTokens": input_tokens + output_tokens,
        })
        logger.log(f"[tokens] input={input_tokens} output={output_tokens} total={input_tokens + output_tokens}")

    # --- Save assistant response ---
    if cid and full_response.strip():
        try:
            await store_adapter.append_message(
                conversation_id=cid,
                role="assistant",
                content=full_response.strip(),
                user_id=user_id,
            )
        except Exception as e:
            logger.error(f"[store] failed to save assistant response: {e}")

    yield sse_event("done", {"stopped": stopped})
