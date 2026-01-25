from __future__ import annotations

import json
import logging
from contextlib import nullcontext
from typing import Any, Dict, List

try:
    from opentelemetry import trace
except Exception:  # pragma: no cover - optional dependency resolution
    trace = None

from .llm.openai_client import OpenAIClient
from .storage.base import ListStore
from .tools.registry import build_list_tool_registry


class HomeOpsAgent:
    def __init__(self, store: ListStore, llm: OpenAIClient) -> None:
        self._store = store
        self._llm = llm
        self._registry = build_list_tool_registry(store)
        self._logger = logging.getLogger("home_ops.agent")
        self._tracer = trace.get_tracer("home_ops.agent") if trace else None

    def chat(self, message: str) -> Dict[str, Any]:
        system_prompt = (
            "You are Home Ops Copilot for woodfamily.ai. "
            "You can manage household lists using the available tools. "
            "Use tools when needed, then respond with a helpful summary."
        )
        messages: List[Dict[str, Any]] = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": message},
        ]
        tool_calls_meta: List[Dict[str, Any]] = []
        tools = self._registry.get_tool_schemas()

        for _ in range(3):
            try:
                response = self._llm.chat(messages=messages, tools=tools)
            except Exception as exc:
                self._logger.exception("llm_call_failed error=%s", exc)
                return {
                    "reply": "The LLM request failed. Check server logs for details.",
                    "tool_calls": tool_calls_meta,
                }
            choice = response.get("choices", [{}])[0]
            msg = choice.get("message", {})
            tool_calls = msg.get("tool_calls") or []

            if tool_calls:
                messages.append(
                    {
                        "role": "assistant",
                        "content": msg.get("content"),
                        "tool_calls": tool_calls,
                    }
                )
                for call in tool_calls:
                    tool_name = call.get("function", {}).get("name")
                    raw_args = call.get("function", {}).get("arguments", "{}")
                    try:
                        parsed_args = json.loads(raw_args) if raw_args else {}
                    except json.JSONDecodeError:
                        parsed_args = {}
                    span_context = (
                        self._tracer.start_as_current_span(
                            "tool.call",
                            attributes={
                                "tool.name": tool_name,
                                "tool.args": json.dumps(parsed_args),
                            },
                        )
                        if self._tracer
                        else nullcontext()
                    )
                    with span_context:
                        result = self._registry.call(tool_name, parsed_args)
                    self._logger.info(
                        "tool_call name=%s args=%s result=%s",
                        tool_name,
                        parsed_args,
                        result,
                    )
                    tool_calls_meta.append(
                        {
                            "id": call.get("id"),
                            "name": tool_name,
                            "args": parsed_args,
                            "result": result,
                        }
                    )
                    messages.append(
                        {
                            "role": "tool",
                            "tool_call_id": call.get("id"),
                            "name": tool_name,
                            "content": json.dumps(result),
                        }
                    )
                continue

            content = msg.get("content") or ""
            return {"reply": content, "tool_calls": tool_calls_meta}

        return {
            "reply": "I couldn't complete the request right now.",
            "tool_calls": tool_calls_meta,
        }
