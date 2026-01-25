from __future__ import annotations

import json
import logging
from contextlib import nullcontext
from typing import Any, Dict, List, Optional

try:
    from opentelemetry import trace
except Exception:  # pragma: no cover - optional dependency resolution
    trace = None

from .llm.openai_client import OpenAIClient
from .storage.base import ListStore, ThreadStore, ThreadState
from .tools.registry import build_list_tool_registry


class HomeOpsAgent:
    def __init__(self, store: ListStore, llm: OpenAIClient) -> None:
        self._store = store
        self._llm = llm
        self._registry = build_list_tool_registry(store)
        self._logger = logging.getLogger("home_ops.agent")
        self._tracer = trace.get_tracer("home_ops.agent") if trace else None

    def chat(self, message: str, thread_id: Optional[str] = None) -> Dict[str, Any]:
        system_prompt = (
            "You are Home Ops Copilot for woodfamily.ai. "
            "You manage household lists using the available tools. "
            "When the user asks to create/add/get a list, use tools. "
            "After using tools, respond with a helpful summary."
        )
        thread_state: Optional[ThreadState] = None
        if isinstance(self._store, ThreadStore):
            thread_id = self._store.create_thread(thread_id)
            thread_state = self._store.get_thread(thread_id)

        messages: List[Dict[str, Any]] = [{"role": "system", "content": system_prompt}]
        if thread_state and thread_state.summary:
            messages.append(
                {
                    "role": "system",
                    "content": f"Conversation summary: {thread_state.summary}",
                }
            )
        if thread_state and thread_state.recent_messages:
            messages.extend(thread_state.recent_messages)
        messages.append({"role": "user", "content": message})
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
                    "thread_id": thread_id,
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
                    tool_name = (call.get("function") or {}).get("name")
                    raw_args = ((call.get("function") or {}).get("arguments") or "").strip()
                    if raw_args.startswith("```"):
                        lines = raw_args.splitlines()
                        if lines and lines[0].startswith("```"):
                            lines = lines[1:]
                        if lines and lines[-1].startswith("```"):
                            lines = lines[:-1]
                        raw_args = "\n".join(lines).strip()
                    try:
                        parsed_args = json.loads(raw_args) if raw_args else {}
                    except json.JSONDecodeError:
                        self._logger.warning(
                            "tool_args_json_decode_failed tool=%s raw=%r",
                            tool_name,
                            raw_args,
                        )
                        parsed_args = {}
                    if not tool_name or not self._registry.has_tool(tool_name):
                        result = {
                            "status": "error",
                            "error": "unknown_tool",
                            "tool_name": tool_name,
                        }
                    else:
                        try:
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
                        except Exception as exc:
                            self._logger.exception(
                                "tool_failed name=%s args=%s error=%s",
                                tool_name,
                                parsed_args,
                                exc,
                            )
                            result = {
                                "status": "error",
                                "error": "tool_failed",
                                "message": str(exc),
                                "tool_name": tool_name,
                            }
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
            response_payload = {
                "reply": content,
                "tool_calls": tool_calls_meta,
                "thread_id": thread_id,
            }
            self._maybe_update_thread(thread_id, messages, message, content)
            return response_payload

        response_payload = {
            "reply": "I couldn't complete the request right now.",
            "tool_calls": tool_calls_meta,
            "thread_id": thread_id,
        }
        self._maybe_update_thread(thread_id, messages, message, response_payload["reply"])
        return response_payload

    def _maybe_update_thread(
        self,
        thread_id: Optional[str],
        messages: List[Dict[str, Any]],
        user_message: str,
        assistant_reply: str,
    ) -> None:
        if not thread_id or not isinstance(self._store, ThreadStore):
            return
        thread_state = self._store.get_thread(thread_id)
        if thread_state is None:
            return

        recent_messages = [
            msg for msg in thread_state.recent_messages if msg.get("role") in {"user", "assistant"}
        ]
        recent_messages.append({"role": "user", "content": user_message})
        recent_messages.append({"role": "assistant", "content": assistant_reply})
        recent_messages = recent_messages[-10:]

        summary = thread_state.summary.strip()
        if not summary:
            summary = f"Recent: {user_message} -> {assistant_reply}"
        else:
            summary = f"{summary} | {user_message} -> {assistant_reply}"
        if len(summary) > 1000:
            summary = summary[-1000:]

        self._store.update_thread(thread_id, summary, recent_messages)
