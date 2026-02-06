from __future__ import annotations

import json
import logging
import os
import re
from datetime import datetime
from contextlib import nullcontext
from typing import Any, Dict, List, Optional

try:
    from opentelemetry import trace
except Exception:  # pragma: no cover - optional dependency resolution
    trace = None

from .llm.openai_client import OpenAIClient
from .storage.base import BowlingHintStore, ListStore, ThreadStore, ThreadState
from .tools.registry import build_list_tool_registry
from .bowling.casco_stats import get_casco_monday_bowlers
from .bowling.casco_monday import get_casco_monday_team_summary
from .bowling.bopo_averages import get_bopo_averages


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
            "You can also manage bowling routing hints (bowler/team/league) "
            "using the bowling hint tools when asked. "
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

        bowling_reply = self._handle_bowling_query(message)
        if bowling_reply:
            response_payload = {
                "reply": bowling_reply,
                "tool_calls": tool_calls_meta,
                "thread_id": thread_id,
            }
            self._maybe_update_thread(thread_id, messages, message, bowling_reply)
            return response_payload

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

    def _handle_bowling_query(self, message: str) -> Optional[str]:
        lowered = message.lower()
        if not any(
            keyword in lowered
            for keyword in (
                "bowl",
                "bowling",
                "average",
                "league",
                "bopo",
                "casco",
                "monday",
                "thursday",
            )
        ):
            return None

        bowler_name = self._extract_bowler_name(message)
        team_hint = self._match_hint(lowered, self._load_hints("BOWLING_TEAM_HINTS"))
        league = self._infer_league(lowered)

        if "average" in lowered and bowler_name:
            if league == "bopo":
                result = get_bopo_averages(player_name=bowler_name)
                entry = (result.get("bowlers") or [None])[0]
                if entry:
                    return (
                        f"{entry.get('bowler')} bowls for {entry.get('team')}. "
                        f"BoPo average: {entry.get('average') or 'N/A'}."
                    )
                return f"I couldn't find a BoPo average for {bowler_name}."
            result = get_casco_monday_bowlers(player_name=bowler_name)
            entry = (result.get("bowlers") or [None])[0]
            if entry:
                return (
                    f"{entry.get('bowler')} bowls for {entry.get('team')}. "
                    f"Current average: {entry.get('average') or 'N/A'}."
                )
            return f"I couldn't find an average for {bowler_name}."

        if "when does" in lowered and "bowl" in lowered and "next" in lowered:
            target = self._extract_when_target(message)
            if not target and team_hint:
                target = team_hint
            if not target and bowler_name:
                target = bowler_name
            if not target:
                return None
            team_name = target
            if bowler_name:
                result = get_casco_monday_bowlers(player_name=bowler_name)
                entry = (result.get("bowlers") or [None])[0]
                if entry and entry.get("team"):
                    team_name = entry.get("team")
            summary = get_casco_monday_team_summary(team_name=team_name)
            schedule = ((summary.get("team_summary") or {}).get("schedule") or [])
            next_game = self._next_game(schedule)
            if not next_game:
                return (
                    f"{team_name} is in the Monday league, but I couldn't find the "
                    "next scheduled game."
                )
            if bowler_name:
                return (
                    f"{bowler_name} bowls for {team_name}. Next game: "
                    f"{next_game['date']} at {next_game['time']} on lane "
                    f"{next_game.get('lane') or 'TBD'}."
                )
            return (
                f"{team_name} bowls next on {next_game['date']} at "
                f"{next_game['time']} on lane {next_game.get('lane') or 'TBD'}."
            )

        return None

    def _load_hints(self, env_key: str) -> List[str]:
        raw = os.getenv(env_key, "")
        env_hints = [item.strip().lower() for item in raw.split(",") if item.strip()]
        hint_type = self._hint_type_from_env(env_key)
        if hint_type and isinstance(self._store, BowlingHintStore):
            stored = [
                hint.value.strip().lower()
                for hint in self._store.list_bowling_hints(hint_type=hint_type)
                if hint.value
            ]
            return list(dict.fromkeys(env_hints + stored))
        return env_hints

    def _hint_type_from_env(self, env_key: str) -> Optional[str]:
        if env_key == "BOWLING_BOWLER_HINTS":
            return "bowler"
        if env_key == "BOWLING_TEAM_HINTS":
            return "team"
        if env_key == "BOWLING_LEAGUE_HINTS":
            return "league"
        return None

    def _match_hint(self, lowered: str, hints: List[str]) -> Optional[str]:
        for hint in hints:
            if hint in lowered:
                return hint
        return None

    def _infer_league(self, lowered: str) -> Optional[str]:
        if "bopo" in lowered or "thursday" in lowered:
            return "bopo"
        if "casco" in lowered or "monday" in lowered:
            return "casco"
        league_hints = self._load_hints("BOWLING_LEAGUE_HINTS")
        for hint in league_hints:
            if hint in lowered:
                if hint in {"bopo", "casco"}:
                    return hint
        return None

    def _normalize_name(self, raw: str) -> str:
        cleaned = raw.strip()
        cleaned = re.sub(r"'s$", "", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"[^\w .'-]+$", "", cleaned)
        return cleaned.strip()

    def _extract_bowler_name(self, message: str) -> Optional[str]:
        hints = self._load_hints("BOWLING_BOWLER_HINTS")
        lowered = message.lower()
        hint = self._match_hint(lowered, hints)
        if hint:
            return hint
        avg_match = re.search(r"what is ([a-z0-9 .'-]+)s average", lowered)
        if avg_match:
            return self._normalize_name(avg_match.group(1))
        avg_match = re.search(r"what is ([a-z0-9 .'-]+)s bowling average", lowered)
        if avg_match:
            return self._normalize_name(avg_match.group(1))
        return None

    def _extract_when_target(self, message: str) -> Optional[str]:
        match = re.search(r"when does ([a-z0-9 .'-]+) bowl next", message, re.IGNORECASE)
        if not match:
            return None
        return self._normalize_name(match.group(1))

    def _next_game(self, schedule: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if not schedule:
            return None
        now = datetime.now()
        parsed: List[Dict[str, Any]] = []
        for item in schedule:
            date_value = item.get("date") or ""
            month_day = date_value.split("/")
            if len(month_day) != 2:
                continue
            try:
                month = int(month_day[0])
                day = int(month_day[1])
            except ValueError:
                continue
            date_obj = datetime(now.year, month, day)
            parsed.append({**item, "date_obj": date_obj})
        parsed.sort(key=lambda item: item["date_obj"])
        for item in parsed:
            if item["date_obj"] >= now:
                return item
        return parsed[0] if parsed else None

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
