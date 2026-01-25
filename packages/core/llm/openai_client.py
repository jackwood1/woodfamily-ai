from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from typing import Any, Dict, List, Optional


class OpenAIClient:
    def __init__(
        self,
        api_key: Optional[str] = None,
        base_url: str = "https://api.openai.com/v1",
        model: str = "gpt-4o-mini",
    ) -> None:
        self._api_key = api_key or os.getenv("OPENAI_API_KEY")
        self._base_url = base_url.rstrip("/")
        self._model = model

    def chat(
        self,
        messages: List[Dict[str, Any]],
        tools: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        if not self._api_key:
            raise RuntimeError("OPENAI_API_KEY is required for LLM calls.")

        payload = {
            "model": self._model,
            "messages": messages,
            "tools": tools,
            "tool_choice": "auto",
            "temperature": 0.2,
        }
        data = json.dumps(payload).encode("utf-8")
        request = urllib.request.Request(
            f"{self._base_url}/chat/completions",
            data=data,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self._api_key}",
            },
            method="POST",
        )

        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                body = response.read().decode("utf-8")
                return json.loads(body)
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8") if exc.fp else ""
            raise RuntimeError(
                f"OpenAI HTTP {exc.code} error: {body or exc.reason}"
            ) from exc
