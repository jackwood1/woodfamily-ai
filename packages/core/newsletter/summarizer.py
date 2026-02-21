from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

from packages.core.llm.openai_client import OpenAIClient


class NewsletterSummarizer:
    """Summarizes newsletter content using LLM."""

    def __init__(self, llm_client: Optional[OpenAIClient] = None):
        self.llm = llm_client or OpenAIClient(
            model=os.getenv("OPENAI_MODEL", "gpt-4o-mini")
        )

    def summarize_newsletter(
        self,
        subject: str,
        body: str,
        sender: str
    ) -> str:
        """
        Summarize a single newsletter.

        Args:
            subject: Newsletter subject line
            body: Newsletter body text
            sender: Newsletter sender

        Returns:
            Concise summary of the newsletter
        """
        # Truncate body if too long (max ~3000 chars)
        truncated_body = body[:3000] if len(body) > 3000 else body

        messages = [
            {
                "role": "system",
                "content": (
                    "You are a newsletter summarization assistant. "
                    "Your job is to extract the key points from newsletters and create "
                    "concise, actionable summaries. Focus on the most important information "
                    "that the reader needs to know. Keep summaries under 150 words."
                )
            },
            {
                "role": "user",
                "content": (
                    f"Summarize this newsletter:\n\n"
                    f"From: {sender}\n"
                    f"Subject: {subject}\n\n"
                    f"{truncated_body}\n\n"
                    f"Provide a concise summary of the key points in 2-3 sentences."
                )
            }
        ]

        # Use simple chat without tools for summarization
        response = self._simple_chat(messages)

        return response

    def summarize_batch(
        self,
        newsletters: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Summarize multiple newsletters.

        Args:
            newsletters: List of newsletter dicts with 'subject', 'body', 'sender'

        Returns:
            List of newsletters with added 'summary' field
        """
        summarized = []

        for newsletter in newsletters:
            subject = newsletter.get("subject", "")
            body = newsletter.get("body", "")
            sender = newsletter.get("sender", "")

            try:
                summary = self.summarize_newsletter(
                    subject=subject,
                    body=body,
                    sender=sender
                )
                summarized.append({
                    **newsletter,
                    "summary": summary
                })
            except Exception as e:
                # If summarization fails, use snippet as fallback
                summarized.append({
                    **newsletter,
                    "summary": newsletter.get("snippet", "Summary unavailable")
                })

        return summarized

    def create_digest_summary(
        self,
        newsletter_summaries: List[Dict[str, Any]]
    ) -> str:
        """
        Create an overall summary of multiple newsletter summaries.

        Args:
            newsletter_summaries: List of newsletters with summaries

        Returns:
            Overall digest summary
        """
        if not newsletter_summaries:
            return "No newsletters to summarize."

        # Build a combined summary request
        summary_text = "\n\n".join([
            f"- {item.get('sender', 'Unknown')}: {item.get('summary', '')}"
            for item in newsletter_summaries[:15]  # Limit to 15 for context
        ])

        messages = [
            {
                "role": "system",
                "content": (
                    "You are a newsletter digest assistant. "
                    "Create a brief overview of the key themes and highlights "
                    "from multiple newsletter summaries."
                )
            },
            {
                "role": "user",
                "content": (
                    f"Here are summaries from {len(newsletter_summaries)} newsletters:\n\n"
                    f"{summary_text}\n\n"
                    f"Provide a 2-3 sentence overview of the main themes and highlights."
                )
            }
        ]

        response = self._simple_chat(messages)
        return response

    def _simple_chat(self, messages: List[Dict[str, Any]]) -> str:
        """
        Simple chat call without tools, returning just the text response.

        Args:
            messages: Chat messages

        Returns:
            Response text
        """
        # Make a simple completion call (no tools)
        payload = {
            "model": self.llm._model,
            "messages": messages,
            "temperature": 0.3,
            "max_tokens": 300,
        }

        import json
        import urllib.request

        data = json.dumps(payload).encode("utf-8")
        request = urllib.request.Request(
            f"{self.llm._base_url}/chat/completions",
            data=data,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.llm._api_key}",
            },
            method="POST",
        )

        with urllib.request.urlopen(request, timeout=self.llm._timeout) as response:
            body = response.read().decode("utf-8")
            result = json.loads(body)
            return result["choices"][0]["message"]["content"]
