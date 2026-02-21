from __future__ import annotations

import re
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from packages.core.google.gmail import list_messages, get_message


class NewsletterDetector:
    """Detects and filters newsletters from Gmail."""

    # Common newsletter indicators
    NEWSLETTER_PATTERNS = [
        r"newsletter",
        r"digest",
        r"weekly\s+update",
        r"daily\s+update",
        r"subscription",
        r"unsubscribe",
    ]

    # Common newsletter headers
    NEWSLETTER_HEADERS = [
        "list-unsubscribe",
        "list-id",
        "precedence: bulk",
    ]

    def __init__(self):
        self.pattern_regex = re.compile(
            "|".join(self.NEWSLETTER_PATTERNS),
            re.IGNORECASE
        )

    def detect_newsletters(
        self,
        limit: int = 50,
        days_back: int = 7
    ) -> List[Dict[str, Any]]:
        """
        Scan Gmail for potential newsletters.

        Args:
            limit: Maximum number of messages to scan
            days_back: How many days back to search

        Returns:
            List of detected newsletter messages with metadata
        """
        # Build Gmail query for recent messages
        date_filter = self._build_date_filter(days_back)
        query = f"newer_than:{days_back}d"

        # Get message list
        messages = list_messages(limit=limit, query=query)

        newsletters = []
        for msg in messages:
            msg_id = msg.get("id")
            if not msg_id:
                continue

            # Get full message details
            full_msg = get_message(msg_id)

            # Check if it's a newsletter
            if self._is_newsletter(full_msg):
                newsletters.append({
                    "message_id": msg_id,
                    "sender": full_msg.get("from", ""),
                    "sender_email": self._extract_email(full_msg.get("from", "")),
                    "sender_name": self._extract_name(full_msg.get("from", "")),
                    "subject": full_msg.get("subject", ""),
                    "date": full_msg.get("date", ""),
                    "snippet": full_msg.get("snippet", ""),
                })

        return newsletters

    def _is_newsletter(self, message: Dict[str, Any]) -> bool:
        """
        Determine if a message is likely a newsletter.

        Checks:
        - Subject line for newsletter keywords
        - Body content for newsletter patterns
        - Presence of List-Unsubscribe header (via snippet/body check)
        """
        subject = message.get("subject", "").lower()
        body = message.get("bodyText", "").lower()
        snippet = message.get("snippet", "").lower()

        # Check subject line
        if self.pattern_regex.search(subject):
            return True

        # Check body/snippet for unsubscribe links
        if "unsubscribe" in body or "unsubscribe" in snippet:
            return True

        # Check for list-id or other bulk indicators
        if "list-unsubscribe" in body.lower():
            return True

        return False

    def _extract_email(self, from_field: str) -> str:
        """Extract email address from 'Name <email@domain.com>' format."""
        if not from_field:
            return ""

        match = re.search(r"<([^>]+)>", from_field)
        if match:
            return match.group(1).strip()

        # If no angle brackets, assume entire field is email
        if "@" in from_field:
            return from_field.strip()

        return ""

    def _extract_name(self, from_field: str) -> str:
        """Extract sender name from 'Name <email@domain.com>' format."""
        if not from_field:
            return ""

        # Check for name before angle bracket
        match = re.match(r"([^<]+)<", from_field)
        if match:
            return match.group(1).strip().strip('"')

        # If no angle brackets, try to extract before @ sign
        if "@" in from_field:
            return from_field.split("@")[0].strip()

        return from_field.strip()

    def _build_date_filter(self, days_back: int) -> str:
        """Build a date filter for Gmail query."""
        date = datetime.now() - timedelta(days=days_back)
        return date.strftime("%Y/%m/%d")

    def get_newsletters_from_sender(
        self,
        sender_email: str,
        limit: int = 10,
        days_back: int = 30
    ) -> List[Dict[str, Any]]:
        """
        Get recent newsletters from a specific sender.

        Args:
            sender_email: Email address of sender
            limit: Maximum number of messages
            days_back: How many days back to search

        Returns:
            List of newsletter messages from this sender
        """
        query = f"from:{sender_email} newer_than:{days_back}d"
        messages = list_messages(limit=limit, query=query)

        newsletters = []
        for msg in messages:
            msg_id = msg.get("id")
            if not msg_id:
                continue

            full_msg = get_message(msg_id)
            newsletters.append({
                "message_id": msg_id,
                "sender": full_msg.get("from", ""),
                "sender_email": sender_email,
                "subject": full_msg.get("subject", ""),
                "date": full_msg.get("date", ""),
                "snippet": full_msg.get("snippet", ""),
                "body": full_msg.get("bodyText", ""),
            })

        return newsletters
