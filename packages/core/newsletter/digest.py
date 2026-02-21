from __future__ import annotations

import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from .detector import NewsletterDetector
from .summarizer import NewsletterSummarizer


class DigestGenerator:
    """Generates newsletter digests from subscribed sources."""

    def __init__(
        self,
        detector: Optional[NewsletterDetector] = None,
        summarizer: Optional[NewsletterSummarizer] = None
    ):
        self.detector = detector or NewsletterDetector()
        self.summarizer = summarizer or NewsletterSummarizer()

    def generate_digest(
        self,
        subscribed_senders: List[str],
        since_date: Optional[str] = None,
        max_newsletters: int = 20
    ) -> Dict[str, Any]:
        """
        Generate a digest from subscribed newsletter sources.

        Args:
            subscribed_senders: List of subscribed email addresses
            since_date: ISO date string (e.g., "2024-01-15"), defaults to 7 days ago
            max_newsletters: Maximum number of newsletters to include

        Returns:
            Digest data with summary and newsletter details
        """
        # Parse date or default to 7 days ago
        if since_date:
            try:
                start_date = datetime.fromisoformat(since_date)
            except ValueError:
                start_date = datetime.now() - timedelta(days=7)
        else:
            start_date = datetime.now() - timedelta(days=7)

        # Calculate days back
        days_back = (datetime.now() - start_date).days
        days_back = max(1, min(days_back, 30))  # Clamp between 1-30 days

        # Collect newsletters from all subscribed senders
        all_newsletters = []

        for sender_email in subscribed_senders:
            try:
                newsletters = self.detector.get_newsletters_from_sender(
                    sender_email=sender_email,
                    limit=max_newsletters // len(subscribed_senders) + 1,
                    days_back=days_back
                )
                all_newsletters.extend(newsletters)
            except Exception as e:
                # Skip sender if there's an error
                continue

        # Limit total newsletters
        all_newsletters = all_newsletters[:max_newsletters]

        if not all_newsletters:
            return {
                "digest_id": str(uuid.uuid4()),
                "period_start": start_date.isoformat(),
                "period_end": datetime.now().isoformat(),
                "summary": "No newsletters found in this period.",
                "newsletter_count": 0,
                "newsletters": [],
                "created_at": datetime.now().isoformat()
            }

        # Summarize each newsletter
        summarized_newsletters = self.summarizer.summarize_batch(all_newsletters)

        # Create overall digest summary
        digest_summary = self.summarizer.create_digest_summary(summarized_newsletters)

        # Build digest
        digest_id = str(uuid.uuid4())
        digest = {
            "digest_id": digest_id,
            "period_start": start_date.isoformat(),
            "period_end": datetime.now().isoformat(),
            "summary": digest_summary,
            "newsletter_count": len(summarized_newsletters),
            "newsletters": [
                {
                    "message_id": n.get("message_id"),
                    "sender": n.get("sender"),
                    "sender_email": n.get("sender_email"),
                    "subject": n.get("subject"),
                    "date": n.get("date"),
                    "summary": n.get("summary"),
                    "snippet": n.get("snippet"),
                }
                for n in summarized_newsletters
            ],
            "created_at": datetime.now().isoformat()
        }

        return digest

    def format_digest_for_email(self, digest: Dict[str, Any]) -> str:
        """
        Format a digest as readable email text.

        Args:
            digest: Digest data dictionary

        Returns:
            Formatted email text
        """
        lines = [
            f"Newsletter Digest",
            f"Period: {digest.get('period_start', '')} to {digest.get('period_end', '')}",
            f"",
            f"Overview:",
            f"{digest.get('summary', '')}",
            f"",
            f"--- {digest.get('newsletter_count', 0)} Newsletters ---",
            f""
        ]

        for i, newsletter in enumerate(digest.get('newsletters', []), 1):
            lines.extend([
                f"{i}. {newsletter.get('sender', 'Unknown')}",
                f"   Subject: {newsletter.get('subject', '')}",
                f"   {newsletter.get('summary', '')}",
                f""
            ])

        return "\n".join(lines)

    def format_digest_for_html(self, digest: Dict[str, Any]) -> str:
        """
        Format a digest as HTML email.

        Args:
            digest: Digest data dictionary

        Returns:
            HTML email content
        """
        newsletters_html = ""
        for newsletter in digest.get('newsletters', []):
            newsletters_html += f"""
            <div style="margin-bottom: 20px; padding: 15px; border-left: 3px solid #4CAF50;">
                <h3 style="margin: 0 0 5px 0;">{newsletter.get('sender', 'Unknown')}</h3>
                <p style="margin: 0 0 5px 0; color: #666; font-size: 14px;">
                    <strong>{newsletter.get('subject', '')}</strong>
                </p>
                <p style="margin: 0; color: #333;">
                    {newsletter.get('summary', '')}
                </p>
            </div>
            """

        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="utf-8">
            <title>Newsletter Digest</title>
        </head>
        <body style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto; padding: 20px;">
            <h1 style="color: #333;">Newsletter Digest</h1>
            <p style="color: #666;">
                Period: {digest.get('period_start', '')} to {digest.get('period_end', '')}
            </p>

            <div style="background-color: #f5f5f5; padding: 15px; margin: 20px 0; border-radius: 5px;">
                <h2 style="margin-top: 0;">Overview</h2>
                <p>{digest.get('summary', '')}</p>
            </div>

            <h2>{digest.get('newsletter_count', 0)} Newsletters</h2>

            {newsletters_html}

            <div style="margin-top: 40px; padding-top: 20px; border-top: 1px solid #ddd; color: #999; font-size: 12px;">
                Generated on {digest.get('created_at', '')}
            </div>
        </body>
        </html>
        """

        return html
