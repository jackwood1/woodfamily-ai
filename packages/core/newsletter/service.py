from __future__ import annotations

import os
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

from packages.core.storage.base import (
    NewsletterConfigState,
    NewsletterDigestState,
    NewsletterSubscriptionState,
    NewsletterSummaryState,
)
from packages.core.storage.sqlite import SQLiteListStore

from .detector import NewsletterDetector
from .digest import DigestGenerator
from .summarizer import NewsletterSummarizer


class NewsletterService:
    """High-level service for newsletter management."""

    def __init__(self, db_path: Optional[str] = None):
        if db_path is None:
            base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
            data_dir = os.path.join(base_dir, "apps/api/data")
            db_path = os.path.join(data_dir, "lists.db")

        self.store = SQLiteListStore(db_path=db_path)
        self.detector = NewsletterDetector()
        self.summarizer = NewsletterSummarizer()
        self.digest_generator = DigestGenerator(
            detector=self.detector,
            summarizer=self.summarizer
        )

    def detect_newsletters(
        self,
        limit: int = 50,
        days_back: int = 7
    ) -> List[Dict[str, object]]:
        """Detect newsletters in Gmail."""
        return self.detector.detect_newsletters(limit=limit, days_back=days_back)

    def subscribe_newsletter(
        self,
        sender_email: str,
        sender_name: Optional[str] = None
    ) -> Dict[str, str]:
        """Subscribe to a newsletter."""
        existing = self.store.get_subscription(sender_email)
        if existing:
            return {"status": "already_subscribed", "sender_email": sender_email}

        subscription = NewsletterSubscriptionState(
            id=str(uuid.uuid4()),
            sender_email=sender_email,
            sender_name=sender_name,
            status="active",
            created_at=datetime.now().isoformat(),
            updated_at=datetime.now().isoformat(),
        )
        self.store.create_subscription(subscription)
        return {"status": "ok", "sender_email": sender_email}

    def unsubscribe_newsletter(self, sender_email: str) -> Dict[str, str]:
        """Unsubscribe from a newsletter."""
        self.store.delete_subscription(sender_email)
        return {"status": "ok", "sender_email": sender_email}

    def list_subscriptions(self) -> List[Dict[str, object]]:
        """List all subscriptions."""
        subscriptions = self.store.list_subscriptions()
        return [
            {
                "id": sub.id,
                "sender_email": sub.sender_email,
                "sender_name": sub.sender_name,
                "status": sub.status,
                "created_at": sub.created_at,
                "updated_at": sub.updated_at,
            }
            for sub in subscriptions
        ]

    def pause_subscription(self, sender_email: str) -> Dict[str, str]:
        """Pause a subscription."""
        existing = self.store.get_subscription(sender_email)
        if not existing:
            return {"status": "not_found", "sender_email": sender_email}

        updated = NewsletterSubscriptionState(
            id=existing.id,
            sender_email=existing.sender_email,
            sender_name=existing.sender_name,
            status="paused",
            created_at=existing.created_at,
            updated_at=datetime.now().isoformat(),
        )
        self.store.update_subscription(updated)
        return {"status": "ok", "sender_email": sender_email}

    def resume_subscription(self, sender_email: str) -> Dict[str, str]:
        """Resume a paused subscription."""
        existing = self.store.get_subscription(sender_email)
        if not existing:
            return {"status": "not_found", "sender_email": sender_email}

        updated = NewsletterSubscriptionState(
            id=existing.id,
            sender_email=existing.sender_email,
            sender_name=existing.sender_name,
            status="active",
            created_at=existing.created_at,
            updated_at=datetime.now().isoformat(),
        )
        self.store.update_subscription(updated)
        return {"status": "ok", "sender_email": sender_email}

    def generate_digest(
        self,
        since_date: Optional[str] = None,
        max_newsletters: int = 20
    ) -> Dict[str, object]:
        """Generate a digest from active subscriptions."""
        # Get active subscriptions
        active_subs = self.store.list_subscriptions(status="active")
        if not active_subs:
            return {
                "status": "error",
                "message": "No active subscriptions found"
            }

        sender_emails = [sub.sender_email for sub in active_subs]

        # Generate digest
        digest_data = self.digest_generator.generate_digest(
            subscribed_senders=sender_emails,
            since_date=since_date,
            max_newsletters=max_newsletters
        )

        # Save to database
        digest_state = NewsletterDigestState(
            id=digest_data["digest_id"],
            period_start=digest_data["period_start"],
            period_end=digest_data["period_end"],
            summary=digest_data["summary"],
            newsletter_count=digest_data["newsletter_count"],
            created_at=digest_data["created_at"],
        )

        summaries = [
            NewsletterSummaryState(
                id=str(uuid.uuid4()),
                digest_id=digest_data["digest_id"],
                message_id=n["message_id"] or "",
                sender=n["sender"] or "",
                sender_email=n["sender_email"] or "",
                subject=n["subject"] or "",
                summary=n["summary"] or "",
                received_date=n["date"] or "",
            )
            for n in digest_data.get("newsletters", [])
        ]

        self.store.create_digest(digest_state, summaries)

        return digest_data

    def list_digests(self, limit: int = 10) -> List[Dict[str, object]]:
        """List recent digests."""
        digests = self.store.list_digests(limit=limit)
        return [
            {
                "id": d.id,
                "period_start": d.period_start,
                "period_end": d.period_end,
                "summary": d.summary,
                "newsletter_count": d.newsletter_count,
                "created_at": d.created_at,
            }
            for d in digests
        ]

    def get_digest(self, digest_id: str) -> Optional[Dict[str, object]]:
        """Get a specific digest with all summaries."""
        result = self.store.get_digest(digest_id)
        if not result:
            return None

        digest, summaries = result
        return {
            "id": digest.id,
            "period_start": digest.period_start,
            "period_end": digest.period_end,
            "summary": digest.summary,
            "newsletter_count": digest.newsletter_count,
            "created_at": digest.created_at,
            "newsletters": [
                {
                    "message_id": s.message_id,
                    "sender": s.sender,
                    "sender_email": s.sender_email,
                    "subject": s.subject,
                    "summary": s.summary,
                    "date": s.received_date,
                }
                for s in summaries
            ],
        }

    def get_digest_config(self) -> Dict[str, object]:
        """Get digest configuration."""
        config = self.store.get_config()
        if not config:
            # Return defaults
            return {
                "schedule": "manual",
                "max_per_digest": 20,
                "auto_generate": False,
            }
        return {
            "schedule": config.schedule,
            "max_per_digest": config.max_per_digest,
            "auto_generate": config.auto_generate,
        }

    def update_digest_config(
        self,
        schedule: Optional[str] = None,
        max_per_digest: Optional[int] = None,
        auto_generate: Optional[bool] = None
    ) -> Dict[str, str]:
        """Update digest configuration."""
        current = self.store.get_config()

        new_config = NewsletterConfigState(
            schedule=schedule or (current.schedule if current else "manual"),
            max_per_digest=max_per_digest or (current.max_per_digest if current else 20),
            auto_generate=auto_generate if auto_generate is not None else (current.auto_generate if current else False),
            updated_at=datetime.now().isoformat(),
        )

        self.store.update_config(new_config)
        return {"status": "ok"}
