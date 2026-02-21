from __future__ import annotations

import os
from typing import Dict, List, Optional

try:
    from mcp.server.fastmcp import FastMCP
except Exception as exc:  # pragma: no cover - optional dependency resolution
    FastMCP = None
    _MCP_IMPORT_ERROR = exc

from packages.core.newsletter.service import NewsletterService


def _build_mcp() -> "FastMCP":
    if FastMCP is None:
        raise RuntimeError(
            "MCP SDK is not installed. Use Python 3.10+ and install the "
            "'mcp' package in a dedicated environment."
        ) from _MCP_IMPORT_ERROR
    return FastMCP("HomeOpsNewsletterAgent", json_response=True)


mcp = _build_mcp()


def _service() -> NewsletterService:
    db_path = os.getenv("HOME_OPS_DB_PATH")
    return NewsletterService(db_path=db_path)


@mcp.tool()
def detect_newsletters(limit: int = 50, days_back: int = 7) -> List[Dict[str, object]]:
    """Scan Gmail for newsletters from the last N days."""
    return _service().detect_newsletters(limit=limit, days_back=days_back)


@mcp.tool()
def subscribe_newsletter(sender_email: str, sender_name: Optional[str] = None) -> Dict[str, str]:
    """Add a newsletter sender to the digest subscription list."""
    return _service().subscribe_newsletter(sender_email=sender_email, sender_name=sender_name)


@mcp.tool()
def unsubscribe_newsletter(sender_email: str) -> Dict[str, str]:
    """Remove a newsletter sender from the digest subscription list."""
    return _service().unsubscribe_newsletter(sender_email=sender_email)


@mcp.tool()
def list_subscriptions() -> List[Dict[str, object]]:
    """List all newsletter subscriptions."""
    return _service().list_subscriptions()


@mcp.tool()
def pause_subscription(sender_email: str) -> Dict[str, str]:
    """Pause a newsletter subscription without unsubscribing."""
    return _service().pause_subscription(sender_email=sender_email)


@mcp.tool()
def resume_subscription(sender_email: str) -> Dict[str, str]:
    """Resume a paused newsletter subscription."""
    return _service().resume_subscription(sender_email=sender_email)


@mcp.tool()
def generate_digest(
    since_date: Optional[str] = None,
    max_newsletters: int = 20
) -> Dict[str, object]:
    """Generate a digest from subscribed newsletters since a given date (ISO format)."""
    return _service().generate_digest(since_date=since_date, max_newsletters=max_newsletters)


@mcp.tool()
def list_digests(limit: int = 10) -> List[Dict[str, object]]:
    """List recently generated digests."""
    return _service().list_digests(limit=limit)


@mcp.tool()
def get_digest(digest_id: str) -> Optional[Dict[str, object]]:
    """Get a specific digest by ID with all newsletter summaries."""
    return _service().get_digest(digest_id=digest_id)


@mcp.tool()
def get_digest_config() -> Dict[str, object]:
    """Get current digest configuration (schedule, preferences)."""
    return _service().get_digest_config()


@mcp.tool()
def update_digest_config(
    schedule: Optional[str] = None,
    max_per_digest: Optional[int] = None,
    auto_generate: Optional[bool] = None
) -> Dict[str, str]:
    """Update digest configuration settings."""
    return _service().update_digest_config(
        schedule=schedule,
        max_per_digest=max_per_digest,
        auto_generate=auto_generate
    )


def main() -> None:
    if FastMCP is None:
        raise RuntimeError(
            "MCP SDK is not installed. Use Python 3.10+ and install the "
            "'mcp' package in a dedicated environment."
        ) from _MCP_IMPORT_ERROR
    transport = os.getenv("MCP_TRANSPORT", "stdio")
    mcp.run(transport=transport)


if __name__ == "__main__":
    main()
