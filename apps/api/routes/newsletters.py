from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from packages.core.newsletter.service import NewsletterService


router = APIRouter(prefix="/api/newsletters", tags=["newsletters"])


# Pydantic models for request/response

class SubscribeRequest(BaseModel):
    sender_email: str
    sender_name: Optional[str] = None


class UpdateConfigRequest(BaseModel):
    schedule: Optional[str] = None
    max_per_digest: Optional[int] = None
    auto_generate: Optional[bool] = None


class GenerateDigestRequest(BaseModel):
    since_date: Optional[str] = None
    max_newsletters: int = 20


def _service() -> NewsletterService:
    return NewsletterService()


# Routes

@router.get("/detect")
def detect_newsletters(
    limit: int = 50,
    days_back: int = 7
) -> List[Dict[str, Any]]:
    """Detect potential newsletters in Gmail."""
    try:
        return _service().detect_newsletters(limit=limit, days_back=days_back)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/subscriptions")
def list_subscriptions() -> List[Dict[str, Any]]:
    """List all newsletter subscriptions."""
    return _service().list_subscriptions()


@router.post("/subscriptions")
def subscribe_newsletter(request: SubscribeRequest) -> Dict[str, Any]:
    """Subscribe to a newsletter."""
    return _service().subscribe_newsletter(
        sender_email=request.sender_email,
        sender_name=request.sender_name
    )


@router.delete("/subscriptions/{sender_email}")
def unsubscribe_newsletter(sender_email: str) -> Dict[str, Any]:
    """Unsubscribe from a newsletter."""
    return _service().unsubscribe_newsletter(sender_email=sender_email)


@router.patch("/subscriptions/{sender_email}/pause")
def pause_subscription(sender_email: str) -> Dict[str, Any]:
    """Pause a newsletter subscription."""
    return _service().pause_subscription(sender_email=sender_email)


@router.patch("/subscriptions/{sender_email}/resume")
def resume_subscription(sender_email: str) -> Dict[str, Any]:
    """Resume a newsletter subscription."""
    return _service().resume_subscription(sender_email=sender_email)


@router.get("/digests")
def list_digests(limit: int = 10) -> List[Dict[str, Any]]:
    """List recent newsletter digests."""
    return _service().list_digests(limit=limit)


@router.get("/digests/{digest_id}")
def get_digest(digest_id: str) -> Dict[str, Any]:
    """Get a specific digest with all newsletter summaries."""
    digest = _service().get_digest(digest_id=digest_id)
    if not digest:
        raise HTTPException(status_code=404, detail="Digest not found")
    return digest


@router.post("/digests/generate")
def generate_digest(request: GenerateDigestRequest) -> Dict[str, Any]:
    """Generate a new newsletter digest."""
    result = _service().generate_digest(
        since_date=request.since_date,
        max_newsletters=request.max_newsletters
    )
    if result.get("status") == "error":
        raise HTTPException(status_code=400, detail=result.get("message"))
    return result


@router.get("/config")
def get_config() -> Dict[str, Any]:
    """Get newsletter digest configuration."""
    return _service().get_digest_config()


@router.patch("/config")
def update_config(request: UpdateConfigRequest) -> Dict[str, Any]:
    """Update newsletter digest configuration."""
    return _service().update_digest_config(
        schedule=request.schedule,
        max_per_digest=request.max_per_digest,
        auto_generate=request.auto_generate
    )
