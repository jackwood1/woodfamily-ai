from __future__ import annotations

from typing import Optional

import httpx


def fetch_pdf(url: str, timeout: int = 20) -> bytes:
    response = httpx.get(url, timeout=timeout)
    response.raise_for_status()
    content = response.content
    content_type = response.headers.get("content-type", "")
    if not _looks_like_pdf(content, content_type):
        preview = content[:200].decode("utf-8", errors="replace")
        raise RuntimeError(
            "expected_pdf_response",
            {
                "url": url,
                "content_type": content_type,
                "preview": preview,
            },
        )
    return content


def fetch_html(url: Optional[str], timeout: int = 20) -> Optional[str]:
    if not url:
        return None
    response = httpx.get(url, timeout=timeout)
    response.raise_for_status()
    return response.text


def safe_fetch_pdf(url: Optional[str]) -> Optional[bytes]:
    if not url:
        return None
    return fetch_pdf(url)


def _looks_like_pdf(content: bytes, content_type: str) -> bool:
    if content.startswith(b"%PDF"):
        return True
    lowered = content_type.lower()
    if "application/pdf" in lowered:
        # Guard against HTML or error bodies mislabeled as PDF
        return False
    return False
