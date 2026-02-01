from __future__ import annotations

from typing import Optional

import httpx


def fetch_pdf(url: str, timeout: int = 20) -> bytes:
    response = httpx.get(url, timeout=timeout)
    response.raise_for_status()
    return response.content


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
