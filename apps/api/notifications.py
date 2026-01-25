from __future__ import annotations

import os
import smtplib
from email.message import EmailMessage
from typing import Optional


def _smtp_config() -> dict:
    return {
        "host": os.getenv("SMTP_HOST", ""),
        "port": int(os.getenv("SMTP_PORT", "587")),
        "user": os.getenv("SMTP_USER", ""),
        "password": os.getenv("SMTP_PASSWORD", ""),
        "from_email": os.getenv("SMTP_FROM", ""),
        "use_tls": os.getenv("SMTP_USE_TLS", "true").lower() == "true",
    }


def send_email(to_email: str, subject: str, body: str) -> None:
    config = _smtp_config()
    if not config["host"] or not config["from_email"]:
        raise RuntimeError("SMTP is not configured. Set SMTP_HOST and SMTP_FROM.")

    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = config["from_email"]
    message["To"] = to_email
    message.set_content(body)

    with smtplib.SMTP(config["host"], config["port"]) as server:
        if config["use_tls"]:
            server.starttls()
        if config["user"]:
            server.login(config["user"], config["password"])
        server.send_message(message)


def send_sms_via_email(
    phone: str, gateway_domain: str, subject: str, body: str
) -> None:
    to_email = f"{phone}@{gateway_domain}"
    send_email(to_email, subject, body)
