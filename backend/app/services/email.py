from app.core.config import Settings
from app.services.email_service import (
    send_password_reset_email_message,
    send_verification_email_message,
)

def send_verification_email(email: str, token: str) -> None:
    verification_link = (
        f"{Settings.FRONTEND_URL}/verify-email#token={token}"
    )
    send_verification_email_message(
        to_email=email,
        verification_url=verification_link,
    )

def send_password_reset_email(email: str, token: str) -> None:
    reset_link = (
        f"{Settings.FRONTEND_URL}/reset-password#token={token}"
    )
    send_password_reset_email_message(
        to_email=email,
        reset_url=reset_link,
    )
