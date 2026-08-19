from app.core.config import Settings
from app.services.email_service import send_ses_email, send_ses_password_reset_email

def send_verification_email(email: str, token: str) -> None:
    # Points at the frontend's /verify-email page (which calls the backend
    # /auth/verify-email API itself and then enters the app already
    # authenticated), not directly at the backend - environment-specific via
    # the existing FRONTEND_URL setting instead of a hardcoded host.
    verification_link = (
        f"{Settings.FRONTEND_URL}/verify-email?token={token}"
    )
    send_ses_email(
        to_email=email,
        verification_url=verification_link,
    )

def send_password_reset_email(email: str, token: str) -> None:
    # Frontend's /reset-password page, environment-specific via FRONTEND_URL
    # (never a hardcoded host) - same convention as send_verification_email
    # above, deliberately a distinct SES template so a reset never reads as
    # the "Verify your Devflo account" email.
    reset_link = (
        f"{Settings.FRONTEND_URL}/reset-password?token={token}"
    )
    send_ses_password_reset_email(
        to_email=email,
        reset_url=reset_link,
    )

