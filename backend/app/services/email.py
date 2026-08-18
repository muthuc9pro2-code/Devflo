from app.core.config import Settings
from app.services.email_service import send_ses_email

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

