from app.services.email_service import send_ses_email

def send_verification_email(email: str, token: str) -> None:
    verification_link = (
        f"http://localhost:8000/auth/verify-email?token={token}"
    )
    send_ses_email(
        to_email=email,
        verification_url=verification_link,
    )

