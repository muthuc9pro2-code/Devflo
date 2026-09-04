import resend
from app.core.config import Settings

resend.api_key = Settings.RESEND_API_KEY

def send_verification_email_message(to_email: str, verification_url: str) -> None:
    resend.Emails.send(
        {
            "from": Settings.EMAIL_FROM,
            "to": [to_email],
            "subject": "Verify your Devflo account",
            "text": (
                "Welcome to Devflo.\n\n"
                "Verify your email address using this link:\n"
                f"{verification_url}\n\n"
                "If you did not create this account, ignore this email."
            ),
        }
    )

def send_password_reset_email_message(to_email: str, reset_url: str) -> None:
    resend.Emails.send(
        {
            "from": Settings.EMAIL_FROM,
            "to": [to_email],
            "subject": "Reset your Devflo password",
            "text": (
                "We received a request to reset your Devflo password.\n\n"
                "Reset your password using this link:\n"
                f"{reset_url}\n\n"
                "If you did not request a password reset, ignore this email."
            ),
        }
    )
