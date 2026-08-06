def send_verification_email(email: str, token: str) -> None:
    verification_link = (
        f"http://localhost:8000/auth/verify-email?token={token}"
    )

    print(f"Verification email to {email}")
    print(verification_link)
