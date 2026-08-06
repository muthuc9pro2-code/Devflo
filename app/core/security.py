from pwdlib import PasswordHash
from datetime import datetime, timedelta, UTC
import jwt

password_hash = PasswordHash.recommended()

SECRET_KEY = "change_this_later"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30
REFRESH_TOKEN_EXPIRE_DAYS = 7

def create_email_verification_token(email: str) -> str:
    payload = {
        "sub": email,
        "type": "email_verification",
        "exp": datetime.now(UTC) + timedelta(hours=24)
    }

    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)

def decode_email_verification_token(token: str) -> dict:
    payload = jwt.decode(
        token,
        SECRET_KEY,
        algorithms=[ALGORITHM]
    )

    if payload.get("type") != "email_verification":
        raise ValueError("Invalid token type")

    return payload

def verify_password(
    plain_password: str,
    hashed_password: str
) -> bool:
    return password_hash.verify(
        plain_password,
        hashed_password
    )

def create_access_token(email: str) -> str:
    payload = {
        "sub": email,
        "type": "access",
        "exp": datetime.now(UTC) + timedelta(hours=6)
    }

    return jwt.encode(
        payload,
        SECRET_KEY,
        algorithm=ALGORITHM
    )

def create_refresh_token(email: str) -> str:
    payload = {
        "sub": email,
        "type": "refresh",
        "exp": datetime.now(UTC) + timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS)
    }

    return jwt.encode(
        payload,
        SECRET_KEY,
        algorithm=ALGORITHM
    )

