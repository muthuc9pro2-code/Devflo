from datetime import UTC, datetime
import logging
from fastapi import APIRouter, BackgroundTasks, HTTPException, Depends
from fastapi.responses import JSONResponse
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from app.crud.user import create_user, get_user_by_email, authenticate_user, get_user_by_username
from app.db.database import get_db
from app.schemas.user import UserRegister, UserLogin, UserResponse, LoginResponse, RegisterResponse, ForgotPasswordRequest, ResetPasswordRequest, ResetPasswordStatusRequest, VerifyEmailRequest
from app.core.security import (
    create_email_verification_token,
    decode_email_verification_token,
    create_access_token,
    create_refresh_token,
    create_verification_handoff_token,
    decode_verification_handoff_token,
    VERIFICATION_HANDOFF_EXPIRE_MINUTES,
)
from app.services.email import send_verification_email, send_password_reset_email
from fastapi import Response, Request
import jwt
from app.core.security import ALGORITHM, SECRET_KEY, create_password_reset_token, hash_password, decode_password_reset_token, verify_password
from app.models.user import User
from app.core.config import Settings
from app.api.dependencies import get_current_verified_user
from resend.exceptions import ResendError

router = APIRouter(prefix="/auth", tags=["Authentication"])
VERIFICATION_HANDOFF_COOKIE_PATH = "/auth/verification-session"
logger = logging.getLogger(__name__)


def _set_verification_handoff_cookie(
    response: Response,
    email: str,
    token_version: int,
) -> None:
    response.set_cookie(
        key="verification_handoff",
        value=create_verification_handoff_token(email, token_version),
        httponly=True,
        secure=Settings.COOKIE_SECURE,
        samesite="lax",
        max_age=VERIFICATION_HANDOFF_EXPIRE_MINUTES * 60,
        path=VERIFICATION_HANDOFF_COOKIE_PATH,
    )


def _delete_verification_handoff_cookie(response: Response) -> None:
    response.delete_cookie(
        key="verification_handoff",
        path=VERIFICATION_HANDOFF_COOKIE_PATH,
        secure=Settings.COOKIE_SECURE,
        httponly=True,
        samesite="lax",
    )


def _delete_auth_cookies(response: Response) -> None:
    response.delete_cookie(key="access_token", path="/")
    response.delete_cookie(key="refresh_token", path="/")


def _refresh_rejection(detail: str) -> JSONResponse:
    response = JSONResponse(status_code=401, content={"detail": detail})
    _delete_auth_cookies(response)
    return response


def _send_verification_email_or_503(email: str, token: str) -> None:
    try:
        send_verification_email(email=email, token=token)
    except ResendError:
        logger.error("Verification email delivery failed")
        raise HTTPException(
            status_code=503,
            detail="Unable to send verification email. Please try again.",
        ) from None


def _send_password_reset_email_background(email: str, token: str) -> None:
    """Runs after the neutral /forgot-password response has already been
    sent (see BackgroundTasks usage below) - never inline in the request
    path. The email provider's round-trip latency (whether it succeeds,
    fails, or is skipped entirely because no account matched) must never
    be observable in how long the HTTP response itself takes: that timing
    is exactly the side channel an attacker could otherwise use to
    enumerate which email addresses have an account, defeating the whole
    point of the response's identical wording for every input."""
    try:
        send_password_reset_email(email=email, token=token)
    except ResendError:
        logger.error("Password reset email delivery failed")


@router.post("/register", response_model=RegisterResponse)
def register(
    user: UserRegister,
    response: Response,
    db: Session = Depends(get_db),
):
    existing_email = get_user_by_email(db, user.email)

    if existing_email:
        if existing_email.is_verified:
            raise HTTPException(
            status_code=409,
            detail="Email already registered"
        )

        if not verify_password(user.password, existing_email.hashed_password):
            raise HTTPException(
                status_code=409,
                detail="Email already registered"
            )

        existing_email.unverified_activity_at = datetime.now(UTC)
        db.commit()
        verification_token = create_email_verification_token(existing_email.email)
        _send_verification_email_or_503(existing_email.email, verification_token)
        _set_verification_handoff_cookie(
            response,
            existing_email.email,
            existing_email.token_version,
        )
        
        return {
            "message": "Verification email resent. Please verify email.",
            "email": existing_email.email
            }

    existing_user = get_user_by_username(db, user.username)

    if existing_user:
        raise HTTPException(
            status_code=409,
            detail="Username already taken"
        )

    try:
        created_user = create_user(db, user)
    except IntegrityError:
        db.rollback()
        raise HTTPException(
            status_code=409,
            detail="Registration conflict",
        ) from None
    verification_token = create_email_verification_token(created_user.email)
    _send_verification_email_or_503(created_user.email, verification_token)
    _set_verification_handoff_cookie(
        response,
        created_user.email,
        created_user.token_version,
    )

    return {
        "message": "Registration successful. Please verify email.",
        "email": created_user.email
    }


@router.post("/verify-email")
def verify_email(
    request: VerifyEmailRequest,
    db: Session = Depends(get_db),
):
    try:
        payload = decode_email_verification_token(request.token)
    except (jwt.PyJWTError, ValueError):
        raise HTTPException(
            status_code=400,
            detail="Invalid or expired verification link",
        )

    email = payload["sub"]

    user = (
        db.query(User)
        .filter(User.email == email)
        .with_for_update()
        .first()
    )

    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    if user.is_verified:
        raise HTTPException(
            status_code=400,
            detail="This verification link has already been used.",
        )

    user.is_verified = True
    db.commit()

    return {"message": "Email verified successfully"}


@router.post("/verification-session")
def complete_verification_session(
    request: Request,
    response: Response,
    db: Session = Depends(get_db),
):
    handoff_token = request.cookies.get("verification_handoff")

    if not handoff_token:
        raise HTTPException(
            status_code=401,
            detail="Verification handoff unavailable or expired",
        )

    try:
        payload = decode_verification_handoff_token(handoff_token)
    except (jwt.PyJWTError, ValueError):
        raise HTTPException(
            status_code=401,
            detail="Verification handoff unavailable or expired",
        )

    email = payload.get("sub")
    handoff_version = payload.get("ver")

    if not email:
        raise HTTPException(
            status_code=401,
            detail="Verification handoff unavailable or expired",
        )

    user = get_user_by_email(db, email)

    if not user:
        raise HTTPException(
            status_code=401,
            detail="Verification handoff unavailable or expired",
        )

    if handoff_version != user.token_version:
        raise HTTPException(
            status_code=401,
            detail="Verification handoff unavailable or expired",
        )

    if not user.is_verified:
        return {"status": "pending"}

    access_token = create_access_token(user.email, user.token_version)
    refresh_token = create_refresh_token(user.email, user.token_version)

    response.set_cookie(
        key="access_token",
        value=access_token,
        httponly=True,
        secure=Settings.COOKIE_SECURE,
        samesite="lax",
        max_age=Settings.ACCESS_TOKEN_EXPIRE_MINUTES * 60,
    )

    response.set_cookie(
        key="refresh_token",
        value=refresh_token,
        httponly=True,
        secure=Settings.COOKIE_SECURE,
        samesite="lax",
        max_age=Settings.REFRESH_TOKEN_EXPIRE_DAYS * 24 * 60 * 60,
    )

    _delete_verification_handoff_cookie(response)

    return {"status": "authenticated"}


@router.post("/login", response_model=LoginResponse)
def login(
    response: Response,
    user: UserLogin,
    db: Session = Depends(get_db),
):
    authenticated_user = authenticate_user(
        db=db, email=user.email, password=user.password
    )

    access_token = create_access_token(
        authenticated_user.email,
        authenticated_user.token_version,
    )
    refresh_token = create_refresh_token(
        authenticated_user.email,
        authenticated_user.token_version,
    )

    response.set_cookie(
        key="access_token",
        value=access_token,
        httponly=True,
        secure=Settings.COOKIE_SECURE,
        samesite="lax",
        max_age=Settings.ACCESS_TOKEN_EXPIRE_MINUTES * 60,
    )

    response.set_cookie(
        key="refresh_token",
        value=refresh_token,
        httponly=True,
        secure=Settings.COOKIE_SECURE,
        samesite="lax",
        max_age=Settings.REFRESH_TOKEN_EXPIRE_DAYS * 24 * 60 * 60,
    )

    _delete_verification_handoff_cookie(response)

    return {"message": "Login successful"}


@router.post("/refresh")
def refresh_token(request: Request, response: Response, db: Session = Depends(get_db)):
    refresh_token = request.cookies.get("refresh_token")

    if not refresh_token:
        return _refresh_rejection("Refresh token missing")

    try:
       payload = jwt.decode(
           refresh_token,
           SECRET_KEY,
           algorithms=[ALGORITHM],
           options={"require": ["sub", "type", "ver", "exp"]},
       )
    except jwt.InvalidTokenError:
        return _refresh_rejection("Invalid refresh token")
    
    if payload.get("type") != "refresh":
        return _refresh_rejection("Invalid refresh token")

    email = payload.get("sub")

    if not email:
        return _refresh_rejection("Invalid refresh token")

    user = get_user_by_email(db, email)

    if not user:
        return _refresh_rejection("Invalid refresh token")

    token_version = payload.get("ver")

    if type(token_version) is not int or token_version != user.token_version:
        return _refresh_rejection("Invalid refresh token")

    if not user.is_verified:
        return _refresh_rejection("Invalid refresh token")

    new_access_token = create_access_token(user.email, user.token_version)
    new_refresh_token = create_refresh_token(user.email, user.token_version)

    response.set_cookie(
        key="access_token",
        value=new_access_token,
        httponly=True,
        secure=Settings.COOKIE_SECURE,
        samesite="lax",
        max_age=Settings.ACCESS_TOKEN_EXPIRE_MINUTES * 60,
    )

    response.set_cookie(
        key="refresh_token",
        value=new_refresh_token,
        httponly=True,
        secure=Settings.COOKIE_SECURE,
        samesite="lax",
        max_age=Settings.REFRESH_TOKEN_EXPIRE_DAYS * 24 * 60 * 60,
    )

    return {
        "message": "Tokens refreshed successfully"
    }

@router.post("/forgot-password")
def forgot_password(
    request: ForgotPasswordRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    user = get_user_by_email(db, request.email)

    if user and user.is_verified:
        reset_token = create_password_reset_token(user.email, user.token_version)
        # Backgrounded so the email provider's latency never leaks into
        # this response's timing (see _send_password_reset_email_background).
        background_tasks.add_task(
            _send_password_reset_email_background,
            email=user.email,
            token=reset_token,
        )

    return {
        "message": (
            "If an account exists for this email, "
            "a password reset link has been sent."
        )
    }

@router.post("/reset-password")
def reset_password(
    request: ResetPasswordRequest,
    db: Session = Depends(get_db),
):
    try:
        payload = decode_password_reset_token(request.token)
    except (jwt.PyJWTError, ValueError):
        raise HTTPException(
            status_code=400,
            detail="Invalid or expired password reset token",
        )

    email = payload.get("sub")
    token_version = payload.get("ver")

    if not email or type(token_version) is not int:
        raise HTTPException(
            status_code=400,
            detail="Invalid or expired password reset token",
        )

    user = (
        db.query(User)
        .filter(User.email == email)
        .with_for_update()
        .first()
    )

    if not user:
        raise HTTPException(
            status_code=400,
            detail="Invalid password reset token",
        )

    if token_version != user.token_version:
        raise HTTPException(
            status_code=400,
            detail="Invalid or expired password reset token",
        )

    if verify_password(request.new_password, user.hashed_password):
        raise HTTPException(
            status_code=400,
            detail="Choose a password different from your current password.",
        )

    user.hashed_password = hash_password(request.new_password)
    user.token_version += 1

    db.commit()

    return {
        "message": "Password reset successfully"
    }

@router.post("/reset-password-status")
def reset_password_status(
    request: ResetPasswordStatusRequest,
    db: Session = Depends(get_db),
):
    """Read-only classification for a password-reset link, used purely to
    drive frontend UX (e.g. an already-used link should show the success
    screen instead of a password form). Never authorizes or performs a
    reset - POST /auth/reset-password independently re-validates
    everything from scratch."""
    try:
        payload = decode_password_reset_token(request.token)
    except (jwt.PyJWTError, ValueError):
        return {"status": "invalid"}

    email = payload.get("sub")
    token_version = payload.get("ver")

    if not email or type(token_version) is not int:
        return {"status": "invalid"}

    user = get_user_by_email(db, email)

    if not user:
        return {"status": "invalid"}

    if token_version == user.token_version:
        return {"status": "valid"}

    if token_version < user.token_version:
        return {"status": "used"}

    return {"status": "invalid"}

@router.post("/logout")
def logout(response: Response):
    _delete_auth_cookies(response)
    _delete_verification_handoff_cookie(response)

    return {
        "message": "Logged out successfully"
    }

@router.get("/me", response_model=UserResponse)
def get_me(
    response: Response,
    current_user: User = Depends(get_current_verified_user)
):
    response.headers["Cache-Control"] = "no-store"
    return current_user
