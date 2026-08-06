from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy.orm import Session
from app.crud.user import create_user, get_user_by_email, authenticate_user, get_user_by_username
from app.db.database import get_db
from app.schemas.user import UserRegister, UserLogin, UserResponse, LoginResponse, RegisterResponse
from app.core.security import (
    create_email_verification_token,
    decode_email_verification_token,
    create_access_token,
    create_refresh_token,
)
from app.services.email import send_verification_email
from fastapi import Response, Request
import jwt
from app.core.security import ALGORITHM, SECRET_KEY
from app.models.user import User
from app.api.dependencies import get_current_verified_user

router = APIRouter(prefix="/auth", tags=["Authentication"])


@router.post("/register", response_model=RegisterResponse)
def register(
    user: UserRegister,
    db: Session = Depends(get_db),
):
    existing_email = get_user_by_email(db, user.email)

    if existing_email:
        raise HTTPException(
            status_code=409,
            detail="Email already register",
        )

    existing_user = get_user_by_username(db, user.username)

    if existing_user:
        raise HTTPException(
            status_code=409,
            detail="Username already taken"
        )

    verification_token = create_email_verification_token(user.email)

    send_verification_email(email=user.email, token=verification_token)

    created_user = create_user(db, user)

    return {
        "message": "Resgistration successful. Please verify email.",
        "email": created_user.email
    }


@router.get("/verify-email")
def verify_email(token: str, db: Session = Depends(get_db)):
    payload = decode_email_verification_token(token)
    email = payload["sub"]

    user = get_user_by_email(db, email)

    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    if user.is_verified:
        return {"message": "Email already verified"}

    user.is_verified = True

    db.commit()

    return {"message": "Email verified successfully"}


@router.post("/login", response_model=LoginResponse)
def login(
    response: Response,
    user: UserLogin,
    db: Session = Depends(get_db),
):
    authenticated_user = authenticate_user(
        db=db, email=user.email, password=user.password
    )

    access_token = create_access_token(authenticated_user.email)
    refresh_token = create_refresh_token(authenticated_user.email)

    response.set_cookie(
        key="access_token",
        value=access_token,
        httponly=True,
        secure=False,
        samesite="lax",
        max_age=30 * 60,
    )

    response.set_cookie(
        key="refresh_token",
        value=refresh_token,
        httponly=True,
        secure=False,
        samesite="lax",
        max_age=7 * 24 * 60 * 60,
    )

    return {"message": "Login successful", "user": authenticated_user}


@router.post("/refresh")
def refresh_token(request: Request, response: Response, db: Session = Depends(get_db)):
    refresh_token = request.cookies.get("refresh_token")

    if not refresh_token:
        raise HTTPException(status_code=401, detail="Refresh token missing")

    try:
       payload = jwt.decode(refresh_token, SECRET_KEY, algorithms=[ALGORITHM])
    except jwt.InvalidTokenError:
        raise HTTPException(
            status_code=401,
            detail="Invalid refresh token",
        )
    
    if payload.get("type") != "refresh":
        raise HTTPException(status_code=401, detail="Invalid refresh token")

    email = payload.get("sub")

    if not email:
        raise HTTPException(
            status_code=401,
            detail="Invalid refresh token",
        )

    user = get_user_by_email(db, email)

    if not user:
        raise HTTPException(
            status_code=401,
            detail="User not found"
        )

    new_access_token = create_access_token(email)
    new_refresh_token = create_refresh_token(email)

    response.set_cookie(
        key="access_token",
        value=new_access_token,
        httponly=True,
        secure=False,
        samesite="lax",
        max_age=30 * 60,
    )

    response.set_cookie(
        key="refresh_token",
        value=new_refresh_token,
        httponly=True,
        secure=False,
        samesite="lax",
        max_age=7 * 24 * 60 * 60,
    )

    return {
        "message": "Tokens refreshed successfully"
    }

@router.post("/logout")
def logout (response: Response):
    response.delete_cookie("access_token")
    response.delete_cookie("refresh_token")

    return {
        "messsage": "Logged out successfully"
    }

@router.get("/me", response_model=UserResponse)
def get_me(
    current_user: User = Depends(get_current_verified_user)
):
    return {
        "username": current_user.username,
        "email": current_user.email
    }



