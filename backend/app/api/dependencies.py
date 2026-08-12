from fastapi import Depends, HTTPException, Request
from sqlalchemy.orm import Session
import jwt
from app.db.database import get_db
from app.models.user import User
from app.crud.user import get_user_by_email
from app.core.security import SECRET_KEY, ALGORITHM

def get_current_user(
        request: Request,
        db: Session = Depends(get_db)
) -> User:

    access_token = request.cookies.get("access_token")

    if not access_token:
        raise HTTPException(
            status_code=401,
            detail="Not authenticate"
        )

    try:
        payload = jwt.decode(
            access_token,
            SECRET_KEY,
            algorithms=[ALGORITHM]
        )
    except jwt.InvalidTokenError:
        raise HTTPException(
            status_code=401,
            detail="Invalid access token"
        )

    if payload.get("type") != "access":
        raise HTTPException(
            status_code=401,
            detail="Invalid token type"
        )

    email = payload.get("sub")

    if not email:
        raise HTTPException(
            status_code=401,
            detail="Invalid access token"
        )

    user = get_user_by_email(db, email)

    if not user:
        raise HTTPException(
            status_code=401,
            detail="User not found"
        )

    return user

def get_current_verified_user(
        current_user: User = Depends(get_current_user)
        ) -> User:

    if not current_user.is_verified:
        raise HTTPException(
            status_code=403,
            detail="Email not verified"
        )

    return current_user

