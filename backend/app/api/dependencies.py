from fastapi import Depends, HTTPException, Request
from sqlalchemy.orm import Session
import jwt
from app.db.database import get_db, sessionLocal
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


def get_current_verified_user_id_for_stream(request: Request) -> int:
    """Same JWT/verification rules as get_current_user/get_current_verified_user
    above (reused directly, not duplicated) - but for a potentially
    minutes-long SSE connection, only the verified user's id is needed for
    the ownership check. Using the normal get_db-injected Session dependency
    here would keep an ORM User + its auth DB Session alive for the whole
    stream; this opens and closes its own short-lived Session instead and
    returns only a plain int."""
    db = sessionLocal()
    try:
        user = get_current_user(request=request, db=db)
        verified_user = get_current_verified_user(user)
        return verified_user.id
    finally:
        db.close()

