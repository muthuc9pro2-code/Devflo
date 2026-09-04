from datetime import UTC, datetime
from sqlalchemy.orm import Session
from app.core.security import password_hash, verify_password
from app.models.user import User
from app.schemas.user import UserRegister
from fastapi import HTTPException

_DUMMY_PASSWORD_HASH = (
    "$argon2id$v=19$m=65536,t=3,p=4$z3wunkUjnwwhVBPB3kF/RQ$"
    "pDFSbjnTzo2R8XyjhvHB/FOCe5PC80NyWC9qk8d6f8k"
)

def create_user(db: Session, user: UserRegister) -> User:
    db_user = User(
        username=user.username,
        email=user.email,
        hashed_password=password_hash.hash(user.password),
        unverified_activity_at=datetime.now(UTC),
    )

    db.add(db_user)
    db.commit()
    db.refresh(db_user)

    return db_user

def get_user_by_email(db: Session, email: str) -> User | None:
    return db.query(User).filter(User.email == email).first()

def get_user_by_username(db: Session, username: str) -> User | None:
    return db.query(User).filter(User.username == username).first()

def authenticate_user(
        db: Session,
        email: str,
        password: str
) -> User:
    user = get_user_by_email(db, email)

    if not user:
        verify_password(password, _DUMMY_PASSWORD_HASH)
        raise HTTPException(
            status_code=401,
            detail="Invalid email or password"
        )

    if not verify_password(
        password,
        user.hashed_password
    ):
        raise HTTPException(
            status_code=401,
            detail="Invalid email or password"
        )

    if not user.is_verified:
        raise HTTPException(
            status_code=403,
            detail="please verify your email before logging in."
        )

    return user
