import logging

from fastapi import APIRouter, HTTPException
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from app.db.database import engine

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/health")
def health_check():
    """Process liveness only. Never depend on DB/Redis availability."""
    return {"status": "healthy"}


@router.get("/ready")
def readiness_check():
    """Ready to serve DB-backed application requests."""
    try:
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
    except SQLAlchemyError:
        logger.warning("Readiness check failed: database unavailable")
        raise HTTPException(
            status_code=503,
            detail="Database unavailable",
        ) from None
    return {"status": "ready"}
