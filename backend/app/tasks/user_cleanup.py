from datetime import UTC, datetime, timedelta
from app.core.celery_app import celery_app
from app.db.database import sessionLocal
from app.models.user import User

@celery_app.task
def delete_stale_unverified_users() -> int:
    cutoff = datetime.now(UTC) - timedelta(hours=24)

    db = sessionLocal()

    try:
        deleted_count = (
            db.query(User)
            .filter(
                User.is_verified.is_(False),
                User.created_at < cutoff,
            )
            .delete(synchronize_session=False)
        )

        db.commit()

        return deleted_count

    finally:
        db.close()