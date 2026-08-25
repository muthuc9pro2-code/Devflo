import os
from app.core.config import Settings
from celery import Celery

celery_app = Celery(
    "devflo",
    broker=Settings.REDIS_BROKER_URL,
    backend=Settings.REDIS_RESULT_BACKEND_URL,
    include=["app.tasks.analysis",
             "app.tasks.user_cleanup"
             ]
)

celery_app.conf.update(
    task_track_started=True,
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    worker_prefetch_multiplier=1,
    worker_concurrency=int(os.environ.get("CELERY_WORKER_CONCURRENCY", "2")),
    beat_schedule={
        "delete-stale-unverified-users": {
        "task": "app.tasks.user_cleanup.delete_stale_unverified_users",
        "schedule": 60 * 60,
        },
        "recover-stale-analyses": {
        "task": "app.tasks.analysis.recover_stale_analyses",
        "schedule": 60,
        },
    },
    timezone="UTC"
)




