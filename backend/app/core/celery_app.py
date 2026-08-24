import os

from celery import Celery

celery_app = Celery(
    "devflo",
    broker="redis://localhost:6379/0",
    backend="redis://localhost:6379/1",
    include=["app.tasks.analysis",
             "app.tasks.user_cleanup"
             ]
)

celery_app.conf.update(
    task_track_started=True,
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    worker_prefetch_multiplier=1,
    # Global cap on how many tasks this worker runs at once - across ALL
    # analyses/artifacts/users, not per-analysis. Sized for the current
    # 2 vCPU EC2 target so bounded cross-artifact concurrency
    # (app/tasks/analysis.py) can never oversubscribe the box; multiple
    # concurrent analyses simply share these same slots. Override via
    # CELERY_WORKER_CONCURRENCY for other environments.
    worker_concurrency=int(os.environ.get("CELERY_WORKER_CONCURRENCY", "2")),
    beat_schedule={
        "delete-stale-unverified-users": {
        "task": "app.tasks.user_cleanup.delete_stale_unverified_users",
        "schedule": 60 * 60,
        },
        # Low-cadence orphan/stale-analysis recovery (see
        # app/tasks/analysis.py:recover_stale_analyses) - 60 seconds is
        # frequent enough to reclaim a genuinely-orphaned analysis
        # reasonably promptly after it crosses the (larger, 300 second)
        # staleness threshold, while staying negligible DB load: one
        # bounded, indexed query plus at most _RECOVERY_SCAN_BATCH_LIMIT
        # small conditional UPDATEs every tick.
        "recover-stale-analyses": {
        "task": "app.tasks.analysis.recover_stale_analyses",
        "schedule": 60,
        },
    },
    timezone="UTC"
)




