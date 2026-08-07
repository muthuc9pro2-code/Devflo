from celery import Celery

celery_app = Celery(
    "devflo",
    broker="redis://localhost:6379/0",
    backend="redis://localhost:6379/1",
    include=["app.tasks.analysis"]
)

celery_app.conf.update(
    task_track_started=True
)




