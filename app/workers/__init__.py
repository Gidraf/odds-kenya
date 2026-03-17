from celery import Celery


celery = Celery(
    "odds_harvester",
    broker  = "redis://localhost:6379/0",
    backend = "redis://localhost:6379/1",
)