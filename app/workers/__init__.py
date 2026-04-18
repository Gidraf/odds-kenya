from celery import Celery


celery = Celery(
    "odds_harvester",
    broker  = "redis://localhost:6382/0",
    backend = "redis://localhost:6382/1",
)