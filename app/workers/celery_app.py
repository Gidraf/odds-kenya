# app/workers/celery_app.py
from __future__ import annotations
import os
from app.extensions import celery

def make_celery() -> Celery:
    broker  = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    backend = os.getenv("REDIS_URL", "redis://localhost:6379/0")

    app = celery
    app.conf.update(
        task_serializer          = "json",
        result_serializer        = "json",
        accept_content           = ["json"],
        task_track_started       = True,
        task_acks_late           = True,
        worker_prefetch_multiplier = 4,
        task_soft_time_limit     = 30,
        task_time_limit          = 40,
        result_expires           = 300,
        broker_connection_retry_on_startup = True,

        # ─── TASK ROUTES (queue assignments) ─────────────────────────────────
        task_routes = {
            "harvest.bookmaker_sport":             {"queue": "harvest"},
            "harvest.all_upcoming":                {"queue": "harvest"},
            "harvest.merge_broadcast":             {"queue": "harvest"},
            "harvest.value_bets":                  {"queue": "ev_arb"},
            "harvest.cleanup":                     {"queue": "harvest"},
            "tasks.sp.harvest_sport":              {"queue": "harvest"},
            "tasks.sp.harvest_all_upcoming":       {"queue": "harvest"},
            "tasks.sp.harvest_all_live":           {"queue": "live"},
            "tasks.sp.cross_bk_enrich":           {"queue": "harvest"},
            "tasks.sp.enrich_analytics":          {"queue": "harvest"},
            "tasks.sp.get_match_analytics":       {"queue": "harvest"},
            "tasks.sp.harvest_page":              {"queue": "harvest"},
            "tasks.sp.merge_pages":               {"queue": "results"},
            "tasks.sp.harvest_sport_paged":       {"queue": "harvest"},
            "tasks.sp.harvest_all_paged":         {"queue": "harvest"},
            "tasks.bt_od.harvest_sport":          {"queue": "harvest"},
            "tasks.bt_od.harvest_all":            {"queue": "harvest"},
            "tasks.bt.harvest_sport":              {"queue": "harvest"},
            "tasks.bt.harvest_page":              {"queue": "harvest"},
            "tasks.bt.merge_pages":               {"queue": "results"},
            "tasks.bt.harvest_sport_paged":       {"queue": "harvest"},
            "tasks.bt.harvest_all_paged":         {"queue": "harvest"},
            "tasks.bt.enrich_sport":              {"queue": "harvest"},
            "tasks.bt.harvest_all_upcoming":       {"queue": "harvest"},
            "tasks.bt.harvest_all_live":           {"queue": "live"},
            "tasks.od.harvest_sport":              {"queue": "harvest"},
            "tasks.od.harvest_date_chunk":        {"queue": "harvest"},
            "tasks.od.merge_pages":               {"queue": "results"},
            "tasks.od.harvest_sport_paged":       {"queue": "harvest"},
            "tasks.od.harvest_all_paged":         {"queue": "harvest"},
            "tasks.od.harvest_all_upcoming":       {"queue": "harvest"},
            "tasks.od.harvest_all_live":           {"queue": "live"},
            "tasks.b2b.harvest_sport":             {"queue": "harvest"},
            "tasks.b2b.harvest_all_upcoming":      {"queue": "harvest"},
            "tasks.b2b.harvest_all_live":          {"queue": "live"},
            "tasks.b2b_page.harvest_page":         {"queue": "harvest"},
            "tasks.b2b_page.harvest_all_upcoming": {"queue": "harvest"},
            "tasks.b2b_page.harvest_all_live":     {"queue": "live"},
            "tasks.b2b.harvest_bk_sport":         {"queue": "harvest"},
            "tasks.b2b.merge_sport":              {"queue": "results"},
            "tasks.b2b.harvest_all_paged":        {"queue": "harvest"},
            "tasks.b2b.harvest_all_live_paged":   {"queue": "live"},
            "tasks.sbo.harvest_sport":             {"queue": "harvest"},
            "tasks.sbo.harvest_all_upcoming":      {"queue": "harvest"},
            "tasks.sbo.harvest_all_live":          {"queue": "live"},
            "tasks.sp.poll_all_event_details":     {"queue": "live"},
            "tasks.ops.compute_ev_arb":            {"queue": "ev_arb"},
            "tasks.ops.update_match_results":      {"queue": "results"},
            "tasks.ops.dispatch_notifications":    {"queue": "notify"},
            "tasks.ops.publish_ws_event":          {"queue": "notify"},
            "tasks.ops.health_check":              {"queue": "default"},
            "tasks.ops.healthcheck":               {"queue": "default"},
            "tasks.ops.expire_subscriptions":      {"queue": "default"},
            "tasks.ops.cache_finished_games":      {"queue": "results"},
            "tasks.ops.send_async_email":          {"queue": "notify"},
            "tasks.ops.send_message":              {"queue": "notify"},
            "tasks.ops.persist_combined_batch":    {"queue": "results"},
            "tasks.ops.persist_all_sports":        {"queue": "results"},
            "tasks.ops.build_health_report":       {"queue": "default"},
            "tasks.ops.publish_bk_snapshot":       {"queue": "harvest"},
            "tasks.align.sport":                   {"queue": "results"},
            "tasks.align.all_sports":              {"queue": "results"},
            "tasks.harvest.all_paged":             {"queue": "harvest"},
            "tasks.ops.beat.harvest_all_paged":    {"queue": "harvest"},
            "tasks.ops.beat.b2b_live":             {"queue": "harvest"},
            "tasks.ops.beat.alignment":            {"queue": "results"},
            "tasks.ops.beat.prune":                {"queue": "default"},
        },

        # ─── TASK MODULES TO LOAD ────────────────────────────────────────────
        include = [
            "app.workers.tasks_ops",
            "app.workers.tasks_upcoming",
            "app.workers.tasks_live",
            "app.workers.tasks_market_align",
            "app.workers.tasks_bt_od",
            "app.workers.tasks_harvest_pages",
            "app.workers.tasks_harvest_b2b",
            "app.workers.tasks_align",
        ],
    )
    return app

celery_app = make_celery()