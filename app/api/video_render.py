"""
video_routes.py — the HTTP API blueprint.

    POST /api/video/upload          multipart: "video" (+ optional "fps", "duration")
                                    -> { "job_id": "<celery-task-id>" }

    GET  /api/video/status/<job_id> -> { "state": ..., "progress": 0-100,
                                         "error": null | "..." }

    GET  /api/video/download/<job_id>
                                    -> the encoded MP4 (attachment)

The Celery task id is used directly as the job id, so no database is needed:
the output file is always `<job_id>.mp4` in OUTPUT_DIR.

NOTE on "duration": MediaRecorder WebM blobs frequently carry NO duration in
their container header, so the server's ffprobe sees nothing and the encode
progress bar would be stuck at 0%. The frontend already knows the exact clip
length, so it sends it as a form field and the task uses it as a fallback.
"""

import uuid
from pathlib import Path

from celery.result import AsyncResult
from flask import Blueprint, jsonify, request, send_file, abort
from werkzeug.utils import secure_filename

from app.views.customer import config
from app.workers.celery_app import celery_app as celery
from app.workers.video_tasks import transcode_to_mp4

video_bp = Blueprint("video", __name__, url_prefix="/api/video")


# ── POST /api/video/upload ──────────────────────────────────────────────
@video_bp.route("/upload", methods=["POST"])
def upload():
    """Accept a recorded WebM clip and queue an MP4 encode job."""
    if "video" not in request.files:
        return jsonify(error="no 'video' file in request"), 400

    file = request.files["video"]
    if not file.filename:
        return jsonify(error="empty filename"), 400

    ext = Path(secure_filename(file.filename)).suffix.lower() or ".webm"
    if ext not in config.ALLOWED_EXTENSIONS:
        return jsonify(error=f"unsupported file type: {ext}"), 415

    # fps — optional string form field.
    try:
        fps = int(float(request.form.get("fps", 30)))
        fps = max(10, min(60, fps))
    except (TypeError, ValueError):
        fps = 30

    # duration — the known clip length in seconds (progress-bar fallback).
    try:
        duration = float(request.form.get("duration", 0) or 0)
        duration = max(0.0, min(600.0, duration))
    except (TypeError, ValueError):
        duration = 0.0

    # One id shared by the upload filename, the Celery task, and <job_id>.mp4.
    job_id = uuid.uuid4().hex
    saved_path = config.UPLOAD_DIR / f"{job_id}{ext}"
    file.save(saved_path)

    transcode_to_mp4.apply_async(
        args=[str(saved_path), fps, duration],
        task_id=job_id,
    )

    return jsonify(job_id=job_id), 202


# ── GET /api/video/status/<job_id> ──────────────────────────────────────
@video_bp.route("/status/<job_id>", methods=["GET"])
def status(job_id):
    """Report encode progress, shaped for the frontend poll."""
    res = AsyncResult(job_id, app=celery)
    state = res.state                       # PENDING/STARTED/PROGRESS/SUCCESS/FAILURE
    progress = 0
    error = None

    if state in ("PROGRESS", "STARTED"):
        info = res.info if isinstance(res.info, dict) else {}
        progress = int(info.get("progress", 0))
    elif state == "SUCCESS":
        progress = 100
    elif state == "FAILURE":
        # res.info holds the raised exception.
        error = str(res.info) if res.info else "encode failed"

    return jsonify(state=state, progress=progress, error=error)


# ── GET /api/video/download/<job_id> ────────────────────────────────────
@video_bp.route("/download/<job_id>", methods=["GET"])
def download(job_id):
    """Stream the finished MP4 back to the browser."""
    safe_id = secure_filename(job_id)       # guards against path traversal
    mp4_path = config.OUTPUT_DIR / f"{safe_id}.mp4"

    if not mp4_path.exists():
        res = AsyncResult(safe_id, app=celery)
        if res.state == "FAILURE":
            abort(410, description="encode failed — nothing to download")
        abort(404, description="not ready or unknown job id")

    return send_file(
        mp4_path,
        mimetype="video/mp4",
        as_attachment=True,
        download_name=f"odds-video-{safe_id[:8]}.mp4",
        conditional=True,                   # enables range requests / scrubbing
    )