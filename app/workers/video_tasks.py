from __future__ import annotations

import logging
import os
import subprocess
import re
import time
from pathlib import Path
from celery.utils.log import get_task_logger

from app.workers.celery_tasks import celery
from app.views.customer import config

logger = get_task_logger(__name__)
log = logging.getLogger(__name__)

@celery.task(
    name="tasks.video.transcode_to_mp4",
    bind=True,
    max_retries=1,
    default_retry_delay=10,
    soft_time_limit=3600,
    time_limit=6000,
)
def transcode_to_mp4(self, input_path: str, fps: int, duration: float = 0.0) -> dict:
    """
    Transcode a WebM (or other) clip into an optimized H.264/AAC MP4.
    Reports progress to Celery (0..100).
 
    `duration` is the known clip length in seconds, sent by the frontend.
    This is ESSENTIAL: MediaRecorder WebM files frequently ship with NO
    duration in the container header, so ffprobe returns nothing — without
    this fallback the progress bar can never advance.
 
    On failure this RAISES, so Celery marks the task FAILURE and the status
    endpoint reports the error (returning a dict would look like SUCCESS).
    """
    logger.info("Starting transcode: %s (fps=%d, client_duration=%.2fs)",
                input_path, fps, duration)
 
    job_id = self.request.id or uuid.uuid4().hex
 
    input_file = Path(input_path)
    if not input_file.exists():
        raise FileNotFoundError(f"Input file {input_path} does not exist")
 
    config.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_file = config.OUTPUT_DIR / f"{job_id}.mp4"
 
    # ── 1. Determine total duration for progress tracking ──────────────────
    probed = 0.0
    try:
        result = subprocess.run(
            [config.FFPROBE_BIN, "-v", "error",
             "-show_entries", "format=duration",
             "-of", "default=noprint_wrappers=1:nokey=1", str(input_file)],
            capture_output=True, text=True, check=True,
        )
        txt = (result.stdout or "").strip()
        if txt and txt.lower() != "n/a":
            probed = float(txt)
    except Exception as exc:                                # noqa
        logger.warning("ffprobe could not read duration: %s", exc)
 
    # Prefer the probed value; fall back to the client-supplied duration.
    total = probed if probed > 0 else float(duration or 0.0)
    if total > 0:
        logger.info("Progress duration = %.2fs (%s)",
                    total, "probed" if probed > 0 else "client-supplied")
    else:
        logger.warning("No duration available — progress will jump 0 -> 100")
 
    # ── 2. Build the ffmpeg command (tuned for SPEED) ──────────────────────
    # 'veryfast' encodes several times quicker than 'medium' at a barely
    # perceptible quality cost for short social clips. '-threads 0' lets
    # x264 use every core. Override via the H264_PRESET env var if needed.
    preset = os.environ.get("H264_PRESET", getattr(config, "H264_PRESET", "") or "veryfast")
    crf = str(getattr(config, "H264_CRF", "23"))
 
    cmd = [
        config.FFMPEG_BIN,
        "-y",
        "-fflags", "+genpts",          # rebuild timestamps (MediaRecorder webm)
        "-i", str(input_file),
        "-r", str(fps),
        "-c:v", "libx264",
        "-preset", preset,
        "-crf", crf,
        "-threads", "0",               # use all available cores
        "-c:a", "aac",
        "-b:a", getattr(config, "AUDIO_BITRATE", "128k"),
        "-pix_fmt", "yuv420p",         # universal web/mobile compatibility
        "-movflags", "+faststart",     # moov atom up front -> instant playback
        "-progress", "pipe:1",
        str(output_file),
    ]
    logger.info("ffmpeg: %s", " ".join(cmd))
 
    self.update_state(state="STARTED", meta={"progress": 0})
 
    out_time_re = re.compile(r"(\d+):(\d+):(\d+)\.(\d+)")
    try:
        process = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            universal_newlines=True, bufsize=1,
        )
        for line in iter(process.stdout.readline, ""):
            line = line.strip()
            # -progress emits 'out_time=HH:MM:SS.ffffff' lines
            if line.startswith("out_time=") and total > 0:
                m = out_time_re.search(line)
                if m:
                    hh, mm, ss, frac = m.groups()
                    elapsed = int(hh) * 3600 + int(mm) * 60 + int(ss) + float("0." + frac)
                    progress = max(1, min(99, int((elapsed / total) * 100)))
                    self.update_state(state="PROGRESS", meta={"progress": progress})
            elif line == "progress=end":
                self.update_state(state="PROGRESS", meta={"progress": 99})
        process.wait()
 
        if process.returncode != 0:
            raise RuntimeError(f"ffmpeg exited with code {process.returncode}")
        if not output_file.exists() or output_file.stat().st_size == 0:
            raise RuntimeError("ffmpeg produced no output file")
 
    except Exception as exc:
        logger.error("Transcode failed: %s", exc)
        # Re-raise so Celery records FAILURE and the status endpoint surfaces it.
        raise
 
    logger.info("Transcode finished: %s (%.1f KB)",
                output_file, output_file.stat().st_size / 1024)
    return {"ok": True, "output_file": str(output_file), "progress": 100}
 
 
@celery.task(name="tasks.video.cleanup_old_videos")
def cleanup_old_videos():
    """Delete uploads and encoded MP4s older than RESULT_TTL_SECONDS (hourly)."""
    logger.info("Running video storage cleanup...")
    now = time.time()
    ttl = config.RESULT_TTL_SECONDS
    count_uploads = count_outputs = 0
 
    if config.UPLOAD_DIR.exists():
        for item in config.UPLOAD_DIR.iterdir():
            if item.is_file() and (now - item.stat().st_mtime) > ttl:
                try:
                    item.unlink()
                    count_uploads += 1
                except Exception as e:
                    logger.warning("Failed to delete upload %s: %s", item, e)
 
    if config.OUTPUT_DIR.exists():
        for item in config.OUTPUT_DIR.iterdir():
            if item.is_file() and item.suffix.lower() == ".mp4" \
                    and (now - item.stat().st_mtime) > ttl:
                try:
                    item.unlink()
                    count_outputs += 1
                except Exception as e:
                    logger.warning("Failed to delete output %s: %s", item, e)
 
    logger.info("Cleanup done. Removed %d uploads, %d outputs.",
                count_uploads, count_outputs)
    return {"ok": True, "removed_uploads": count_uploads, "removed_outputs": count_outputs}
    

@celery.task(name="tasks.video.cleanup_old_videos")
def cleanup_old_videos():
    """
    Deletes uploaded files and encoded mp4 results older than RESULT_TTL_SECONDS.
    Runs periodically (hourly).
    """
    logger.info("Running video storage cleanup...")
    
    now = time.time()
    ttl = config.RESULT_TTL_SECONDS
    
    count_uploads = 0
    count_outputs = 0
    
    # Cleanup uploads
    if config.UPLOAD_DIR.exists():
        for item in config.UPLOAD_DIR.iterdir():
            if item.is_file():
                age = now - item.stat().st_mtime
                if age > ttl:
                    try:
                        item.unlink()
                        count_uploads += 1
                    except Exception as e:
                        logger.warning("Failed to delete upload file %s: %s", item, e)
                        
    # Cleanup outputs
    if config.OUTPUT_DIR.exists():
        for item in config.OUTPUT_DIR.iterdir():
            if item.is_file() and item.suffix.lower() == ".mp4":
                age = now - item.stat().st_mtime
                if age > ttl:
                    try:
                        item.unlink()
                        count_outputs += 1
                    except Exception as e:
                        logger.warning("Failed to delete output file %s: %s", item, e)
                        
    logger.info("Cleanup complete. Removed %d uploads and %d outputs.", count_uploads, count_outputs)
    return {"ok": True, "removed_uploads": count_uploads, "removed_outputs": count_outputs}
