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
    time_limit=6000
)
def transcode_to_mp4(self, input_path: str, fps: int) -> dict:
    """
    Transcode a WebM or other video format into an optimized MP4 (H.264/AAC) file.
    Reports progress back to Celery (0 to 100).
    """
    logger.info("Starting video transcode: %s (fps=%d)", input_path, fps)
    
    # job_id is the celery task id
    job_id = self.request.id
    if not job_id:
        import uuid
        job_id = uuid.uuid4().hex
        
    input_file = Path(input_path)
    if not input_file.exists():
        err_msg = f"Input file {input_path} does not exist"
        logger.error(err_msg)
        return {"ok": False, "error": err_msg}
        
    output_file = config.OUTPUT_DIR / f"{job_id}.mp4"
    
    # Ensure parent directories exist
    config.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    # 1. Get input video duration using ffprobe
    duration = 0.0
    try:
        cmd_probe = [
            config.FFPROBE_BIN,
            "-v", "error",
            "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1",
            str(input_file)
        ]
        result = subprocess.run(cmd_probe, capture_output=True, text=True, check=True)
        duration_str = result.stdout.strip()
        if duration_str:
            duration = float(duration_str)
            logger.info("Video duration: %.2fs", duration)
    except Exception as exc:
        logger.warning("Could not probe video duration: %s", exc)
        
    # 2. Run ffmpeg and track progress
    # We want to transcode to H.264 MP4 with the config preset and crf
    cmd_ffmpeg = [
        config.FFMPEG_BIN,
        "-y",
        "-i", str(input_file),
        "-r", str(fps),
        "-c:v", "libx264",
        "-preset", config.H264_PRESET,
        "-crf", config.H264_CRF,
        "-c:a", "aac",
        "-b:a", config.AUDIO_BITRATE,
        "-pix_fmt", "yuv420p",        # essential for general web compatibility
        "-movflags", "+faststart",    # optimizes for streaming/progressive download
        "-progress", "pipe:1",
        str(output_file)
    ]
    
    logger.info("Executing ffmpeg: %s", " ".join(cmd_ffmpeg))
    
    # Update state to STARTED
    self.update_state(state="STARTED", meta={"progress": 0})
    
    try:
        process = subprocess.Popen(
            cmd_ffmpeg,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True
        )
        
        out_time_re = re.compile(r"out_time=(\d+):(\d+):(\d+)\.(\d+)")
        
        while True:
            line = process.stdout.readline()
            if not line:
                break
                
            line = line.strip()
            # Look for out_time
            if line.startswith("out_time="):
                time_str = line.split("=", 1)[1]
                match = out_time_re.match(f"out_time={time_str}" if "out_time=" not in time_str else time_str)
                if not match:
                    match = re.search(r"(\d+):(\d+):(\d+)\.(\d+)", time_str)
                if match and duration > 0:
                    hours, minutes, seconds, ms = map(float, match.groups())
                    elapsed = hours * 3600 + minutes * 60 + seconds + ms / 1000000.0
                    progress = min(99, int((elapsed / duration) * 100))
                    self.update_state(state="PROGRESS", meta={"progress": progress})
                    logger.debug("Transcoding progress: %d%% (elapsed=%.2fs)", progress, elapsed)
                    
        process.wait()
        
        if process.returncode != 0:
            err_msg = f"ffmpeg failed with return code {process.returncode}"
            logger.error(err_msg)
            return {"ok": False, "error": err_msg}
            
    except Exception as exc:
        err_msg = f"Exception during transcoding: {exc}"
        logger.error(err_msg)
        return {"ok": False, "error": err_msg}
        
    logger.info("Transcoding finished successfully: %s", output_file)
    self.update_state(state="SUCCESS", meta={"progress": 100})
    return {"ok": True, "output_file": str(output_file)}


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
