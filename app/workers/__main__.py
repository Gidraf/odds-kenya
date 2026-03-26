"""
app/workers/__main__.py
========================
Proper entry point — avoids the RuntimeWarning produced by:
  python -m app.workers.sp_live_harvester

Use instead:
  python -m app.workers

The warning ("found in sys.modules after import of package") fires because
app/workers/__init__.py may import sp_live_harvester before Python tries to
execute it as __main__. This file side-steps the problem entirely.
"""
import logging
import sys


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        stream=sys.stdout,
    )
    log = logging.getLogger("sp_live")

    # Lazy import — package is already loaded by the time we get here
    from app.workers.sp_live_harvester import (  # noqa: PLC0415
        SpLiveHarvester,
        snapshot_all_sports,
    )

    log.info("Warming live snapshot…")
    try:
        result = snapshot_all_sports()
        log.info("Snapshot complete: %d sports, %d total events",
                 len(result), sum(len(v) for v in result.values()))
    except Exception as exc:
        log.warning("Snapshot failed (harvester will still start): %s", exc)

    log.info("Starting Sportpesa WebSocket harvester — press Ctrl+C to stop")
    harvester = SpLiveHarvester()
    try:
        harvester.run_forever()
    except KeyboardInterrupt:
        log.info("Interrupted — shutting down")
        harvester.stop()


if __name__ == "__main__":
    main()