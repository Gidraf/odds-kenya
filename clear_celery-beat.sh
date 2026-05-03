#!/bin/bash
# Delete corrupt db file everywhere it could be
find /root/sports/odds-kenya -name "celerybeat-schedule*" -delete 2>/dev/null
docker exec odds-kenya-celery-beat-1 find / -name "celerybeat-schedule*" -delete 2>/dev/null || true

# Force recreate beat container with fresh image
docker compose rm -f celery-beat
docker compose build --no-cache celery-beat
docker compose up -d celery-beat

sleep 5
docker logs odds-kenya-celery-beat-1
docker system prune -a --volumes -f