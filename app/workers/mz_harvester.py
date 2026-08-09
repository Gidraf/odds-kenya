"""
app/workers/mz_harvester.py
============================
Mozzart Prematch / Upcoming HTTP Harvester.
Scrapes paginated betOffer2 and fetches full match odds via /matchBetting
with bounded Semaphore concurrency.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any

import httpx

from app.workers.mappers.mozzart import MozzartMapper, MZ_SPORT_SLUGS

log = logging.getLogger("mz_upcoming")

BASE_URL   = "https://www.mozzartbet.co.ke"
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
HEADERS    = {"User-Agent": USER_AGENT, "Origin": BASE_URL, "Content-Type": "application/json"}


def _get_redis():
    import redis
    url = os.getenv("REDIS_URL", "redis://redis6382:6382/0")
    if os.path.exists("/.dockerenv") and "localhost" in url:
        url = url.replace("localhost", "redis6382").replace("6379", "6382")
    return redis.Redis.from_url(url, decode_responses=True, socket_connect_timeout=3, socket_timeout=3)


class MozzartPrematchHarvester:
    """
    HTTP Harvester for Mozzart Prematch / Upcoming odds.
    """

    def __init__(self, max_concurrency: int = 5):
        self.semaphore = asyncio.Semaphore(max_concurrency)
        self.client: httpx.AsyncClient | None = None
        self._redis = None

    @property
    def r(self):
        if self._redis is None:
            self._redis = _get_redis()
        return self._redis

    async def _get_client(self) -> httpx.AsyncClient:
        if self.client is None or self.client.is_closed:
            self.client = httpx.AsyncClient(
                base_url=BASE_URL,
                headers=HEADERS,
                timeout=httpx.Timeout(15.0, connect=5.0),
                limits=httpx.Limits(max_keepalive_connections=20, max_connections=30),
            )
        return self.client

    async def close(self):
        if self.client and not self.client.is_closed:
            await self.client.aclose()

    async def fetch_prematch_page(self, offset: int = 0, size: int = 50) -> dict:
        """POST /betOffer2 to fetch match list."""
        client = await self._get_client()
        payload = {
            "date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            "sportIds": [],
            "competitionIds": [],
            "sort": "bycompetition",
            "specials": None,
            "subgames": [],
            "size": size,
            "mostPlayed": False,
            "type": "betting",
            "numberOfGames": 0,
            "activeCompleteOffer": True,
            "lang": "en",
            "offset": offset,
        }
        try:
            res = await client.post("/betOffer2", json=payload)
            if res.status_code == 200:
                return res.json()
        except Exception as exc:
            log.warning("Mozzart /betOffer2 offset=%s failed: %s", offset, exc)
        return {}

    async def fetch_match_odds(self, match_id: int) -> dict:
        """POST /matchBetting with Semaphore throttle to fetch full kodds."""
        async with self.semaphore:
            client = await self._get_client()
            try:
                res = await client.post("/matchBetting", json={"id": match_id})
                if res.status_code == 200:
                    return res.json()
            except Exception as exc:
                log.debug("Mozzart /matchBetting id=%s failed: %s", match_id, exc)
            return {}

    def parse_match(self, match_info: dict, odds_info: dict) -> dict | None:
        """Parse raw match and kodds into normalized structure."""
        match_id = match_info.get("id")
        if not match_id:
            return None

        comp_obj = match_info.get("competition") or {}
        comp_name = comp_obj.get("name") or match_info.get("competition_name_en") or ""
        country_obj = comp_obj.get("country") or {}
        country_name = country_obj.get("name") or ""
        sport_obj = comp_obj.get("sport") or {}
        sport_id = sport_obj.get("id") or 1
        sport_slug = MZ_SPORT_SLUGS.get(sport_id, "soccer")

        participants = match_info.get("participants") or []
        home_name = participants[0].get("name", "") if len(participants) > 0 else ""
        away_name = participants[1].get("name", "") if len(participants) > 1 else ""

        if not home_name or not away_name:
            return None

        start_ts = match_info.get("startTime")
        start_time_iso = None
        if start_ts:
            try:
                start_time_iso = datetime.fromtimestamp(start_ts / 1000.0, timezone.utc).isoformat()
            except Exception:
                pass

        sms_id = str(match_info.get("matchNumber") or "").strip()
        betradar_id = str(match_info.get("betradarId") or "").strip()
        bookmaker_match_id = str(match_id)
        parent_match_id = f"mz_{bookmaker_match_id}"

        # Extract markets from kodds
        normalized_markets: dict[str, dict[str, float]] = {}
        kodds = odds_info.get("kodds") or {}

        for kodd_id, kval in kodds.items():
            if not isinstance(kval, dict):
                continue
            if kval.get("winStatus") != "ACTIVE":
                continue

            sub_game = kval.get("subGame") or {}
            game_id = sub_game.get("gameId")
            if not game_id:
                continue

            raw_name = sub_game.get("gameName", "")
            special_type = sub_game.get("specialOddValueType", "")
            special_value = kval.get("specialOddValue") or "-1"

            canonical_slug = MozzartMapper.get_canonical_slug(
                game_id, special_value, raw_name, special_type
            )
            normalized_markets.setdefault(canonical_slug, {})

            sub_name = str(sub_game.get("subGameName") or sub_game.get("subGameId") or "").strip()
            val_str = kval.get("value")
            try:
                price = float(val_str)
            except (TypeError, ValueError):
                continue

            if price <= 1.0:
                continue

            out_key = MozzartMapper.normalize_outcome_key(canonical_slug, sub_name)
            normalized_markets[canonical_slug][out_key] = price

        return {
            "match_id": parent_match_id,
            "bookmaker_match_id": bookmaker_match_id,
            "sms_id": sms_id,
            "betradar_id": betradar_id,
            "home_team": home_name,
            "away_team": away_name,
            "competition": comp_name,
            "country": country_name,
            "sport": sport_slug,
            "sport_id": sport_id,
            "start_time": start_time_iso,
            "status": "PRE_MATCH",
            "is_live": False,
            "markets": normalized_markets,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }

    async def harvest_all_upcoming(self, max_pages: int = 6) -> dict[str, list[dict]]:
        """Harvest paginated upcoming matches across all sports."""
        all_parsed: list[dict] = []
        offset = 0
        size = 50

        for page in range(max_pages):
            data = await self.fetch_prematch_page(offset=offset, size=size)
            matches = data.get("matches") or []
            if not matches:
                break

            # Fetch odds for all matches in parallel with semaphore
            tasks = [self.fetch_match_odds(m["id"]) for m in matches]
            odds_results = await asyncio.gather(*tasks, return_exceptions=True)

            for m_info, o_res in zip(matches, odds_results):
                if isinstance(o_res, dict) and o_res:
                    parsed = self.parse_match(m_info, o_res)
                    if parsed and parsed.get("markets"):
                        all_parsed.append(parsed)

            total = data.get("total", 0)
            offset += size
            if offset >= total:
                break

        # Group by sport and write to Redis
        by_sport: dict[str, list[dict]] = {}
        for m in all_parsed:
            sp = m["sport"]
            by_sport.setdefault(sp, []).append(m)

        r = self.r
        for sport_slug, matches in by_sport.items():
            redis_key = f"odds:mz:upcoming:{sport_slug}"
            r.setex(redis_key, 3600, json.dumps(matches))
            r.publish(f"odds:mz:upcoming:{sport_slug}:updates", json.dumps(matches))

        await self.close()
        log.info("Mozzart upcoming harvest complete: %s matches parsed across %s sports", len(all_parsed), len(by_sport))
        return by_sport


def run_mozzart_upcoming_harvest():
    """Sync wrapper entrypoint for Celery worker."""
    harvester = MozzartPrematchHarvester()
    return asyncio.run(harvester.harvest_all_upcoming())
