"""
app/cli/seed_bookmakers.py
===========================
Flask CLI to seed all bookmaker configuration into the database.
Replaces every hardcoded BK dict in the codebase.

Usage:
  flask seed-bookmakers              # seed everything
  flask seed-bookmakers --reset      # drop + reseed
  flask seed-bookmakers --bk sp      # seed only SportPesa
  flask list-bookmakers              # show what's in DB
  flask market-failures              # show top failing markets
  flask harvest-health               # show last harvest per BK/sport
"""

import click
from flask import current_app
from app.extensions import db


# =============================================================================
# CANONICAL DATA
# =============================================================================


# Canonical sport slugs — single source of truth
CANONICAL_SPORTS = [
    "soccer", "basketball", "tennis", "ice-hockey", "volleyball",
    "cricket", "rugby", "table-tennis", "handball", "baseball",
    "mma", "boxing", "darts", "american-football", "esoccer",
]

BOOKMAKERS = [
    # ── LOCAL (Kenya-facing) ─────────────────────────────────────────────────
    {
        "slug":           "sp",
        "name":           "SportPesa",
        "short_code":     "SP",
        "tier":           "local",
        "status":         "active",
        "logo_url":       "https://upload.wikimedia.org/wikipedia/en/thumb/9/9d/SportPesa_logo.png/250px-SportPesa_logo.png",
        "primary_color":  "#22C55E",
        "website_url":    "https://www.sportpesa.com",
        "api_base_url":   "https://www.ke.sportpesa.com",
        "api_key_env":    "",
        "requires_auth":  False,
        "supported_sports": CANONICAL_SPORTS,
        "harvest_interval_seconds": 300,
        "max_pages_per_sport": 30,
        "page_size": 50,
        "redis_ttl_seconds": 3600,
        "countries": [
            {
                "code": "KE",
                "local_domain":   "www.ke.sportpesa.com",
                "api_url":        "https://www.ke.sportpesa.com/api/v1",
                "is_primary":     True,
                "is_licensed":    True,
                "payment_methods": ["mpesa", "airtel_money", "bank", "card"],
                "min_deposit":    50,
                "max_deposit":    1_000_000,
                "min_withdrawal": 100,
            },
            {
                "code": "TZ",
                "local_domain":   "www.tz.sportpesa.com",
                "api_url":        "https://www.tz.sportpesa.com/api/v1",
                "is_primary":     False,
                "is_licensed":    True,
                "payment_methods": ["mpesa"],
                "min_deposit":    1000,
            },
            {"code": "ZA"},
            {"code": "GB"},
            {"code": "IE"},
            {"code": "IM"},
        ],
    },
    {
        "slug":           "bt",
        "name":           "Betika",
        "short_code":     "BT",
        "tier":           "local",
        "status":         "active",
        "logo_url":       "https://upload.wikimedia.org/wikipedia/commons/thumb/a/a9/Betika_Logo.png/250px-Betika_Logo.png",
        "primary_color":  "#EF4444",
        "website_url":    "https://www.betika.com",
        "api_base_url":   "https://api.betika.com",
        "api_key_env":    "",
        "requires_auth":  False,
        "supported_sports": [
            "soccer", "basketball", "tennis", "ice-hockey", "volleyball",
            "cricket", "rugby", "table-tennis", "darts", "handball", "mma", "boxing",
        ],
        "harvest_interval_seconds": 300,
        "max_pages_per_sport": 30,
        "page_size": 50,
        "redis_ttl_seconds": 3600,
        "countries": [
            {
                "code": "KE",
                "local_domain":   "www.betika.com",
                "api_url":        "https://api.betika.com/v1/uo",
                "is_primary":     True,
                "is_licensed":    True,
                "payment_methods": ["mpesa", "airtel_money"],
                "min_deposit":    10,
                "max_deposit":    500_000,
                "min_withdrawal": 100,
            },
            {"code": "CD"},
            {"code": "ET"},
            {"code": "GH"},
            {"code": "MW"},
            {"code": "MZ"},
            {"code": "TZ"},
            {"code": "UG"},
            {"code": "ZM"},
        ],
    },
    {
        "slug":           "od",
        "name":           "OdiBets",
        "short_code":     "OD",
        "tier":           "local",
        "status":         "active",
        "logo_url":       "https://www.odibets.com/images/odibets-logo.png",
        "primary_color":  "#EAB308",
        "website_url":    "https://www.odibets.com",
        "api_base_url":   "https://api.odi.site",
        "api_key_env":    "",
        "requires_auth":  False,
        "supported_sports": [
            "soccer", "basketball", "tennis", "ice-hockey", "volleyball",
            "cricket", "rugby", "boxing", "handball", "mma", "table-tennis",
            "darts", "american-football", "esoccer",
        ],
        "harvest_interval_seconds": 300,
        "max_pages_per_sport": 20,
        "page_size": 50,
        "redis_ttl_seconds": 3600,
        "countries": [
            {
                "code": "KE",
                "local_domain":   "www.odibets.com",
                "api_url":        "https://api.odi.site/sportsbook/v1",
                "is_primary":     True,
                "is_licensed":    True,
                "payment_methods": ["mpesa", "airtel_money"],
                "min_deposit":    20,
                "max_deposit":    300_000,
                "min_withdrawal": 100,
            },
            {"code": "GH"},
        ],
    },

    # ── B2B / INTERNATIONAL ──────────────────────────────────────────────────
    {
        "slug":           "1xbet",
        "name":           "1xBet",
        "short_code":     "1X",
        "tier":           "b2b",
        "status":         "active",
        "logo_url":       "https://1xbet.com/images/logo/1xbet-logo.png",
        "primary_color":  "#1F8AEB",
        "website_url":    "https://1xbet.com",
        "api_base_url":   "https://api.1xbet.com",
        "api_key_env":    "XBET_API_KEY",
        "requires_auth":  True,
        "supported_sports": CANONICAL_SPORTS,
        "harvest_interval_seconds": 600,
        "redis_ttl_seconds": 3600,
        "countries": [
            {
                "code": "KE",
                "local_domain":   "ke.1xbet.com",
                "api_url":        "https://api.1xbet.com/v2",
                "is_primary":     False,
                "is_licensed":    False,
                "payment_methods": ["mpesa", "card", "crypto"],
                "min_deposit":    100,
            },
            {
                "code": "CW",
                "local_domain":   "1xbet.com",
                "api_url":        "https://api.1xbet.com/v2",
                "is_primary":     True,
                "is_licensed":    True,
                "payment_methods": ["card", "crypto", "bank"],
            },
            {"code": "NG"},
            {"code": "GH"},
            {"code": "UG"},
            {"code": "CM"},
            {"code": "SN"},
            {"code": "ZM"},
            {"code": "BI"},
            {"code": "CD"},
        ],
    },
    {
        "slug":           "22bet",
        "name":           "22Bet",
        "short_code":     "22",
        "tier":           "b2b",
        "status":         "active",
        "logo_url":       "https://22bet.com/images/22bet-logo.png",
        "primary_color":  "#3A8EFF",
        "website_url":    "https://22bet.com",
        "api_base_url":   "https://api.22bet.com",
        "api_key_env":    "BET22_API_KEY",
        "requires_auth":  True,
        "supported_sports": CANONICAL_SPORTS,
        "harvest_interval_seconds": 600,
        "redis_ttl_seconds": 3600,
        "countries": [
            {
                "code": "KE",
                "local_domain":   "ke.22bet.com",
                "api_url":        "https://api.22bet.com/v1",
                "is_primary":     False,
                "is_licensed":    False,
                "payment_methods": ["mpesa", "card"],
            },
            {
                "code": "CW",
                "local_domain":   "22bet.com",
                "api_url":        "https://api.22bet.com/v1",
                "is_primary":     True,
                "is_licensed":    True,
                "payment_methods": ["card", "crypto"],
            },
            {"code": "NG"},
            {"code": "TZ"},
            {"code": "GH"},
            {"code": "UG"},
            {"code": "ZM"},
        ],
    },
    {
        "slug":           "betwinner",
        "name":           "Betwinner",
        "short_code":     "BW",
        "tier":           "b2b",
        "status":         "active",
        "logo_url":       "https://betwinner.com/images/logo/betwinner-logo.png",
        "primary_color":  "#FF6600",
        "website_url":    "https://betwinner.com",
        "api_base_url":   "https://api.betwinner.com",
        "api_key_env":    "BETWINNER_API_KEY",
        "requires_auth":  True,
        "supported_sports": CANONICAL_SPORTS,
        "harvest_interval_seconds": 600,
        "redis_ttl_seconds": 3600,
        "countries": [
            {
                "code": "KE",
                "local_domain":   "betwinner.com",
                "api_url":        "https://api.betwinner.com/v1",
                "is_primary":     False,
                "is_licensed":    False,
                "payment_methods": ["mpesa", "card", "crypto"],
            },
            {"code": "CW"},
            {"code": "NG"},
            {"code": "GH"},
            {"code": "ZM"},
            {"code": "UG"},
            {"code": "ZA"},
        ],
    },
    {
        "slug":           "melbet",
        "name":           "Melbet",
        "short_code":     "MB",
        "tier":           "b2b",
        "status":         "active",
        "logo_url":       "https://melbet.com/images/logo.png",
        "primary_color":  "#FF2020",
        "website_url":    "https://melbet.com",
        "api_base_url":   "https://api.melbet.com",
        "api_key_env":    "MELBET_API_KEY",
        "requires_auth":  True,
        "supported_sports": CANONICAL_SPORTS,
        "harvest_interval_seconds": 600,
        "redis_ttl_seconds": 3600,
        "countries": [
            {
                "code": "KE",
                "local_domain":   "melbet.com",
                "api_url":        "https://api.melbet.com/v1",
                "is_primary":     False,
                "is_licensed":    False,
                "payment_methods": ["mpesa", "card"],
            },
            {"code": "CW"},
            {"code": "NG"},
            {"code": "GH"},
            {"code": "UG"},
            {"code": "ZM"},
            {"code": "BI"},
        ],
    },
    {
        "slug":           "megapari",
        "name":           "Megapari",
        "short_code":     "MP",
        "tier":           "b2b",
        "status":         "active",
        "logo_url":       "https://megapari.com/images/logo.png",
        "primary_color":  "#7B2FBE",
        "website_url":    "https://megapari.com",
        "api_base_url":   "https://api.megapari.com",
        "api_key_env":    "MEGAPARI_API_KEY",
        "requires_auth":  True,
        "supported_sports": CANONICAL_SPORTS,
        "harvest_interval_seconds": 600,
        "redis_ttl_seconds": 3600,
        "countries": [
            {
                "code": "KE",
                "local_domain":   "megapari.com",
                "api_url":        "https://api.megapari.com/v1",
                "is_primary":     False,
                "is_licensed":    False,
                "payment_methods": ["mpesa", "card", "crypto"],
            },
            {"code": "CW"},
            {"code": "NG"},
        ],
    },
    {
        "slug":           "helabet",
        "name":           "Helabet",
        "short_code":     "HL",
        "tier":           "b2b",
        "status":         "active",
        "logo_url":       "https://helabet.com/images/logo.png",
        "primary_color":  "#9C27B0",
        "website_url":    "https://helabet.com",
        "api_base_url":   "https://api.helabet.com",
        "api_key_env":    "HELABET_API_KEY",
        "requires_auth":  True,
        "supported_sports": [
            "soccer", "basketball", "tennis", "ice-hockey",
            "volleyball", "cricket", "rugby", "table-tennis",
        ],
        "harvest_interval_seconds": 600,
        "redis_ttl_seconds": 3600,
        "countries": [
            {
                "code": "KE",
                "local_domain":   "helabet.com",
                "api_url":        "https://api.helabet.com/v1",
                "is_primary":     False,
                "is_licensed":    False,
                "payment_methods": ["mpesa", "card"],
            },
            {"code": "CW"},
            {"code": "BI"},
        ],
    },
    {
        "slug":           "paripesa",
        "name":           "Paripesa",
        "short_code":     "PP",
        "tier":           "b2b",
        "status":         "active",
        "logo_url":       "https://paripesa.com/images/logo.png",
        "primary_color":  "#FF6B35",
        "website_url":    "https://paripesa.com",
        "api_base_url":   "https://api.paripesa.com",
        "api_key_env":    "PARIPESA_API_KEY",
        "requires_auth":  True,
        "supported_sports": [
            "soccer", "basketball", "tennis", "cricket",
            "ice-hockey", "volleyball", "rugby",
        ],
        "harvest_interval_seconds": 600,
        "redis_ttl_seconds": 3600,
        "countries": [
            {
                "code": "KE",
                "local_domain":   "paripesa.com",
                "api_url":        "https://api.paripesa.com/v1",
                "is_primary":     False,
                "is_licensed":    False,
                "payment_methods": ["mpesa", "card", "crypto"],
            },
            {"code": "CW"},
            {"code": "NG"},
            {"code": "ZM"},
        ],
    },
]


# Full country list — must cover every code used above
COUNTRIES = [
    {"code": "KE", "name": "Kenya",                   "flag_emoji": "🇰🇪", "currency_code": "KES", "currency_symbol": "KSh"},
    {"code": "TZ", "name": "Tanzania",                "flag_emoji": "🇹🇿", "currency_code": "TZS", "currency_symbol": "TSh"},
    {"code": "UG", "name": "Uganda",                  "flag_emoji": "🇺🇬", "currency_code": "UGX", "currency_symbol": "USh"},
    {"code": "NG", "name": "Nigeria",                 "flag_emoji": "🇳🇬", "currency_code": "NGN", "currency_symbol": "₦"},
    {"code": "GH", "name": "Ghana",                   "flag_emoji": "🇬🇭", "currency_code": "GHS", "currency_symbol": "₵"},
    {"code": "ZA", "name": "South Africa",            "flag_emoji": "🇿🇦", "currency_code": "ZAR", "currency_symbol": "R"},
    {"code": "ET", "name": "Ethiopia",                "flag_emoji": "🇪🇹", "currency_code": "ETB", "currency_symbol": "Br"},
    {"code": "RW", "name": "Rwanda",                  "flag_emoji": "🇷🇼", "currency_code": "RWF", "currency_symbol": "RF"},
    {"code": "ZM", "name": "Zambia",                  "flag_emoji": "🇿🇲", "currency_code": "ZMW", "currency_symbol": "K"},
    {"code": "MW", "name": "Malawi",                  "flag_emoji": "🇲🇼", "currency_code": "MWK", "currency_symbol": "MK"},
    {"code": "MZ", "name": "Mozambique",              "flag_emoji": "🇲🇿", "currency_code": "MZN", "currency_symbol": "MT"},
    {"code": "CD", "name": "DR Congo",                "flag_emoji": "🇨🇩", "currency_code": "CDF", "currency_symbol": "FC"},
    {"code": "CM", "name": "Cameroon",                "flag_emoji": "🇨🇲", "currency_code": "XAF", "currency_symbol": "FCFA"},
    {"code": "SN", "name": "Senegal",                 "flag_emoji": "🇸🇳", "currency_code": "XOF", "currency_symbol": "CFA"},
    {"code": "BI", "name": "Burundi",                 "flag_emoji": "🇧🇮", "currency_code": "BIF", "currency_symbol": "Fr"},
    {"code": "CW", "name": "Curaçao",                 "flag_emoji": "🇨🇼", "currency_code": "ANG", "currency_symbol": "ƒ"},
    {"code": "GB", "name": "United Kingdom",          "flag_emoji": "🇬🇧", "currency_code": "GBP", "currency_symbol": "£"},
    {"code": "IE", "name": "Ireland",                 "flag_emoji": "🇮🇪", "currency_code": "EUR", "currency_symbol": "€"},
    {"code": "IM", "name": "Isle of Man",             "flag_emoji": "🇮🇲", "currency_code": "GBP", "currency_symbol": "£"},
    {"code": "MT", "name": "Malta",                   "flag_emoji": "🇲🇹", "currency_code": "EUR", "currency_symbol": "€"},
    {"code": "CY", "name": "Cyprus",                  "flag_emoji": "🇨🇾", "currency_code": "EUR", "currency_symbol": "€"},
]

# =============================================================================
# SEED LOGIC
# =============================================================================

def _seed_countries(reset: bool = False) -> dict[str, int]:
    """Seed countries table. Returns {code: id} map."""
    from app.models.bookmakers_model import Country
    id_map: dict[str, int] = {}
    seeded = failed = 0

    for c in COUNTRIES:
        try:
            existing = Country.query.filter_by(code=c["code"]).first()
            if reset and existing:
                db.session.delete(existing)
                db.session.flush()
                existing = None

            if existing:
                for k, v in c.items():
                    setattr(existing, k, v)
                id_map[c["code"]] = existing.id
            else:
                obj = Country(**c)
                db.session.add(obj)
                db.session.flush()
                id_map[c["code"]] = obj.id
            seeded += 1
        except Exception as exc:
            failed += 1
            click.echo(f"  ⚠️  Country {c['code']} failed: {exc}", err=True)
            db.session.rollback()

    db.session.commit()
    click.echo(f"  ✅ Countries: {seeded} seeded, {failed} failed")
    return id_map


def _seed_one_bookmaker(bk_data: dict, country_map: dict[str, int], reset: bool) -> bool:
    """Seed one bookmaker + its country relationships. Returns True on success."""
    from app.models.bookmakers_model import Bookmaker, BookmakerCountry, BkTier, BkStatus

    slug = bk_data["slug"]
    try:
        existing = Bookmaker.query.filter_by(slug=slug).first()
        if reset and existing:
            db.session.delete(existing)
            db.session.flush()
            existing = None

        bk_fields = {
            k: v for k, v in bk_data.items()
            if k not in ("countries",) and hasattr(Bookmaker, k)
        }
        bk_fields["tier"]   = BkTier(bk_fields.get("tier", "local"))
        bk_fields["status"] = BkStatus(bk_fields.get("status", "active"))

        if existing:
            for k, v in bk_fields.items():
                setattr(existing, k, v)
            bk = existing
        else:
            bk = Bookmaker(**bk_fields)
            db.session.add(bk)

        db.session.flush()

        # Seed country relationships
        countries_ok = countries_fail = 0
        for c_data in bk_data.get("countries", []):
            code = c_data["code"]
            c_id = country_map.get(code)
            if not c_id:
                click.echo(f"    ⚠️  {slug}: country {code} not in DB — skipping", err=True)
                countries_fail += 1
                continue
            try:
                rel = BookmakerCountry.query.filter_by(
                    bookmaker_id=bk.id, country_id=c_id,
                ).first()
                rel_fields = {k: v for k, v in c_data.items() if k != "code"}
                if rel:
                    for k, v in rel_fields.items():
                        setattr(rel, k, v)
                else:
                    rel = BookmakerCountry(bookmaker_id=bk.id, country_id=c_id, **rel_fields)
                    db.session.add(rel)
                countries_ok += 1
            except Exception as exc:
                click.echo(f"    ⚠️  {slug}/{code} country rel failed: {exc}", err=True)
                countries_fail += 1
                db.session.rollback()

        db.session.commit()
        status = "✅" if countries_fail == 0 else "⚠️ "
        click.echo(f"  {status} {slug:12} ({bk_data['name']}) — {countries_ok} countries, {countries_fail} failed")
        return True

    except Exception as exc:
        click.echo(f"  ❌ {slug}: FAILED — {exc}", err=True)
        db.session.rollback()
        return False


# =============================================================================
# CLI COMMANDS
# =============================================================================

def register_cli(app):
    """Call this from run.py: register_cli(flask_app)"""

    @app.cli.command("seed-bookmakers")
    @click.option("--reset", is_flag=True, default=False, help="Drop and re-seed all records")
    @click.option("--bk",    default=None,  help="Seed only this bookmaker slug (e.g. --bk sp)")
    def seed_bookmakers(reset, bk):
        """Seed bookmakers, countries, and payment methods into the database."""
        click.echo("\n📚 Seeding bookmaker catalog…\n")

        # 1. Countries first
        country_map = _seed_countries(reset)

        # 2. Bookmakers
        targets = [b for b in BOOKMAKERS if (bk is None or b["slug"] == bk)]
        if not targets:
            click.echo(f"  ❌ No bookmaker found with slug '{bk}'", err=True)
            return

        click.echo(f"\n📖 Seeding {len(targets)} bookmakers…\n")
        ok = fail = 0
        for bk_data in targets:
            if _seed_one_bookmaker(bk_data, country_map, reset):
                ok += 1
            else:
                fail += 1

        click.echo(f"\n{'='*50}")
        click.echo(f"  Bookmakers: {ok} seeded, {fail} failed")
        click.echo(f"  Countries:  {len(country_map)} available")
        click.echo(f"{'='*50}\n")

    @app.cli.command("list-bookmakers")
    def list_bookmakers():
        """Show all bookmakers currently in the database."""
        from app.models.bookmakers_model import Bookmaker
        bks = Bookmaker.query.order_by(Bookmaker.tier, Bookmaker.slug).all()
        if not bks:
            click.echo("No bookmakers seeded yet. Run: flask seed-bookmakers")
            return

        click.echo(f"\n{'Slug':<12} {'Name':<15} {'Tier':<8} {'Status':<10} {'Sports':<5} {'Countries'}")
        click.echo("-" * 70)
        for b in bks:
            countries = len(b.countries)
            sports    = len(b.supported_sports or [])
            click.echo(f"  {b.slug:<10} {b.name:<15} {b.tier.value:<8} {b.status.value:<10} {sports:<5} {countries}")
        click.echo()

    @app.cli.command("market-failures")
    @click.option("--bk",    default=None, help="Filter by bookmaker slug")
    @click.option("--sport", default=None, help="Filter by sport slug")
    @click.option("--top",   default=20,   help="Show top N failures")
    def market_failures(bk, sport, top):
        """Show markets that fail most often across bookmakers."""
        from app.models.bookmakers_model import MarketFailure, Bookmaker
        q = db.session.query(MarketFailure).join(Bookmaker)
        if bk:
            q = q.filter(Bookmaker.slug == bk)
        if sport:
            q = q.filter(MarketFailure.sport_slug == sport)
        rows = q.order_by(MarketFailure.failure_count.desc()).limit(top).all()

        if not rows:
            click.echo("No market failures recorded yet.")
            return

        click.echo(f"\n{'BK':<12} {'Sport':<16} {'Market':<32} {'Fails':<8} {'Last Seen'}")
        click.echo("-" * 85)
        for r in rows:
            bk_slug  = r.bookmaker.slug if r.bookmaker else "?"
            last     = r.last_seen.strftime("%m-%d %H:%M") if r.last_seen else "?"
            click.echo(f"  {bk_slug:<10} {r.sport_slug:<16} {r.market_name:<32} {r.failure_count:<8} {last}")
        click.echo()

    @app.cli.command("harvest-health")
    @click.option("--bk",    default=None, help="Filter by bookmaker slug")
    @click.option("--sport", default=None, help="Filter by sport slug")
    def harvest_health(bk, sport):
        """Show last harvest job per bookmaker/sport."""
        from app.models.bookmakers_model import HarvestJob, Bookmaker
        from sqlalchemy import func

        # Latest job per bk+sport
        sub = (
            db.session.query(
                HarvestJob.bookmaker_id,
                HarvestJob.sport_slug,
                func.max(HarvestJob.started_at).label("latest"),
            )
            .group_by(HarvestJob.bookmaker_id, HarvestJob.sport_slug)
            .subquery()
        )
        q = (
            db.session.query(HarvestJob)
            .join(sub, (HarvestJob.bookmaker_id == sub.c.bookmaker_id) &
                       (HarvestJob.sport_slug == sub.c.sport_slug) &
                       (HarvestJob.started_at == sub.c.latest))
            .join(Bookmaker)
        )
        if bk:
            q = q.filter(Bookmaker.slug == bk)
        if sport:
            q = q.filter(HarvestJob.sport_slug == sport)

        rows = q.order_by(Bookmaker.slug, HarvestJob.sport_slug).all()
        if not rows:
            click.echo("No harvest jobs recorded yet.")
            return

        click.echo(f"\n{'BK':<12} {'Sport':<16} {'Status':<8} {'Matches':<9} {'Latency':<10} {'Started'}")
        click.echo("-" * 75)
        for r in rows:
            bk_name  = r.bookmaker.slug if r.bookmaker else "?"
            latency  = f"{r.latency_ms}ms" if r.latency_ms else "?"
            started  = r.started_at.strftime("%m-%d %H:%M") if r.started_at else "?"
            icon     = "✅" if r.status == "ok" else ("⚠️ " if r.status == "partial" else "❌")
            click.echo(f"  {bk_name:<10} {r.sport_slug:<16} {icon} {r.status:<6} {r.match_count:<9} {latency:<10} {started}")
        click.echo()

    return app