"""app/workers/sp_sports/basketball.py — Basketball (sportId=2)"""
from app.workers.sp_harvester_base import SportConfig

CONFIG = SportConfig(
    slugs        = ("basketball",),
    sport_id     = 2,
    days_default = 3,
    max_default  = 150,
    market_ids   = (
        "382,"   # Match Winner (2-way, OT incl.)
        "51,"    # Point Spread (Handicap - OT incl., multi-line)
        "52,"    # Total Points O/U (multi-line: 176.5, 177.5…)
        "353,"   # Home Team Total Points O/U (multi-line)
        "352,"   # Away Team Total Points O/U (multi-line)
        "45,"    # Odd/Even Points
        "222,"   # Winning Margin (H15, H610, H_10…)
        "42,"    # 3 Way - First Half
        "53,"    # Handicap - First Half (multi-line)
        "54,"    # Total Points - First Half (multi-line)
        "224,"   # Highest Scoring Quarter
        "362,"   # 1st Quarter O/U Total Points
        "363,"   # 2nd Quarter O/U Total Points
        "364,"   # 3rd Quarter O/U Total Points
        "365,"   # 4th Quarter O/U Total Points
        "366,"   # 1st Quarter Points Handicap
        "367,"   # 2nd Quarter Points Handicap
        "368,"   # 3rd Quarter Points Handicap
        "369"    # 4th Quarter Points Handicap
    ).replace("\n", "").replace(" ", ""),
)