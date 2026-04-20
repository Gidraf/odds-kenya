from datetime import datetime
from app.extensions import db
from .unified_match import UnifiedMatch
from .arbitrage_opportunity import ArbitrageOpportunity
from .ev_opportunity import EVOpportunity
from .bookmaker_odds_history import BookmakerOddsHistory
from .common import MatchStatus, OpportunityStatus, _utcnow_naive

class OddsQueryHelper:

    @staticmethod
    def matches_by_date(
        date_from:   str,
        date_to:     str,
        sport:       str | None = None,
        competition: str | None = None,
        status:      MatchStatus | None = None,
    ):
        from datetime import date, timedelta
        df = datetime.strptime(date_from, "%Y-%m-%d")
        dt = datetime.strptime(date_to,   "%Y-%m-%d") + timedelta(days=1)

        q = UnifiedMatch.query.filter(
            UnifiedMatch.start_time >= df,
            UnifiedMatch.start_time <  dt,
        )
        if sport:       q = q.filter(UnifiedMatch.sport_name == sport)
        if competition: q = q.filter(UnifiedMatch.competition_name.ilike(f"%{competition}%"))
        if status:      q = q.filter(UnifiedMatch.status == status)
        return q.order_by(UnifiedMatch.start_time)

    @staticmethod
    def finished_matches(
        date_from:  str | None = None,
        date_to:    str | None = None,
        sport:      str | None = None,
    ):
        q = UnifiedMatch.query.filter(UnifiedMatch.status == MatchStatus.FINISHED)
        if date_from:
            df = datetime.strptime(date_from, "%Y-%m-%d")
            q  = q.filter(UnifiedMatch.finished_at >= df)
        if date_to:
            dt = datetime.strptime(date_to, "%Y-%m-%d")
            from datetime import timedelta
            q  = q.filter(UnifiedMatch.finished_at < dt + timedelta(days=1))
        if sport:
            q = q.filter(UnifiedMatch.sport_name == sport)
        return q.order_by(UnifiedMatch.finished_at.desc())

    @staticmethod
    def arb_history(
        sport:        str | None = None,
        market:       str | None = None,
        bookmaker_id: int | None = None,
        min_profit:   float = 0.0,
        days:         int   = 30,
        status:       OpportunityStatus | None = None,
    ):
        from datetime import timedelta
        cutoff = _utcnow_naive() - timedelta(days=days)
        q = ArbitrageOpportunity.query.filter(
            ArbitrageOpportunity.open_at >= cutoff,
            ArbitrageOpportunity.profit_pct >= min_profit,
        )
        if sport:  q = q.filter(ArbitrageOpportunity.sport == sport)
        if market: q = q.filter(ArbitrageOpportunity.market == market)
        if status: q = q.filter(ArbitrageOpportunity.status == status)
        if bookmaker_id:
            q = q.filter(
                ArbitrageOpportunity.bookmaker_ids.contains([bookmaker_id])
            )
        return q.order_by(ArbitrageOpportunity.open_at.desc())

    @staticmethod
    def arb_peak_summary(sport: str | None = None, days: int = 90):
        from sqlalchemy import func
        from datetime import timedelta
        cutoff = _utcnow_naive() - timedelta(days=days)
        q = db.session.query(
            ArbitrageOpportunity.market,
            ArbitrageOpportunity.specifier,
            func.max(ArbitrageOpportunity.peak_profit_pct).label("best_profit_pct"),
            func.avg(ArbitrageOpportunity.profit_pct).label("avg_profit_pct"),
            func.count(ArbitrageOpportunity.id).label("count"),
            func.avg(ArbitrageOpportunity.duration_s).label("avg_duration_s"),
        ).filter(ArbitrageOpportunity.open_at >= cutoff)
        if sport:
            q = q.filter(ArbitrageOpportunity.sport == sport)
        return q.group_by(
            ArbitrageOpportunity.market,
            ArbitrageOpportunity.specifier,
        ).order_by(func.max(ArbitrageOpportunity.peak_profit_pct).desc())

    @staticmethod
    def ev_history(
        sport:        str | None = None,
        market:       str | None = None,
        bookmaker_id: int | None = None,
        min_ev:       float = 0.0,
        days:         int   = 30,
        clv_positive: bool  = False,
        status:       OpportunityStatus | None = None,
    ):
        from datetime import timedelta
        cutoff = _utcnow_naive() - timedelta(days=days)
        q = EVOpportunity.query.filter(
            EVOpportunity.open_at >= cutoff,
            EVOpportunity.ev_pct  >= min_ev,
        )
        if sport:        q = q.filter(EVOpportunity.sport == sport)
        if market:       q = q.filter(EVOpportunity.market == market)
        if bookmaker_id: q = q.filter(EVOpportunity.bookmaker_id == bookmaker_id)
        if status:       q = q.filter(EVOpportunity.status == status)
        if clv_positive: q = q.filter(EVOpportunity.clv_pct > 0)
        return q.order_by(EVOpportunity.open_at.desc())

    @staticmethod
    def odds_movement(
        match_id:     int,
        market:       str,
        selection:    str,
        bookmaker_id: int | None = None,
    ):
        q = BookmakerOddsHistory.query.filter(
            BookmakerOddsHistory.match_id  == match_id,
            BookmakerOddsHistory.market    == market,
            BookmakerOddsHistory.selection == selection,
        )
        if bookmaker_id:
            q = q.filter(BookmakerOddsHistory.bookmaker_id == bookmaker_id)
        return q.order_by(BookmakerOddsHistory.recorded_at)