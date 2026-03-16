
from datetime import datetime
from time import timezone
from app.extensions import db


class ArbitrageOpportunity(db.Model):
    __tablename__ = 'arbitrage_opportunities'
    id = db.Column(db.Integer, primary_key=True)
    match_id = db.Column(db.Integer, db.ForeignKey('unified_matches.id'), nullable=False, index=True)
    market_def_id = db.Column(db.Integer, db.ForeignKey('market_definitions.id'), nullable=False)
    
    # Lifecycle tracking
    started_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc), index=True)
    collapsed_at = db.Column(db.DateTime, nullable=True) # Set when the arb disappears
    is_active = db.Column(db.Boolean, default=True, index=True)
    
    # Value tracking
    initial_profit_percentage = db.Column(db.Float, nullable=False)
    max_profit_percentage = db.Column(db.Float, nullable=False)
    
    # Relationships
    legs = db.relationship('ArbitrageLeg', backref='arbitrage_opportunity', cascade="all, delete-orphan")
    history = db.relationship('ArbitrageHistory', backref='arbitrage_opportunity', cascade="all, delete-orphan")

class ArbitrageLeg(db.Model):
    """The specific bets required to execute the arbitrage."""
    __tablename__ = 'arbitrage_legs'
    id = db.Column(db.Integer, primary_key=True)
    arbitrage_id = db.Column(db.Integer, db.ForeignKey('arbitrage_opportunities.id'), nullable=False)
    odds_id = db.Column(db.Integer, db.ForeignKey('odds.id'), nullable=False) # Links to the exact bookmaker and price
    selection_name = db.Column(db.String(100), nullable=False) # e.g., 'Over 2.5'
    price_at_discovery = db.Column(db.Float, nullable=False)

class ArbitrageHistory(db.Model):
    """The high-frequency tracker that records the arb's value until it collapses."""
    __tablename__ = 'arbitrage_history'
    id = db.Column(db.Integer, primary_key=True)
    arbitrage_id = db.Column(db.Integer, db.ForeignKey('arbitrage_opportunities.id'), nullable=False, index=True)
    profit_percentage = db.Column(db.Float, nullable=False)
    recorded_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))


class EVOpportunity(db.Model):
    __tablename__ = 'ev_opportunities'
    id = db.Column(db.Integer, primary_key=True)
    odds_id = db.Column(db.Integer, db.ForeignKey('odds.id'), nullable=False, index=True) # The +EV bet to place
    
    fair_value_price = db.Column(db.Float, nullable=False) # The true odds with the bookmaker's margin removed
    no_vig_probability = db.Column(db.Float, nullable=False) # The true probability (e.g., 0.55 for 55%)
    edge_percentage = db.Column(db.Float, nullable=False) # e.g., 4.2% edge
    
    # Store the IDs of the sharp bookmakers used to calculate this specific EV (Customizable Formula)
    sharp_bookmaker_ids = db.Column(db.JSON, nullable=False) 
    
    discovered_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc), index=True)
    is_active = db.Column(db.Boolean, default=True)

class SharpMoneySignal(db.Model):
    """Detects when professional bettors hammer a line, causing a sudden price drop (Steam Move)."""
    __tablename__ = 'sharp_money_signals'
    id = db.Column(db.Integer, primary_key=True)
    match_id = db.Column(db.Integer, db.ForeignKey('unified_matches.id'), nullable=False)
    market_def_id = db.Column(db.Integer, db.ForeignKey('market_definitions.id'), nullable=False)
    bookmaker_id = db.Column(db.Integer, db.ForeignKey('bookmakers.id'), nullable=False)
    
    signal_type = db.Column(db.String(50), nullable=False) # 'STEAM_MOVE', 'REVERSE_LINE_MOVEMENT'
    selection_name = db.Column(db.String(100), nullable=False)
    
    old_price = db.Column(db.Float, nullable=False)
    new_price = db.Column(db.Float, nullable=False)
    triggered_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc), index=True)