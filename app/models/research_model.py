"""
Research Session Models
========================
Stores everything the research agents discover about a bookmaker.
"""
from datetime import datetime, timezone
from app.extensions import db


class ResearchSession(db.Model):
    """One full research run against a bookmaker."""
    __tablename__ = "research_sessions"

    id            = db.Column(db.Integer, primary_key=True)
    bookmaker_id  = db.Column(db.Integer, db.ForeignKey("bookmakers.id"), nullable=False, index=True)
    started_at    = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))
    completed_at  = db.Column(db.DateTime, nullable=True)

    # Phase tracking
    phase         = db.Column(db.String(50), default="UNAUTHENTICATED")
    # UNAUTHENTICATED | LOGIN_PENDING | AUTHENTICATED | COMPLETE | FAILED

    # Credentials (encrypted in production — plaintext for MVP)
    login_url     = db.Column(db.String(500), nullable=True)
    username_used = db.Column(db.String(200), nullable=True)
    # Never store real password — store only whether login succeeded
    login_success = db.Column(db.Boolean, nullable=True)
    otp_required  = db.Column(db.Boolean, default=False)
    otp_provided  = db.Column(db.Boolean, default=False)

    # Output
    report_md     = db.Column(db.Text, nullable=True)   # Full markdown research report
    summary       = db.Column(db.Text, nullable=True)   # AI-generated 1-page summary

    findings      = db.relationship("ResearchFinding",  backref="session", cascade="all, delete-orphan")
    endpoints     = db.relationship("ResearchEndpoint", backref="session", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<ResearchSession {self.id} bk={self.bookmaker_id} phase={self.phase}>"


class ResearchFinding(db.Model):
    """
    Individual finding — JS analysis, form structure,
    encryption method, WebSocket protocol, etc.
    """
    __tablename__ = "research_findings"

    id            = db.Column(db.Integer, primary_key=True)
    session_id    = db.Column(db.Integer, db.ForeignKey("research_sessions.id"), nullable=False, index=True)

    category      = db.Column(db.String(80), nullable=False)
    # JS_ENCRYPTION | WEBSOCKET | AUTH_FLOW | FORM_STRUCTURE |
    # AVIATOR | GAME_ENGINE | ANTI_BOT | RATE_LIMIT | CDN | OTHER

    title         = db.Column(db.String(300), nullable=False)
    detail        = db.Column(db.Text, nullable=True)       # Full technical detail
    code_snippet  = db.Column(db.Text, nullable=True)       # Relevant JS/JSON snippet
    severity      = db.Column(db.String(20), default="INFO")  # INFO | WARN | CRITICAL
    phase         = db.Column(db.String(30), nullable=True)   # which phase found this
    discovered_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))


class ResearchEndpoint(db.Model):
    """
    Every HTTP request/response observed during research
    (more detailed than BookmakerEndpoint — includes error responses,
    auth flows, form submissions, etc.)
    """
    __tablename__ = "research_endpoints"

    id              = db.Column(db.Integer, primary_key=True)
    session_id      = db.Column(db.Integer, db.ForeignKey("research_sessions.id"), nullable=False, index=True)

    url             = db.Column(db.String(1000), nullable=False)
    url_pattern     = db.Column(db.String(1000), nullable=True)
    method          = db.Column(db.String(10),   default="GET")
    status_code     = db.Column(db.Integer,      nullable=True)
    content_type    = db.Column(db.String(100),  nullable=True)

    # Classification
    endpoint_type   = db.Column(db.String(80),   nullable=True)
    # ODDS | AUTH | WEBSOCKET | GAME | PAYMENT | CONFIG | ANALYTICS | UNKNOWN

    is_authenticated = db.Column(db.Boolean, default=False)
    requires_auth    = db.Column(db.Boolean, default=False)

    # Captured data
    request_headers  = db.Column(db.JSON,  nullable=True)
    request_body     = db.Column(db.Text,  nullable=True)
    response_sample  = db.Column(db.Text,  nullable=True)  # first 3000 chars
    curl_command     = db.Column(db.Text,  nullable=True)

    # Error response (from submitting wrong data)
    error_response   = db.Column(db.Text,  nullable=True)
    error_fields     = db.Column(db.JSON,  nullable=True)  # field names from error JSON

    # Parser (if odds endpoint)
    parser_code      = db.Column(db.Text,    nullable=True)
    parser_tested    = db.Column(db.Boolean, default=False)

    # Pagination
    pagination_info  = db.Column(db.JSON, nullable=True)
    url_params       = db.Column(db.JSON, nullable=True)

    notes            = db.Column(db.Text, nullable=True)  # AI analysis notes
    discovered_at    = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))