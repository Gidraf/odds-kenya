from flask import Blueprint

# Using the exact name from your crash logs
bp_odds_customer = Blueprint("customer-odds-web", __name__, url_prefix="/api/debug")