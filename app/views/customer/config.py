from datetime import timedelta

FREE_ACCESS = True
_ENDPOINT_ACCESS = {
    "get_upcoming": "free", "get_live": "free", "get_results": "free", 
    "get_results_by_date": "free", "get_match": "free", "search_matches": "free",
    "list_sports": "free", "list_bookmakers": "free", "list_markets": "free", 
    "harvest_status": "free", "get_match_analytics": "free",
}

FREE_MATCH_LIMIT = 1000
_WS_CHANNEL      = "odds:updates"
_ARB_CHANNEL     = "arb:updates"
_EV_CHANNEL      = "ev:updates"
_CACHE_PREFIXES  = ["sbo", "sp", "bt", "od", "b2b", "bt_od"]
_STREAM_BATCH    = 20
MIN_BOOKMAKERS   = 2
_LIVE_WINDOW     = timedelta(hours=2, minutes=30)

_TERMINAL_STATUSES = frozenset({"FINISHED", "CANCELLED", "POSTPONED", "SUSPENDED"})
_EXCLUDE_FROM_UPCOMING = frozenset({"FINISHED", "CANCELLED", "POSTPONED", "SUSPENDED", "IN_PLAY", "LIVE", "INPLAY", "IN PLAY"})

_BK_SLUG = {"sportpesa": "sp", "betika": "bt", "odibets": "od", "sp": "sp", "bt": "bt", "od": "od", "sbo": "sbo", "b2b": "b2b"}

_SPORT_ALIASES = {
    "soccer": ["Soccer", "Football"], "football": ["Soccer", "Football"],
    "basketball": ["Basketball"], "tennis": ["Tennis"], "ice-hockey": ["Ice Hockey"],
    "volleyball": ["Volleyball"], "cricket": ["Cricket"], "rugby": ["Rugby"],
    "table-tennis": ["Table Tennis"], "handball": ["Handball"], "mma": ["MMA"],
    "boxing": ["Boxing"], "darts": ["Darts"], "esoccer": ["eSoccer", "eFootball"],
    "baseball": ["Baseball"], "american-football": ["American Football"],
}

_CANONICAL_SLUG = {
    "Football": "soccer", "football": "soccer", "Soccer": "soccer", "soccer": "soccer",
    "Ice Hockey": "ice-hockey", "ice hockey": "ice-hockey", "ice-hockey": "ice-hockey",
    "Table Tennis": "table-tennis","table tennis": "table-tennis","table-tennis":"table-tennis",
    "Basketball": "basketball", "Tennis": "tennis", "Cricket": "cricket", 
    "Volleyball": "volleyball", "Rugby": "rugby", "Handball": "handball",
    "MMA": "mma", "Boxing": "boxing", "Darts": "darts", 
    "eSoccer": "esoccer", "eFootball": "esoccer", "Baseball": "baseball", "baseball": "baseball",
    "American Football": "american-football", "american football": "american-football", "american-football": "american-football",
}

_SSE_HEADERS = {
    "Content-Type": "text/event-stream", "Cache-Control": "no-cache",
    "X-Accel-Buffering": "no", "Access-Control-Allow-Origin": "*", "Connection": "keep-alive",
}

_POPULARITY_WEIGHTS = {
    "soccer": {"england": 1, "spain": 2, "germany": 3, "italy": 4, "france": 5, "brazil": 6, "argentina": 7, "netherlands": 8, "portugal": 9},
    "basketball": {"usa": 1, "spain": 2, "greece": 3, "turkey": 4, "italy": 5},
    "cricket": {"india": 1, "australia": 2, "england": 3, "pakistan": 4, "south africa": 5, "new zealand": 6},
    "tennis": {"atp": 1, "wta": 2, "challenger": 3, "itf": 4}
}