#!/bin/bash
API="http://5.78.137.59:5500/api"
DB_CHECK='psql $DATABASE_URL -c "SELECT b.name, count(*) as matches FROM bookmaker_match_odds bmo JOIN bookmakers b ON b.id=bmo.bookmaker_id GROUP BY b.name ORDER BY matches DESC;"'

for BK in sp bt od b2b; do
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo "▶ Triggering $BK harvest..."
  
  RESP=$(curl -s -X POST $API/harvest/trigger \
    -H 'Content-Type: application/json' \
    -d "{\"bookmaker\":\"$BK\",\"sport\":\"soccer\",\"mode\":\"upcoming\"}")
  
  echo "$RESP" | python3 -m json.tool
  HID=$(echo "$RESP" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('harvest_id',''))")
  
  echo "⏳ Waiting 30s for harvest + persist to complete..."
  sleep 30
  
  echo "📊 DB coverage after $BK:"
  eval $DB_CHECK
  echo ""
done

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "✅ Final check — matches with multiple bookmakers:"
psql $DATABASE_URL -c "
SELECT 
  count(*) FILTER (WHERE bk_count = 1) as single_bk,
  count(*) FILTER (WHERE bk_count = 2) as two_bk,
  count(*) FILTER (WHERE bk_count >= 3) as three_plus_bk
FROM (
  SELECT match_id, count(DISTINCT bookmaker_id) as bk_count
  FROM bookmaker_match_odds
  GROUP BY match_id
) t;"