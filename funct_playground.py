# sportpesa list
def parse_match_list(raw_data):
    parsed_matches = []

    try:
        if not isinstance(raw_data, list):
            return []
            
        for item in raw_data:
            try:
                parent_match_id = item['betradarId']
                if parent_match_id is None:
                    continue
            except Exception:
                continue
                
            try:
                competitors = item['competitors']
                try:
                    home_competitor = competitors[0]
                    home_team = home_competitor['name']
                    if not home_team:
                        continue
                except Exception:
                    continue
                    
                try:
                    away_competitor = competitors[1]
                    away_team = away_competitor['name']
                    if not away_team:
                        continue
                except Exception:
                    continue
            except Exception:
                continue
                
            try:
                start_time = item['date']
            except Exception:
                start_time = None
                
            try:
                sport_info = item['sport']
                sport = sport_info['name']
            except Exception:
                sport = None
                
            try:
                comp_info = item['competition']
                competition = comp_info['name']
            except Exception:
                competition = None
                
            try:
                markets = item['markets']
            except Exception:
                continue
                
            for market in markets:
                try:
                    market_name = market['name']
                    if not market_name:
                        continue
                except Exception:
                    continue
                    
                try:
                    selections = market['selections']
                except Exception:
                    continue
                    
                for selection in selections:
                    try:
                        selection_name = selection['name']
                        if not selection_name:
                            continue
                    except Exception:
                        continue
                        
                    try:
                        price_val = selection['odds']
                        price = float(price_val)
                        if price <= 1.0:
                            continue
                    except Exception:
                        continue
                        
                    try:
                        spec_value = selection['specValue']
                        if spec_value == 0 or spec_value == '0':
                            specifier = None
                        else:
                            specifier = str(spec_value)
                    except Exception:
                        specifier = None
                        
                    try:
                        parsed_matches.append({
                            'parent_match_id': parent_match_id,
                            'home_team': home_team,
                            'away_team': away_team,
                            'market': market_name,
                            'selection': selection_name,
                            'price': price,
                            'start_time': start_time,
                            'sport': sport,
                            'competition': competition,
                            'specifier': specifier
                        })
                    except Exception:
                        continue
                        
    except Exception:
        return []
        
    return parsed_matches

def parse_match_list(raw_data):
    try:
        out = []
        if type(raw_data) is not list:
            return out

        for item in raw_data:
            try:
                parent_match_id = item["betradarId"]
            except Exception:
                continue
                
            try:
                home_team = item["competitors"][0]["name"]
            except Exception:
                continue
                
            try:
                away_team = item["competitors"][1]["name"]
            except Exception:
                continue
                
            try:
                start_time = item["date"]
            except Exception:
                start_time = None
                
            try:
                sport = item["sport"]["name"]
            except Exception:
                sport = None
                
            try:
                competition = item["competition"]["name"]
            except Exception:
                competition = None
                
            try:
                markets = item["markets"]
            except Exception:
                continue
                
            for m in markets:
                try:
                    market_name = m["name"]
                except Exception:
                    continue
                    
                try:
                    selections = m["selections"]
                except Exception:
                    continue
                    
                for s in selections:
                    try:
                        selection_name = s["name"]
                    except Exception:
                        continue
                        
                    try:
                        price = float(s["odds"])
                        if price <= 1.0:
                            continue
                    except Exception:
                        continue
                        
                    try:
                        spec = s["specValue"]
                        if spec == 0 or spec == 0.0 or spec == "0" or spec == "":
                            specifier = None
                        else:
                            specifier = str(spec)
                    except Exception:
                        specifier = None
                        
                    try:
                        out.append({
                            "parent_match_id": parent_match_id,
                            "home_team": home_team,
                            "away_team": away_team,
                            "market": market_name,
                            "selection": selection_name,
                            "price": price,
                            "start_time": start_time,
                            "sport": sport,
                            "competition": competition,
                            "specifier": specifier
                        })
                    except Exception:
                        continue
                        
        return out
    except Exception:
        return []