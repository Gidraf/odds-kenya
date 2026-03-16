import openai
import json
from flask import current_app

client = openai.OpenAI(
        api_key=os.environ.get("GEMINI_API_KEY", ""),
        base_url="https://generativelanguage.googleapis.com/v1beta/openai/",
    )

def generate_parser(bookmaker_name, sample_json):
    """Uses OpenAI to create a Python dict-mapping function."""
    
    
    prompt = f"""
    Write a Python function `parse_data(raw_json_str)` that parses the following JSON from {bookmaker_name}.
    It MUST return a list of dictionaries exactly matching this format:
    [{{ "sport": "Soccer", "competition": "Premier League", "home_team": "Team A", "away_team": "Team B", "start_time": "2026-03-02T15:00:00Z", "market": "1X2", "selection": "Home", "price": 2.10 }}]
    
    Raw JSON:
    {sample_json[:2000]} # Limit tokens
    
    Return ONLY valid python code. No markdown.
    """
    
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "system", "content": prompt}]
    )
    return response.choices[0].message.content.replace('```python', '').replace('```', '').strip()

def test_parser(code, sample_json):
    """Safely executes the string code to verify it works."""
    local_scope = {}
    try:
        exec(code, {"json": json}, local_scope)
        result = local_scope['parse_data'](sample_json)
        return True, result
    except Exception as e:
        return False, str(e)