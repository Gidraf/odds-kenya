import os
import json
import boto3
from botocore.exceptions import ClientError
from flask import Blueprint, jsonify, request
from openai import OpenAI

bp_story = Blueprint("ai_story", __name__, url_prefix="/api")

# Configuration (Ensure these are set in your environment)
MINIO_ENDPOINT = os.getenv("STORAGE_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("STORAGE_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("STORAGE_SECRET_KEY", "minioadmin")
BUCKET_NAME = "match-stories"
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# Initialize S3 Client (MinIO)
s3_client = boto3.client(
    's3',
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY
)

# Initialize OpenAI Client
client = OpenAI(api_key=OPENAI_API_KEY)

def ensure_bucket_exists():
    """Create the MinIO bucket dynamically if it doesn't exist."""
    try:
        s3_client.head_bucket(Bucket=BUCKET_NAME)
    except ClientError:
        try:
            s3_client.create_bucket(Bucket=BUCKET_NAME)
        except ClientError as e:
            print(f"Error creating bucket: {e}")

@bp_story.route("/odds/match/story")
def get_match_story():
    home_team = request.args.get("home", "Home")
    away_team = request.args.get("away", "Away")
    
    ensure_bucket_exists()
    
    # Create a consistent cache key
    object_key = f"{home_team.lower().replace(' ', '_')}_vs_{away_team.lower().replace(' ', '_')}.json"
    
    # 1. Try to fetch from MinIO
    try:
        response = s3_client.get_object(Bucket=BUCKET_NAME, Key=object_key)
        cached_data = json.loads(response['Body'].read().decode('utf-8'))
        return jsonify({"source": "minio_cache", "data": cached_data})
    except ClientError as e:
        if e.response['Error']['Code'] != 'NoSuchKey':
            print(f"MinIO read error: {e}")

    # 2. If not in MinIO, generate via OpenAI
    prompt = f"""
    You are a master football historian and data storyteller. Analyze the matchup between {home_team} and {away_team}.
    Provide a deeply engaging JSON response containing the following exact keys:
    
    - "trophies_overview": A brief comparison of their historical silverware.
    - "golden_era_comparison": Compare the current squads to their respective historical 'golden era' teams (e.g., Manchester United 2008, Arsenal Invincibles, etc.).
    - "manager_comparison": Compare the historical achievements and playstyles of their current managers vs their legendary past managers.
    - "extraordinary_events": Mention one crazy, rare, or infamous historical event that happened to either team or during this specific fixture.
    - "luck_analogy": A creative, slightly humorous analogy about the "luck" or "curse" surrounding these two teams right now.

    Return ONLY valid JSON matching this structure.
    """

    try:
        completion = client.chat.completions.create(
            model="gpt-4o-mini", # Cheapest, fastest model suitable for this task
            messages=[
                {"role": "system", "content": "You output strict JSON. No markdown formatting blocks."},
                {"role": "user", "content": prompt}
            ],
            response_format={ "type": "json_object" }
        )
        
        story_data = json.loads(completion.choices[0].message.content)
        
        # 3. Store the generated JSON in MinIO for future requests
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=object_key,
            Body=json.dumps(story_data),
            ContentType="application/json"
        )
        
        return jsonify({"source": "openai_generated", "data": story_data})
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500