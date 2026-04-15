import os
import json
import logging
import traceback
import io
from flask import Blueprint, jsonify, request
from openai import OpenAI
from minio import Minio
from minio.error import S3Error

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

bp_story = Blueprint("ai_story", __name__, url_prefix="/api")

# Configuration 
MINIO_ENDPOINT = os.getenv("STORAGE_ENDPOINT", "localhost:9000") # Format: "127.0.0.1:9000" (No http://)
MINIO_ACCESS_KEY = os.getenv("STORAGE_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("STORAGE_SECRET_KEY", "minioadmin")
BUCKET_NAME = "match-stories"
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

is_secure = MINIO_ENDPOINT.startswith("https")
clean_endpoint = MINIO_ENDPOINT.replace("http://", "").replace("https://", "")

logger.info(f"Initializing AI Story Endpoint. MinIO: {clean_endpoint}")

# Initialize MinIO Client
try:
    minio_client = Minio(
        clean_endpoint,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=is_secure
    )
except Exception as e:
    logger.error(f"Failed to initialize MinIO client: {e}")

# Initialize OpenAI Client
try:
    client = OpenAI(api_key=OPENAI_API_KEY)
except Exception as e:
    logger.error(f"Failed to initialize OpenAI client: {e}")

def ensure_bucket_exists():
    """Create the MinIO bucket dynamically if it doesn't exist."""
    try:
        found = minio_client.bucket_exists(BUCKET_NAME)
        if not found:
            logger.info(f"Bucket '{BUCKET_NAME}' not found. Creating it...")
            minio_client.make_bucket(BUCKET_NAME)
            logger.info("Bucket created successfully.")
        else:
            logger.info(f"Bucket '{BUCKET_NAME}' already exists.")
    except S3Error as e:
        logger.error(f"MinIO Bucket Error: {e}")
        raise e

@bp_story.route("/odds/match/story")
def get_match_story():
    home_team = request.args.get("home", "Home")
    away_team = request.args.get("away", "Away")
    
    if home_team == "Home" and away_team == "Away":
        return jsonify({"error": "Invalid team names provided"}), 400

    logger.info(f"Received story request for {home_team} vs {away_team}")
    
    try:
        ensure_bucket_exists()
    except Exception as e:
        logger.error(f"MinIO Bucket Check Failed: {traceback.format_exc()}")
        return jsonify({"error": "Storage configuration failed", "details": str(e)}), 500
    
    object_key = f"{home_team.lower().replace(' ', '_')}_vs_{away_team.lower().replace(' ', '_')}.json"
    
    # 1. Try to fetch from MinIO
    try:
        logger.info(f"Attempting to fetch {object_key} from MinIO...")
        response = minio_client.get_object(BUCKET_NAME, object_key)
        cached_data = json.loads(response.read().decode('utf-8'))
        response.close()
        response.release_conn()
        logger.info("Successfully fetched from MinIO cache.")
        return jsonify({"source": "minio_cache", "data": cached_data})
    except S3Error as e:
        if e.code == 'NoSuchKey':
            logger.info("Cache miss. Proceeding to generate via OpenAI.")
        else:
            logger.warning(f"MinIO Read Error: {e}")
    except Exception as e:
        logger.error(f"Unexpected MinIO error: {traceback.format_exc()}")

    # 2. If not in MinIO, generate via OpenAI
    prompt = f"""
    You are a master football historian and data storyteller. Analyze the matchup between {home_team} and {away_team}.
    Provide a deeply engaging JSON response containing the following exact keys:
    
    - "trophies_overview": A brief comparison of their historical silverware.
    - "golden_era_comparison": Compare the current squads to their respective historical 'golden era' teams.
    - "manager_comparison": Compare the historical achievements and playstyles of their current managers vs legendary past managers.
    - "extraordinary_events": Mention one crazy, rare, or infamous historical event that happened to either team or during this specific fixture.
    - "luck_analogy": A creative, slightly humorous analogy about the "luck" or "curse" surrounding these two teams right now.

    Return ONLY valid JSON matching this structure.
    """

    try:
        logger.info("Sending request to OpenAI API...")
        completion = client.chat.completions.create(
            model="gpt-4o-mini", 
            messages=[
                {"role": "system", "content": "You output strict JSON. No markdown formatting blocks."},
                {"role": "user", "content": prompt}
            ],
            response_format={ "type": "json_object" }
        )
        
        story_data = json.loads(completion.choices[0].message.content)
        logger.info("Successfully generated story from OpenAI.")
        
        # 3. Store the generated JSON in MinIO 
        try:
            json_bytes = json.dumps(story_data).encode('utf-8')
            minio_client.put_object(
                BUCKET_NAME,
                object_key,
                data=io.BytesIO(json_bytes),
                length=len(json_bytes),
                content_type="application/json"
            )
            logger.info("Successfully saved generated story to MinIO.")
        except Exception as put_error:
            logger.error(f"Failed to save to MinIO: {put_error}")
            
        return jsonify({"source": "openai_generated", "data": story_data})
        
    except Exception as e:
        logger.error(f"OpenAI Generation Failed: {traceback.format_exc()}")
        return jsonify({"error": "AI Generation Failed", "traceback": traceback.format_exc()}), 500