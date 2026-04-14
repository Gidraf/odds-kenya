import os
import json
import logging
import traceback
import boto3
from botocore.exceptions import ClientError
from botocore.client import Config
from flask import Blueprint, jsonify, request
from openai import OpenAI

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

bp_story = Blueprint("ai_story", __name__, url_prefix="/api")

# Configuration 
MINIO_ENDPOINT = os.getenv("STORAGE_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("STORAGE_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("STORAGE_SECRET_KEY", "minioadmin")
BUCKET_NAME = "match-stories"
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

logger.info(f"Initializing AI Story Endpoint. MinIO: {MINIO_ENDPOINT}")

# Initialize S3 Client (MinIO)
try:
    s3_client = boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version='s3v4'),
        region_name='us-east-1' # Required by boto3 even for MinIO
    )
except Exception as e:
    logger.error(f"Failed to initialize boto3 client: {e}")

# Initialize OpenAI Client
try:
    client = OpenAI(api_key=OPENAI_API_KEY)
except Exception as e:
    logger.error(f"Failed to initialize OpenAI client: {e}")


def ensure_bucket_exists():
    """Create the MinIO bucket dynamically if it doesn't exist."""
    try:
        s3_client.head_bucket(Bucket=BUCKET_NAME)
        logger.info(f"Bucket '{BUCKET_NAME}' already exists.")
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code')
        if error_code == '404' or error_code == 'NoSuchBucket':
            try:
                logger.info(f"Bucket '{BUCKET_NAME}' not found. Creating it...")
                s3_client.create_bucket(Bucket=BUCKET_NAME)
                logger.info("Bucket created successfully.")
            except Exception as create_error:
                logger.error(f"Failed to create bucket: {create_error}")
                raise create_error
        else:
            logger.error(f"Head bucket error: {e}")
            raise e

@bp_story.route("/odds/match/story")
def get_match_story():
    home_team = request.args.get("home", "Home")
    away_team = request.args.get("away", "Away")
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
        response = s3_client.get_object(Bucket=BUCKET_NAME, Key=object_key)
        cached_data = json.loads(response['Body'].read().decode('utf-8'))
        logger.info("Successfully fetched from MinIO cache.")
        return jsonify({"source": "minio_cache", "data": cached_data})
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            logger.info("Cache miss. Proceeding to generate via OpenAI.")
        else:
            logger.warning(f"MinIO Read Error (not a cache miss): {e}")
    except Exception as e:
        logger.error(f"Unexpected MinIO error: {traceback.format_exc()}")

    # 2. If not in MinIO, generate via OpenAI
    prompt = f"""
    You are a master football historian and data storyteller. Analyze the matchup between {home_team} and {away_team}.
    Provide a deeply engaging JSON response containing the following exact keys:
    
    - "trophies_overview": A brief comparison of their historical silverware.
    - "golden_era_comparison": Compare the current squads to their respective historical 'golden era' teams.
    - "manager_comparison": Compare the historical achievements and playstyles of their current managers vs their legendary past managers.
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
        
        # 3. Store the generated JSON in MinIO for future requests
        try:
            s3_client.put_object(
                Bucket=BUCKET_NAME,
                Key=object_key,
                Body=json.dumps(story_data),
                ContentType="application/json"
            )
            logger.info("Successfully saved generated story to MinIO.")
        except Exception as put_error:
            logger.error(f"Failed to save to MinIO: {put_error}")
            # We don't raise here, we still want to return the generated data to the user
            
        return jsonify({"source": "openai_generated", "data": story_data})
        
    except Exception as e:
        logger.error(f"OpenAI Generation Failed: {traceback.format_exc()}")
        return jsonify({"error": "AI Generation Failed", "traceback": traceback.format_exc()}), 500