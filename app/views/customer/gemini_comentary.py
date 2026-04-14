import logging, os, json, base64, asyncio, io
from flask import Blueprint, request, jsonify, Response
from openai import OpenAI
import edge_tts

try:
    from minio import Minio
    _minio = Minio(
        os.environ.get("MINIO_ENDPOINT", "localhost:9000"),
        access_key=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
        secret_key=os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
        secure=os.environ.get("MINIO_SECURE", "false").lower() == "true",
    )
    if not _minio.bucket_exists("sport"):
        _minio.make_bucket("sport")
    MINIO_OK = True
except Exception as e:
    logging.warning(f"MinIO unavailable: {e}")
    MINIO_OK = False

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("commentary")

bp_commentary = Blueprint("commentary", __name__, url_prefix="/api")
_client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY", ""))

async def _generate_edge_tts(text: str, voice: str) -> bytes:
    communicate = edge_tts.Communicate(text, voice)
    audio_data = b""
    async for chunk in communicate.stream():
        if chunk["type"] == "audio":
            audio_data += chunk["data"]
    return audio_data

# ── 1. Fetch AI Script (No Audio Generation Yet) ──
@bp_commentary.route("/odds/match/<betradar_id>/commentary", methods=["GET"])
def get_commentary(betradar_id: str):
    sport = request.args.get("sport", "soccer")
    
    prompt = f"""
    You are a sports broadcasting AI. Write a thrilling 5-scene cinematic script for match {betradar_id}.
    Return exactly JSON matching this structure:
    {{
      "scenes": [
         {{"id": "intro", "text": "Welcome everyone...", "text_b": "It's a huge night!"}},
         {{"id": "h2h", "text": "Looking at the history...", "text_b": "They have struggled here."}},
         {{"id": "form", "text": "Recent form is crucial...", "text_b": "Yes, 3 wins in 5."}},
         {{"id": "managers", "text": "The tactical battle...", "text_b": "Let's look at the setups."}},
         {{"id": "closing", "text": "Kickoff is next!", "text_b": "Don't go anywhere."}}
      ]
    }}
    """
    
    try:
        resp = _client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a football broadcasting API. JSON only."},
                {"role": "user", "content": prompt}
            ],
            response_format={"type": "json_object"}
        )
        script_data = json.loads(resp.choices[0].message.content)
    except Exception as e:
        log.error(f"Script Gen Error: {e}")
        script_data = {"scenes": []}

    # Format output for the frontend Editor
    for sc in script_data.get("scenes", []):
        sc["audio_a"] = None
        sc["audio_b"] = None
        sc["audio_url_a"] = None
        sc["audio_url_b"] = None
        sc["loading_audio"] = False

    return jsonify({"scenes": script_data.get("scenes", [])})

# ── 2. Manual Generation of Audio via Edge-TTS ──
@bp_commentary.route("/odds/match/<betradar_id>/generate_commentary", methods=["POST"])
def generate_manual_audio(betradar_id):
    data = request.json
    text_a = data.get("text_a", "")
    text_b = data.get("text_b", "")
    voice_a = data.get("voice_a", "en-GB-RyanNeural")
    voice_b = data.get("voice_b", "en-GB-SoniaNeural")
    scene_id = data.get("scene_id", "custom")

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    res = {}
    
    try:
        if text_a:
            audio_bytes_a = loop.run_until_complete(_generate_edge_tts(text_a, voice_a))
            b64_a = base64.b64encode(audio_bytes_a).decode('utf-8')
            res["audio_a_b64"] = b64_a
            
            if MINIO_OK:
                key_a = f"commentary/{betradar_id}_{scene_id}_a.mp3"
                _minio.put_object("sport", key_a, io.BytesIO(audio_bytes_a), len(audio_bytes_a), content_type="audio/mpeg")
                res["audio_url_a"] = f"/api/audio/{key_a}"

        if text_b:
            audio_bytes_b = loop.run_until_complete(_generate_edge_tts(text_b, voice_b))
            b64_b = base64.b64encode(audio_bytes_b).decode('utf-8')
            res["audio_b_b64"] = b64_b
            
            if MINIO_OK:
                key_b = f"commentary/{betradar_id}_{scene_id}_b.mp3"
                _minio.put_object("sport", key_b, io.BytesIO(audio_bytes_b), len(audio_bytes_b), content_type="audio/mpeg")
                res["audio_url_b"] = f"/api/audio/{key_b}"

    finally:
        loop.close()
        
    return jsonify(res)

# ── 3. MinIO File Serving ──
@bp_commentary.route("/audio/<path:key>")
def get_audio(key: str):
    if not MINIO_OK: return jsonify({"error": "MinIO disabled"}), 404
    try:
        resp = _minio.get_object("sport", key)
        data = resp.read()
        resp.close(); resp.release_conn()
        return Response(data, mimetype="audio/mpeg")
    except Exception as e:
        return jsonify({"error": "Not found"}), 404