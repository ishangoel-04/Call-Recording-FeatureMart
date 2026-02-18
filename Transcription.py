import json
import os
from pathlib import Path

from dotenv import load_dotenv
from sarvamai import SarvamAI

load_dotenv()

SCRIPT_DIR = Path(__file__).resolve().parent
AUDIO_FILE = SCRIPT_DIR / os.getenv("AUDIO_FILE", "audios/audio.mp3").strip()
OUTPUT_FILE = SCRIPT_DIR / os.getenv("TRANSCRIPT_OUTPUT_FILE", "output.txt").strip()

client = SarvamAI(api_subscription_key=os.getenv("SARVAM_API_KEY"))

job = client.speech_to_text_job.create_job(
    model=os.getenv("TRANSCRIPTION_MODEL", "saaras:v3"),
    mode=os.getenv("TRANSCRIPTION_MODE", "translate"),
    language_code=os.getenv("TRANSCRIPTION_LANGUAGE_CODE", "en-IN"),
)

job.upload_files(file_paths=[str(AUDIO_FILE)])
job.start()
job.wait_until_complete()

job.download_outputs(output_dir=str(SCRIPT_DIR))

# Read the downloaded JSON (same name as audio + .json)
json_path = SCRIPT_DIR / f"{AUDIO_FILE.name}.json"
with open(json_path, "r", encoding="utf-8") as f:
    data = json.load(f)

transcript = data.get("transcript") or data.get("text") or ""
with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    f.write(transcript)

print(f"Saved to {OUTPUT_FILE}")
