"""
Transcribe all audio files from an input folder using Sarvam AI Batch API.
Processes in batches of up to 20 files per job (per API limit).
Saves .txt (transcript) and .json (full response) per file in an output folder,
using the same base name as each audio file.
See: https://docs.sarvam.ai/api-reference-docs/api-guides-tutorials/speech-to-text/batch-api
"""

import json
import logging
import os
from pathlib import Path

from dotenv import load_dotenv
from sarvamai import SarvamAI

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).resolve().parent
INPUT_DIR = SCRIPT_DIR / os.getenv("TRANSCRIPT_INPUT_DIR", "fetched_audios").strip()
OUTPUT_DIR = SCRIPT_DIR / os.getenv("TRANSCRIPT_OUTPUT_DIR", "transcription_output").strip()
BATCH_SIZE = int(os.getenv("TRANSCRIPT_BATCH_SIZE", "20"))

AUDIO_EXTENSIONS = {".mp3", ".wav", ".m4a", ".ogg", ".webm"}

client = SarvamAI(api_subscription_key=os.getenv("SARVAM_API_KEY"))


def process_batch_outputs(output_dir: Path, batch_paths: list[Path]) -> None:
    """Read SDK-downloaded JSONs ({filename}.json), write .txt and {stem}.json per file."""
    for audio_path in batch_paths:
        stem = audio_path.stem
        sdk_json_path = output_dir / f"{audio_path.name}.json"
        if not sdk_json_path.exists():
            continue
        with open(sdk_json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        transcript = data.get("transcript") or data.get("text") or ""
        txt_path = output_dir / f"{stem}.txt"
        json_path = output_dir / f"{stem}.json"
        with open(txt_path, "w", encoding="utf-8") as f:
            f.write(transcript)
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        if sdk_json_path != json_path:
            sdk_json_path.unlink()
        log.info("Saved %s and %s", txt_path.name, json_path.name)


def main():
    if not os.getenv("SARVAM_API_KEY", "").strip():
        log.error("Set SARVAM_API_KEY in .env")
        return

    if not INPUT_DIR.exists():
        log.error("Input folder not found: %s", INPUT_DIR)
        return

    audio_files = sorted(
        f for f in INPUT_DIR.iterdir()
        if f.is_file() and f.suffix.lower() in AUDIO_EXTENSIONS
    )
    if not audio_files:
        log.warning("No audio files in %s", INPUT_DIR)
        return

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    num_batches = (len(audio_files) + BATCH_SIZE - 1) // BATCH_SIZE
    log.info(
        "Transcribing %d files in %d batch(es) of up to %d (from %s -> %s)",
        len(audio_files), num_batches, BATCH_SIZE, INPUT_DIR.name, OUTPUT_DIR.name,
    )

    for i in range(0, len(audio_files), BATCH_SIZE):
        batch = audio_files[i : i + BATCH_SIZE]
        batch_num = (i // BATCH_SIZE) + 1
        log.info("Batch %d/%d: %d files", batch_num, num_batches, len(batch))

        job = client.speech_to_text_job.create_job(
            model=os.getenv("TRANSCRIPTION_MODEL", "saaras:v3"),
            mode=os.getenv("TRANSCRIPTION_MODE", "translate"),
            language_code=os.getenv("TRANSCRIPTION_LANGUAGE_CODE", "en-IN"),
        )
        job.upload_files(file_paths=[str(p) for p in batch])
        job.start()
        job.wait_until_complete()

        file_results = job.get_file_results()
        for f in file_results.get("failed", []):
            log.warning("Failed %s: %s", f.get("file_name"), f.get("error_message", ""))

        job.download_outputs(output_dir=str(OUTPUT_DIR))
        process_batch_outputs(OUTPUT_DIR, batch)

    log.info("Done. Output folder: %s", OUTPUT_DIR)


if __name__ == "__main__":
    main()
