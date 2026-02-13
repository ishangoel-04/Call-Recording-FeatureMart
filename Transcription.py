import json
import logging
import os
import tempfile
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv
from faster_whisper import WhisperModel

load_dotenv()

# Logging with timestamps (date + time)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).resolve().parent
CSV_PATH = SCRIPT_DIR / "sql_1_2026-02-11T1157.csv"
OUTPUT_EXCEL = SCRIPT_DIR / "transcriptions_output.xlsx"

# Credgenics API: client_id + client_secret -> access_token (Bearer) for downloads
AUTH_URL = "https://apiprod.credgenics.com/user/public/access-token"
CREDGENICS_CLIENT_ID = os.getenv("CREDGENICS_CLIENT_ID", "").strip()
CREDGENICS_CLIENT_SECRET = os.getenv("CREDGENICS_CLIENT_SECRET", "").strip()
CREDGENICS_TOKEN_EXPIRY = int(os.getenv("CREDGENICS_TOKEN_EXPIRY", "900"))


def get_access_token() -> str | None:
    """Obtain access token from Credgenics API. Returns token or None on failure."""
    if not CREDGENICS_CLIENT_ID or not CREDGENICS_CLIENT_SECRET:
        log.error("Set CREDGENICS_CLIENT_ID and CREDGENICS_CLIENT_SECRET in .env")
        return None
    payload = json.dumps({
        "client_id": CREDGENICS_CLIENT_ID,
        "client_secret": CREDGENICS_CLIENT_SECRET,
        "token_expiry_duration": CREDGENICS_TOKEN_EXPIRY,
    })
    headers = {"accept": "application/json", "content-type": "application/json"}
    try:
        r = requests.post(AUTH_URL, headers=headers, data=payload, timeout=15)
        r.raise_for_status()
        data = r.json()
        token = data.get("access_token")
        if token:
            log.info("Access token obtained.")
            return token
        log.error("No access_token in response: %s", data)
        return None
    except Exception as e:
        log.error("Failed to get access token: %s", e)
        return None


def download_recording(url: str, dest_path: Path, *, access_token: str | None = None) -> tuple[bool, str]:
    """Download recording from URL to dest_path. Uses Bearer token if provided."""
    try:
        headers = {}
        if access_token:
            headers["Authorization"] = f"Bearer {access_token}"
            headers["Accept"] = "application/octet-stream, audio/*, */*"
        r = requests.get(url, headers=headers or None, timeout=60, stream=True, allow_redirects=True)
        r.raise_for_status()

        content_type = (r.headers.get("Content-Type") or "").lower()
        if "text/html" in content_type or "application/json" in content_type:
            return False, f"Unexpected content-type: {content_type or 'unknown'} (auth required?)"

        with open(dest_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
        return True, ""
    except Exception as e:
        msg = str(e)
        log.warning("Download failed for %s: %s", url[:60], msg)
        return False, msg


def transcribe_file(model, audio_path: Path):
    """Run faster-whisper on audio file. Returns (full_transcript, language, duration, error)."""
    try:
        segments, info = model.transcribe(str(audio_path), beam_size=5)
        full_text = []
        for segment in segments:
            full_text.append(segment.text)
        transcript = " ".join(full_text).strip()
        duration = getattr(info, "duration", None) or getattr(info, "duration_seconds", 0)
        return transcript, info.language, duration, None
    except Exception as e:
        return "", "", 0, str(e)


def main():
    if not CSV_PATH.exists():
        log.error("CSV not found: %s", CSV_PATH)
        return

    df = pd.read_csv(CSV_PATH)
    if "recording_link" not in df.columns:
        log.error("CSV has no 'recording_link' column")
        return

    mask = df["recording_link"].notna() & (df["recording_link"].astype(str).str.strip() != "")
    links_df = df.loc[mask].copy()
    if links_df.empty:
        log.warning("No rows with recording_link in CSV")
        return

    log.info("Found %d recordings to process", len(links_df))

    access_token = None
    log.info("Obtaining Credgenics access token...")
    access_token = get_access_token()
    if not access_token:
        log.error("Could not obtain access token. Check CREDGENICS_CLIENT_ID and CREDGENICS_CLIENT_SECRET in .env")
        return

    log.info("Loading Whisper model (large-v3)...")
    try:
        model = WhisperModel("large-v3", device="cuda", compute_type="float16")
    except Exception:
        log.warning("CUDA not available, falling back to CPU")
        model = WhisperModel("large-v3", device="cpu", compute_type="int8")

    results = []
    for idx, row in links_df.iterrows():
        event_id = row.get("event_id", "")
        loan_id = row.get("loan_id", "")
        url = (row["recording_link"] or "").strip()
        log.info("Processing %s (%s): %s", event_id, loan_id, url[:70] + "..." if len(url) > 70 else url)

        with tempfile.NamedTemporaryFile(suffix=".mp3", delete=False) as tmp:
            tmp_path = Path(tmp.name)
        try:
            ok, dl_err = download_recording(url, tmp_path, access_token=access_token)
            if not ok:
                results.append({
                    "event_id": event_id,
                    "loan_id": loan_id,
                    "recording_link": url,
                    "transcript": "",
                    "language": "",
                    "duration_seconds": 0,
                    "error": f"Download failed: {dl_err}",
                })
                continue
            transcript, language, duration, err = transcribe_file(model, tmp_path)
            results.append({
                "event_id": event_id,
                "loan_id": loan_id,
                "recording_link": url,
                "transcript": transcript,
                "language": language,
                "duration_seconds": duration,
                "error": err or "",
            })
            log.info("Transcribed: language=%s, duration=%.2fs, chars=%d", language, duration, len(transcript))
        finally:
            if tmp_path.exists():
                tmp_path.unlink()

    out_df = pd.DataFrame(results)
    out_df.to_excel(OUTPUT_EXCEL, index=False, engine="openpyxl")
    log.info("Saved %d transcriptions to %s", len(out_df), OUTPUT_EXCEL)


if __name__ == "__main__":
    main()
