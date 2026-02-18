import logging
import os
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pandas as pd
import requests
from dotenv import load_dotenv
from pydub import AudioSegment

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).resolve().parent
XLSX_PATH = SCRIPT_DIR / os.getenv("DATA_FILE", "Calling data jan.xlsx").strip()
OUTPUT_DIR = SCRIPT_DIR / os.getenv("FETCHED_AUDIOS_DIR", "fetched_audios").strip()

RECORDING_API_BASE = os.getenv("RECORDING_API_BASE", "https://apiprod.credgenics.com/calling/recording").strip()
AUTH_TOKEN = os.getenv("CREDGENICS_AUTHENTICATION_TOKEN", "").strip()
DEFAULT_COMPANY_ID = (os.getenv("CREDGENICS_DEFAULT_COMPANY_ID")).strip()
MIN_TALK_TIME_SECONDS = int(os.getenv("MIN_TALK_TIME_SECONDS", "10"))
MAX_RECORDINGS = int(os.getenv("MAX_RECORDINGS", "50"))
API_REQUEST_TIMEOUT = int(os.getenv("API_REQUEST_TIMEOUT", "30"))
DOWNLOAD_TIMEOUT = int(os.getenv("DOWNLOAD_TIMEOUT", "60"))

def _api_headers() -> dict:
    """Minimal headers for recording API (matches curl: authenticationtoken only)."""
    return {"authenticationtoken": AUTH_TOKEN}


def _talk_time_duration_to_seconds(value) -> float:
    """Convert total_talk_time_duration (e.g. '00:03:02' or '0:01:08') to seconds."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return 0.0
    s = str(value).strip()
    if not s:
        return 0.0
    try:
        parts = s.split(":")
        if len(parts) == 3:
            h, m, s_ = int(parts[0]), int(parts[1]), int(parts[2])
            return h * 3600 + m * 60 + s_
        if len(parts) == 2:
            m, s_ = int(parts[0]), int(parts[1])
            return m * 60 + s_
        return float(s)
    except (ValueError, IndexError):
        return 0.0


def recording_id_from_link(recording_link: str) -> str | None:
    """Extract recording ID from a recording_link URL (id query param)."""
    if not recording_link or not isinstance(recording_link, str):
        return None
    link = (recording_link or "").strip()
    if not link:
        return None
    try:
        parsed = urlparse(link)
        qs = parse_qs(parsed.query)
        ids = qs.get("id")
        if ids and ids[0]:
            return ids[0].strip()
    except Exception:
        pass
    return None


def get_recording_public_url(recording_id: str, company_id: str) -> str | None:
    """Call the recording API and return the public URL from response data."""
    url = f"{RECORDING_API_BASE}/{recording_id}"
    params = {"company_id": company_id}
    headers = _api_headers()
    try:
        r = requests.get(url, params=params, headers=headers, timeout=API_REQUEST_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        # API returns response with public link in the data field
        public_link = data.get("data")
        if isinstance(public_link, str) and public_link.strip():
            return public_link.strip()
        if isinstance(public_link, dict) and public_link.get("url"):
            return public_link["url"].strip()
        log.warning("No public link in response for %s: %s", recording_id, data)
        return None
    except requests.RequestException as e:
        log.warning("API request failed for %s: %s", recording_id, e)
        return None
    except Exception as e:
        log.warning("Unexpected error for %s: %s", recording_id, e)
        return None


def download_audio(public_url: str, dest_path: Path) -> bool:
    """Download audio file from public URL to dest_path."""
    try:
        r = requests.get(public_url, timeout=DOWNLOAD_TIMEOUT, stream=True, allow_redirects=True)
        r.raise_for_status()
        with open(dest_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
        return True
    except Exception as e:
        log.warning("Download failed for %s: %s", public_url[:60], e)
        return False


def _parse_ts(value) -> pd.Timestamp | None:
    """Parse timestamp from Excel (str, datetime, or NaN)."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    try:
        return pd.to_datetime(value)
    except Exception:
        return None


def _detect_audio_format(audio_path: Path) -> str:
    """Detect actual audio format from file header (API may return WAV with .mp3 extension)."""
    try:
        with open(audio_path, "rb") as f:
            header = f.read(12)
        if header[:4] == b"RIFF" and header[8:12] == b"WAVE":
            return "wav"
        if header[:3] == b"ID3" or (len(header) >= 2 and header[0] == 0xFF and (header[1] & 0xE0) == 0xE0):
            return "mp3"
        if header[:4] == b"fLaC":
            return "flac"
    except Exception:
        pass
    return audio_path.suffix.lower().lstrip(".") or "mp3"


def crop_audio_by_timestamps(
    audio_path: Path,
    call_start: pd.Timestamp | None,
    customer_pickup: pd.Timestamp | None,
    call_end: pd.Timestamp | None,
) -> bool:
    """Crop audio file from customer_call_pickup_time to call_end_time (relative to call_start)."""
    if call_start is None or customer_pickup is None or call_end is None:
        log.debug("Skipping crop: missing timestamps")
        return False
    start_sec = (customer_pickup - call_start).total_seconds()
    end_sec = (call_end - call_start).total_seconds()
    if start_sec < 0 or end_sec <= start_sec:
        log.debug("Skipping crop: invalid range start_sec=%s end_sec=%s", start_sec, end_sec)
        return False
    try:
        start_ms = int(start_sec * 1000)
        end_ms = int(end_sec * 1000)
        # Use detected format: API often returns WAV even when URL/extension is .mp3
        fmt = _detect_audio_format(audio_path)
        audio = AudioSegment.from_file(str(audio_path), format=fmt)
        cropped = audio[start_ms:end_ms]
        out_fmt = (audio_path.suffix.lower().lstrip(".")) or "mp3"
        cropped.export(str(audio_path), format=out_fmt)
        log.info("Cropped %s to %.1f–%.1f s", audio_path.name, start_sec, end_sec)
        return True
    except Exception as e:
        log.warning("Crop failed for %s: %s", audio_path.name, str(e).split("\n")[0][:120])
        return False


def main():
    if not AUTH_TOKEN:
        log.error(
            "Set CREDGENICS_AUTHENTICATION_TOKEN in .env (the authenticationtoken from the API)"
        )
        return

    if not XLSX_PATH.exists():
        log.error("Excel file not found: %s", XLSX_PATH)
        return

    df = pd.read_excel(XLSX_PATH, engine="openpyxl")
    if "recording_link" not in df.columns:
        log.error("Excel has no 'recording_link' column")
        return

    mask = df["recording_link"].notna() & (df["recording_link"].astype(str).str.strip() != "")
    rows = df.loc[mask]

    if "total_talk_time_duration" in df.columns:
        rows = rows.copy()
        rows["_talk_seconds"] = rows["total_talk_time_duration"].map(_talk_time_duration_to_seconds)
        rows = rows[rows["_talk_seconds"] > MIN_TALK_TIME_SECONDS]
        rows = rows.drop(columns=["_talk_seconds"], errors="ignore")
        log.info("Filtered to %d rows with total_talk_time_duration > %s seconds", len(rows), MIN_TALK_TIME_SECONDS)
    else:
        log.warning("No 'total_talk_time_duration' column; processing all rows with recording_link")

    if rows.empty:
        log.warning("No rows with recording_link (or none above %s s talk time)", MIN_TALK_TIME_SECONDS)
        return

    rows = rows.head(MAX_RECORDINGS)
    log.info("Limiting to %d recordings (MAX_RECORDINGS=%d)", len(rows), MAX_RECORDINGS)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    log.info("Found %d recordings; saving to %s", len(rows), OUTPUT_DIR)

    for idx, row in rows.iterrows():
        recording_link = (row["recording_link"] or "").strip()
        recording_id = recording_id_from_link(recording_link)
        if not recording_id:
            log.warning("Could not extract recording ID from: %s", recording_link[:80])
            continue

        company_id = (row.get("company_id") or DEFAULT_COMPANY_ID)
        if isinstance(company_id, float):
            company_id = DEFAULT_COMPANY_ID
        company_id = str(company_id).strip()

        log.info("Fetching recording %s (company_id=%s)", recording_id, company_id[:8] + "...")
        public_url = get_recording_public_url(recording_id, company_id)
        if not public_url:
            continue

        # Prefer .mp3; if URL has extension, use it
        ext = ".mp3"
        if "." in public_url.split("?")[0]:
            ext = "." + public_url.split("?")[0].rsplit(".", 1)[-1].lower()
        if ext not in (".mp3", ".wav", ".m4a", ".ogg", ".webm"):
            ext = ".mp3"

        dest_path = OUTPUT_DIR / f"{recording_id}{ext}"
        if download_audio(public_url, dest_path):
            log.info("Saved: %s", dest_path.name)
            call_start = _parse_ts(row.get("call_start_time"))
            customer_pickup = _parse_ts(row.get("customer_call_pickup_time"))
            call_end = _parse_ts(row.get("call_end_time"))
            crop_audio_by_timestamps(dest_path, call_start, customer_pickup, call_end)
        else:
            log.warning("Failed to download: %s", recording_id)

    log.info("Done. Output directory: %s", OUTPUT_DIR)


if __name__ == "__main__":
    main()
