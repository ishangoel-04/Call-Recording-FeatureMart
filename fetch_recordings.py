import logging
import os
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).resolve().parent
CSV_PATH = SCRIPT_DIR / "sql_1_2026-02-11T1157.csv"
OUTPUT_DIR = SCRIPT_DIR / "fetched_audios"

# Credgenics recording API (from your curl)
RECORDING_API_BASE = "https://apiprod.credgenics.com/calling/recording"
AUTH_TOKEN = os.getenv("CREDGENICS_AUTHENTICATION_TOKEN", "").strip()
DEFAULT_COMPANY_ID = "518ac0be-1de4-4339-a09e-28ea63b98cd6"

# Headers matching the curl request
def _api_headers(company_id: str) -> dict:
    return {
        "accept": "application/json, text/plain, */*",
        "accept-language": "en-GB,en-US;q=0.9,en;q=0.8",
        "authenticationtoken": AUTH_TOKEN,
        "cache-control": "no-cache",
        "origin": "https://app.credgenics.com",
        "pragma": "no-cache",
        "referer": "https://app.credgenics.com/",
        "sec-ch-ua": '"Not(A:Brand";v="8", "Chromium";v="144", "Google Chrome";v="144"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Android"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
        "user-agent": "Mozilla/5.0 (Linux; Android) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36 CrKey/1.54.248666",
        "x-company-id": company_id,
    }


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
    headers = _api_headers(company_id)
    try:
        r = requests.get(url, params=params, headers=headers, timeout=30)
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
        r = requests.get(public_url, timeout=60, stream=True, allow_redirects=True)
        r.raise_for_status()
        with open(dest_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
        return True
    except Exception as e:
        log.warning("Download failed for %s: %s", public_url[:60], e)
        return False


def main():
    if not AUTH_TOKEN:
        log.error(
            "Set CREDGENICS_AUTHENTICATION_TOKEN in .env (the authenticationtoken from the API)"
        )
        return

    if not CSV_PATH.exists():
        log.error("CSV not found: %s", CSV_PATH)
        return

    df = pd.read_csv(CSV_PATH)
    if "recording_link" not in df.columns:
        log.error("CSV has no 'recording_link' column")
        return

    mask = df["recording_link"].notna() & (df["recording_link"].astype(str).str.strip() != "")
    rows = df.loc[mask]
    if rows.empty:
        log.warning("No rows with recording_link in CSV")
        return

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
        else:
            log.warning("Failed to download: %s", recording_id)

    log.info("Done. Output directory: %s", OUTPUT_DIR)


if __name__ == "__main__":
    main()
