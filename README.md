# Call-Recording-FeatureMart

## Installation

```bash
source venv/bin/activate
pip install -r requirements.txt
# Optional: install ffmpeg
bash setup_env.sh
```

## Run transcription

**Option 1 – activate venv then run (recommended)**  
Without this, you get `ModuleNotFoundError: No module named 'faster_whisper'` because system Python is used.

```bash
source venv/bin/activate
python Transcription.py
```

**Option 2 – use the run script (no need to activate)**  
```bash
./run.sh
# or
bash run.sh
```

### Credgenics auth

The script uses the **Credgenics API**:

1. POST to `https://apiprod.credgenics.com/user/public/access-token` with `client_id`, `client_secret`, and optional `token_expiry_duration` (default 900 seconds).
2. Uses the returned `access_token` as `Authorization: Bearer <token>` for each recording download.

Set in `.env`: `CREDGENICS_CLIENT_ID` and `CREDGENICS_CLIENT_SECRET` (obtain from Credgenics Support / your CSM; valid 6 months).
