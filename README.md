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
