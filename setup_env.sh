#!/usr/bin/env bash
# Install Python deps (activate venv first: source venv/bin/activate)
pip install -r requirements.txt

# Install ffmpeg
if command -v apt-get &>/dev/null; then
  sudo apt-get update && sudo apt-get install -y ffmpeg
elif command -v brew &>/dev/null; then
  brew install ffmpeg
fi
