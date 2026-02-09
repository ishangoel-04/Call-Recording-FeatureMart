import logging
from pathlib import Path

from faster_whisper import WhisperModel

# Logging with timestamps (date + time)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# Audio path relative to project root
AUDIOS_DIR = Path(__file__).resolve().parent / "audios"
AUDIO_FILE = AUDIOS_DIR / "AT00071_7e0e065c-04d6-4c05-8f51-451225788f65.mp3"


def main():
    if not AUDIO_FILE.exists():
        log.error("Audio file not found: %s", AUDIO_FILE)
        return

    log.info("Loading Whisper model (large-v3)...")
    try:
        model = WhisperModel("large-v3", device="cuda", compute_type="float16")
    except Exception:
        log.warning("CUDA not available, falling back to CPU")
        model = WhisperModel("large-v3", device="cpu", compute_type="int8")

    log.info("Starting transcription: %s", AUDIO_FILE.name)
    segments, info = model.transcribe(str(AUDIO_FILE), beam_size=5)

    log.info("Language: %s (prob: %.2f)", info.language, info.language_probability)
    log.info("Duration: %.2fs", info.duration)

    full_text = []
    for segment in segments:
        print(f"[{segment.start:.2f}s --> {segment.end:.2f}s] {segment.text}")
        full_text.append(segment.text)

    full_transcript = " ".join(full_text).strip()
    log.info("Transcription complete. Segments: %d", len(full_text))
    if full_transcript:
        log.info("Full text length: %d chars", len(full_transcript))


if __name__ == "__main__":
    main()
