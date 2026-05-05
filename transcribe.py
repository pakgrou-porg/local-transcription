import os
import logging
import math
import shutil
import subprocess
import tempfile
import requests
from pathlib import Path


logger = logging.getLogger(__name__)
_TRANSCRIBE_SELECTED_MODEL_ID = None


class TranscriptionError(Exception):
    """Raised when transcription service fails."""
    pass


def get_max_upload_size_mb():
    """Return the configured max audio upload size for transcription requests."""
    return float(os.getenv("TRANSCRIBE_MAX_FILE_SIZE_MB", "24"))


def verify_transcript(transcript):
    """
    Verify that a transcript is valid and not placeholder/corrupt data.
    
    Verification rules:
    - Length must be > 50 characters (stripped)
    - Must not start with "{" (JSON)
    - Must not contain "action items:" (marker of auto-generated summary)
    - Must not contain "discussion topics:" (marker of auto-generated summary)
    - Must not start with "meeting subject:" (marker of auto-generated summary)
    
    Args:
        transcript (str): Text to verify
        
    Returns:
        bool: True if transcript passes all checks, False otherwise
    """
    if not isinstance(transcript, str):
        return False
    
    stripped = transcript.strip()
    
    # Check minimum length
    if len(stripped) <= 50:
        logger.warning(f"Transcript too short (len={len(stripped)}): {stripped[:50]}")
        return False
    
    # Check for JSON marker
    if stripped.startswith("{"):
        logger.warning(f"Transcript appears to be JSON (invalid): {stripped[:100]}")
        return False
    
    # Check for summary markers
    lower = stripped.lower()
    
    if "action items:" in lower:
        logger.warning(f"Transcript contains 'action items:' marker: {stripped[:100]}")
        return False
    
    if "discussion topics:" in lower:
        logger.warning(f"Transcript contains 'discussion topics:' marker: {stripped[:100]}")
        return False
    
    if lower.startswith("meeting subject:"):
        logger.warning(f"Transcript starts with 'meeting subject:' marker: {stripped[:100]}")
        return False
    
    return True


def get_audio_duration_seconds(audio_file_path):
    """Return audio duration using ffprobe, or None when duration cannot be read."""
    try:
        result = subprocess.run(
            [
                "ffprobe",
                "-v",
                "error",
                "-show_entries",
                "format=duration",
                "-of",
                "default=noprint_wrappers=1:nokey=1",
                str(audio_file_path),
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        return float(result.stdout.strip())
    except Exception as e:
        logger.error(f"Unable to determine audio duration for {audio_file_path}: {e}")
        return None


def split_audio_for_upload(audio_file_path, max_size_mb=None):
    """
    Split oversized audio into upload-safe MP3 chunks.

    Returns a tuple of (chunk_paths, temp_dir). Caller owns temp_dir cleanup.
    """
    audio_file_path = Path(audio_file_path)
    max_size_mb = max_size_mb or get_max_upload_size_mb()
    size_mb = audio_file_path.stat().st_size / (1024 * 1024)

    if size_mb <= max_size_mb:
        return [str(audio_file_path)], None

    duration = get_audio_duration_seconds(audio_file_path)
    if not duration or duration <= 0:
        logger.error("Cannot split audio without a valid duration")
        return [], None

    # Estimate a chunk duration under the upload limit, leaving margin for
    # bitrate variation and multipart overhead.
    safety_factor = 0.80
    segment_seconds = max(30, math.floor(duration * (max_size_mb / size_mb) * safety_factor))
    temp_dir = tempfile.mkdtemp(prefix="transcribe_chunks_")
    output_pattern = Path(temp_dir) / f"{audio_file_path.stem}_part_%03d.mp3"

    cmd = [
        "ffmpeg",
        "-i",
        str(audio_file_path),
        "-f",
        "segment",
        "-segment_time",
        str(segment_seconds),
        "-ar",
        "16000",
        "-ac",
        "1",
        "-codec:a",
        "libmp3lame",
        "-b:a",
        os.getenv("TRANSCRIBE_CHUNK_BITRATE", "64k"),
        str(output_pattern),
    ]

    try:
        subprocess.run(cmd, check=True, capture_output=True, shell=False, text=True)
    except FileNotFoundError:
        shutil.rmtree(temp_dir, ignore_errors=True)
        logger.error("ffmpeg not found while splitting oversized audio")
        return [], None
    except subprocess.CalledProcessError as e:
        shutil.rmtree(temp_dir, ignore_errors=True)
        logger.error(f"ffmpeg audio split failed with exit code {e.returncode}: {e.stderr}")
        return [], None

    chunks = sorted(Path(temp_dir).glob("*_part_*.mp3"))
    oversized = [
        chunk for chunk in chunks
        if chunk.stat().st_size / (1024 * 1024) > max_size_mb
    ]
    if oversized:
        shutil.rmtree(temp_dir, ignore_errors=True)
        logger.error(
            f"Audio split produced {len(oversized)} oversized chunk(s); "
            "lower TRANSCRIBE_CHUNK_BITRATE or TRANSCRIBE_MAX_FILE_SIZE_MB"
        )
        return [], None

    logger.info(
        f"Split oversized audio ({size_mb:.2f} MB) into {len(chunks)} chunk(s) "
        f"using {segment_seconds}s segments"
    )
    return [str(chunk) for chunk in chunks], temp_dir


def _content_type_for_path(audio_file_path):
    """Return a reasonable multipart content type for the audio file."""
    suffix = Path(audio_file_path).suffix.lower()
    if suffix == ".mp3":
        return "audio/mpeg"
    if suffix == ".wav":
        return "audio/wav"
    return "application/octet-stream"


def _models_endpoint(base_url):
    """Return the OpenAI-compatible models endpoint for a service base URL."""
    base_url = base_url.rstrip("/")
    if base_url.endswith("/v1/audio/transcriptions"):
        return base_url[: -len("/audio/transcriptions")] + "/models"
    if base_url.endswith("/v1"):
        return f"{base_url}/models"
    return f"{base_url}/v1/models"


def _extract_model_ids(payload):
    """Extract model IDs from common OpenAI-compatible /v1/models responses."""
    if isinstance(payload, dict):
        models = payload.get("data", [])
    elif isinstance(payload, list):
        models = payload
    else:
        return []

    model_ids = []
    for model in models:
        if isinstance(model, dict):
            model_id = model.get("id") or model.get("name")
        else:
            model_id = str(model)
        if model_id:
            model_ids.append(model_id)
    return model_ids


def _looks_like_model_not_found(status_code, body_text, exception_text=""):
    """Return True when the transcription service reports an unavailable model."""
    text = f"{body_text or ''} {exception_text or ''}".lower()
    if status_code not in {400, 404, 422}:
        return False
    return "model" in text and any(
        marker in text
        for marker in [
            "not found",
            "does not exist",
            "not exist",
            "not served",
            "not available",
            "unknown model",
        ]
    )


def list_available_models(base_url, timeout=30):
    """List available transcription model IDs from the service."""
    endpoint = _models_endpoint(base_url)
    try:
        response = requests.get(endpoint, timeout=timeout)
        response.raise_for_status()
        model_ids = _extract_model_ids(response.json())
    except Exception as e:
        logger.error(f"Unable to list transcription models from {endpoint}: {e}")
        return []

    if not model_ids:
        logger.error(f"No transcription models returned from {endpoint}")
        return []

    logger.info(f"Available transcription models: {model_ids}")
    return model_ids


def _send_transcription_request(audio_file_path, endpoint, model_id, language, timeout):
    """Send one audio file to transcription service and return transcript text."""
    audio_file_path = Path(audio_file_path)

    with open(audio_file_path, "rb") as f:
        files = {
            "file": (audio_file_path.name, f, _content_type_for_path(audio_file_path)),
        }
        data = {
            "model": model_id,
            "language": language,
        }

        logger.info(
            f"Sending transcription request to {endpoint} for {audio_file_path.name} "
            f"using model {model_id}"
        )

        return requests.post(
            endpoint,
            files=files,
            data=data,
            timeout=timeout
        )


def _post_transcription_request(audio_file_path):
    """Send one audio file to transcription service and return transcript text."""
    global _TRANSCRIBE_SELECTED_MODEL_ID
    audio_file_path = Path(audio_file_path)

    base_url = os.getenv("TRANSCRIBE_BASE_URL")
    configured_model_id = os.getenv("TRANSCRIBE_MODEL_ID")
    model_id = _TRANSCRIBE_SELECTED_MODEL_ID or configured_model_id
    language = os.getenv("TRANSCRIBE_LANGUAGE")
    timeout = int(os.getenv("TRANSCRIBE_TIMEOUT_SECONDS", "300"))

    if not base_url:
        raise TranscriptionError("TRANSCRIBE_BASE_URL not set in .env")
    if not configured_model_id:
        raise TranscriptionError("TRANSCRIBE_MODEL_ID not set in .env")
    if not language:
        raise TranscriptionError("TRANSCRIBE_LANGUAGE not set in .env")

    if not audio_file_path.exists():
        logger.error(f"Audio file not found: {audio_file_path}")
        return None

    endpoint = base_url.rstrip("/") + "/v1/audio/transcriptions"

    try:
        try:
            response = _send_transcription_request(
                audio_file_path, endpoint, model_id, language, timeout
            )
            response.raise_for_status()

        except requests.Timeout:
            logger.error(f"Transcription request timed out (>{timeout}s) for {audio_file_path.name}")
            return None

        except requests.RequestException as e:
            response = getattr(e, "response", None)
            status = getattr(response, "status_code", None)
            body_text = getattr(response, "text", "") if response is not None else ""
            if not _looks_like_model_not_found(status, body_text, str(e)):
                logger.error(f"Transcription service error: {e}")
                return None

            logger.warning(
                "Transcription model %s was not found; attempting model discovery",
                model_id,
            )
            for fallback_model in [
                candidate
                for candidate in list_available_models(base_url, timeout=min(timeout, 30))
                if candidate != model_id
            ]:
                logger.info("Retrying transcription with discovered model: %s", fallback_model)
                try:
                    response = _send_transcription_request(
                        audio_file_path, endpoint, fallback_model, language, timeout
                    )
                    response.raise_for_status()
                    _TRANSCRIBE_SELECTED_MODEL_ID = fallback_model
                    break
                except requests.RequestException as fallback_error:
                    logger.error(
                        "Transcription fallback model failed: "
                        f"model={fallback_model}, endpoint={endpoint}, exception={fallback_error}"
                    )
            else:
                logger.error(
                    "Transcription failed because model %s was unavailable and no discovered fallback worked",
                    model_id,
                )
                return None

        try:
            result = response.json()
            transcript = result.get("text", "")

            if not transcript:
                logger.error(f"Empty transcript returned for {audio_file_path.name}")
                return None

            logger.info(f"Transcription successful: {len(transcript)} characters")
            return transcript

        except ValueError as e:
            logger.error(f"Failed to parse transcription response: {e}")
            return None

    except Exception as e:
        logger.exception(f"Unexpected error during transcription: {e}")
        return None


def transcribe_audio(audio_file_path):
    """
    Send audio file to transcription service and return transcript.
    
    Uses HTTP POST to TRANSCRIBE_BASE_URL with multipart/form-data.
    Fields: file (binary), model (TRANSCRIBE_MODEL_ID), language (TRANSCRIBE_LANGUAGE)
    
    Args:
        audio_file_path (str or Path): Path to normalized audio file
        
    Returns:
        str: Transcript text if successful, None otherwise
        
    Raises:
        TranscriptionError: If service configuration is invalid
    """
    audio_file_path = Path(audio_file_path)
    temp_dir = None

    try:
        chunks, temp_dir = split_audio_for_upload(audio_file_path)
        if not chunks:
            return None

        transcripts = []
        for index, chunk_path in enumerate(chunks, start=1):
            if len(chunks) > 1:
                logger.info(f"Transcribing chunk {index}/{len(chunks)}: {Path(chunk_path).name}")

            transcript = _post_transcription_request(chunk_path)
            if transcript is None:
                return None
            transcripts.append(transcript.strip())

        return "\n\n".join(transcripts)

    finally:
        if temp_dir:
            shutil.rmtree(temp_dir, ignore_errors=True)


def transcribe_file(audio_file_path):
    """
    Transcribe an audio file and verify the result.
    
    HIGH-LEVEL FUNCTION: combines transcribe_audio + verify_transcript
    
    Args:
        audio_file_path (str or Path): Path to normalized audio file
        
    Returns:
        str: Valid transcript if successful, None otherwise
        
    Raises:
        TranscriptionError: If service configuration is invalid
    """
    transcript = transcribe_audio(audio_file_path)
    
    if transcript is None:
        return None
    
    if not verify_transcript(transcript):
        logger.warning(f"Transcript verification failed for {audio_file_path}")
        return None
    
    return transcript
