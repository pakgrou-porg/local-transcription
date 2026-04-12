"""Transcription service client — HTTP call and transcript verification."""

import logging

import requests

logger = logging.getLogger(__name__)


def call_transcription_service(
    audio_path: str,
    base_url: str,
    model_id: str,
    language: str = "en",
    timeout: int = 300,
) -> str:
    """Send an audio file to the transcription service and return the transcript.

    Parameters
    ----------
    audio_path : str
        Path to the normalized audio file.
    base_url : str
        Base URL of the transcription service (e.g., http://10.116.2.56:8101).
    model_id : str
        Model identifier to send in the request.
    language : str
        ISO 639-1 language code.
    timeout : int
        HTTP timeout in seconds.

    Returns
    -------
    str
        The transcript text.

    Raises
    ------
    TranscriptionError
        On HTTP errors, timeouts, or empty responses.
    """
    url = f"{base_url.rstrip('/')}/v1/audio/transcriptions"
    logger.info("Calling transcription service: %s (model=%s, lang=%s)", url, model_id, language)

    try:
        with open(audio_path, "rb") as audio_file:
            files = {"file": (audio_path.split("/")[-1], audio_file)}
            data = {"model": model_id}
            if language:
                data["language"] = language

            response = requests.post(
                url,
                files=files,
                data=data,
                timeout=timeout,
            )

        if response.status_code != 200:
            body_preview = response.text[:500] if response.text else "(empty)"
            raise TranscriptionError(
                f"Transcription HTTP {response.status_code}: {body_preview}"
            )

        result = response.json()
        transcript = result.get("text", "")

        if not transcript:
            raise TranscriptionError("Transcription response contained empty 'text' field")

        logger.info("Transcription received: %d characters", len(transcript))
        return transcript

    except requests.exceptions.Timeout:
        raise TranscriptionError(
            f"Transcription request timed out after {timeout}s"
        )
    except requests.exceptions.ConnectionError as e:
        raise TranscriptionError(f"Connection error to transcription service: {e}")
    except requests.exceptions.JSONDecodeError as e:
        raise TranscriptionError(f"Failed to parse transcription response JSON: {e}")


def verify_transcript(transcript: str) -> bool:
    """Verify that a transcript is genuine and not an LLM-generated summary.

    Checks:
    - Length > 50 characters
    - Does not start with '{' (not JSON)
    - Does not contain 'Action Items:' (case-insensitive)
    - Does not contain 'Discussion Topics:' (case-insensitive)
    - Does not start with 'Meeting Subject:'

    Parameters
    ----------
    transcript : str
        The transcript text to verify.

    Returns
    -------
    bool
        True if the transcript passes all verification checks.
    """
    stripped = transcript.strip()

    # Length check
    if len(stripped) <= 50:
        logger.warning(
            "Transcript verification FAILED: too short (%d chars). Preview: %s",
            len(stripped), stripped[:200],
        )
        return False

    # Content checks — all must pass
    checks = [
        (stripped[0] != '{', "starts with '{' (looks like JSON)"),
        ('action items:' not in stripped.lower(), "contains 'Action Items:'"),
        ('discussion topics:' not in stripped.lower(), "contains 'Discussion Topics:'"),
        (not stripped.startswith('Meeting Subject:'), "starts with 'Meeting Subject:'"),
    ]

    for passed, reason in checks:
        if not passed:
            logger.warning(
                "Transcript verification FAILED: %s. Preview: %s",
                reason, stripped[:200],
            )
            return False

    logger.info("Transcript verification passed (%d chars)", len(stripped))
    return True


class TranscriptionError(Exception):
    """Raised when transcription fails for any reason."""
    pass
