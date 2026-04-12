"""Audio preprocessing — ffmpeg normalization to 16 kHz mono."""

import logging
import os
import subprocess

logger = logging.getLogger(__name__)


def normalize_audio(input_path: str, output_dir: str) -> str:
    """Normalize an audio file to 16 kHz mono using ffmpeg.

    Parameters
    ----------
    input_path : str
        Path to the raw audio file (.mp3 or .wav).
    output_dir : str
        Directory to write the normalized file into.

    Returns
    -------
    str
        Path to the normalized audio file.

    Raises
    ------
    FileNotFoundError
        If ffmpeg is not found on the system PATH.
    subprocess.CalledProcessError
        If ffmpeg exits with a non-zero status.
    """
    basename = os.path.basename(input_path)
    name, ext = os.path.splitext(basename)
    ext_lower = ext.lower()
    output_filename = f"{name}_16k{ext}"
    output_path = os.path.join(output_dir, output_filename)

    if ext_lower == ".mp3":
        cmd = [
            "ffmpeg", "-i", input_path,
            "-ar", "16000",
            "-ac", "1",
            "-codec:a", "libmp3lame",
            "-b:a", "128k",
            "-y",  # overwrite if exists
            output_path,
        ]
    elif ext_lower in (".wav",):
        cmd = [
            "ffmpeg", "-i", input_path,
            "-ar", "16000",
            "-ac", "1",
            "-y",
            output_path,
        ]
    else:
        # Attempt generic normalization for other audio types
        cmd = [
            "ffmpeg", "-i", input_path,
            "-ar", "16000",
            "-ac", "1",
            "-y",
            output_path,
        ]

    logger.info("Running ffmpeg: %s", " ".join(cmd))

    try:
        result = subprocess.run(
            cmd,
            check=True,
            capture_output=True,
            text=True,
        )
        logger.info("ffmpeg normalization complete: %s", output_path)
        return output_path

    except FileNotFoundError:
        logger.error(
            "ffmpeg not found on PATH. Install with: apt install ffmpeg"
        )
        raise

    except subprocess.CalledProcessError as e:
        logger.error(
            "ffmpeg failed with return code %d.\nstdout: %s\nstderr: %s",
            e.returncode,
            e.stdout[:1000] if e.stdout else "(empty)",
            e.stderr[:1000] if e.stderr else "(empty)",
        )
        raise
