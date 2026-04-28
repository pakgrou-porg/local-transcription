import os
import subprocess
import tempfile
import shutil
import logging
from pathlib import Path


logger = logging.getLogger(__name__)


def normalize_audio(input_path):
    """
    Normalize audio file to 16kHz mono using ffmpeg.
    
    Supports .mp3 and .wav formats.
    Output file placed in same directory with _16k suffix.
    
    Args:
        input_path (str or Path): Path to input audio file
        
    Returns:
        str: Path to normalized audio file
        
    Raises:
        FileNotFoundError: If ffmpeg not found in system PATH
        RuntimeError: If ffmpeg command fails
    """
    input_path = Path(input_path)
    
    if not input_path.exists():
        logger.error(f"Input file does not exist: {input_path}")
        raise FileNotFoundError(f"Input file not found: {input_path}")
    
    suffix = input_path.suffix.lower()
    if suffix not in [".mp3", ".wav"]:
        logger.error(f"Unsupported audio format: {suffix}")
        raise ValueError(f"Unsupported format: {suffix}. Only .mp3 and .wav supported.")
    
    # Generate output path with _16k suffix
    output_path = input_path.parent / f"{input_path.stem}_16k{suffix}"
    
    # Build ffmpeg command
    if suffix == ".wav":
        cmd = [
            "ffmpeg",
            "-i", str(input_path),
            "-ar", "16000",
            "-ac", "1",
            str(output_path)
        ]
    else:  # .mp3
        cmd = [
            "ffmpeg",
            "-i", str(input_path),
            "-ar", "16000",
            "-ac", "1",
            "-codec:a", "libmp3lame",
            "-b:a", "128k",
            str(output_path)
        ]
    
    try:
        result = subprocess.run(
            cmd,
            check=True,
            capture_output=True,
            shell=False,
            text=True
        )
        logger.info(f"Audio normalized: {input_path} -> {output_path}")
        return str(output_path)
    
    except FileNotFoundError:
        logger.error("ffmpeg not found in system PATH")
        raise FileNotFoundError(
            "ffmpeg not found. Install with: apt install ffmpeg (Linux) or "
            "winget install ffmpeg (Windows)"
        )
    
    except subprocess.CalledProcessError as e:
        logger.error(f"ffmpeg failed with exit code {e.returncode}: {e.stderr}")
        raise RuntimeError(f"ffmpeg normalization failed: {e.stderr}")


def preprocess_audio_file(input_path):
    """
    Preprocess audio file: normalize to 16kHz mono.
    
    Creates temporary directory, normalizes audio, and cleans up on failure.
    
    Args:
        input_path (str or Path): Path to input audio file
        
    Returns:
        str: Path to normalized audio file
        
    Raises:
        FileNotFoundError: If ffmpeg not found
        RuntimeError: If preprocessing fails
    """
    input_path = Path(input_path)
    
    try:
        normalized_path = normalize_audio(input_path)
        logger.info(f"Preprocessing complete: {normalized_path}")
        return normalized_path
    
    except Exception as e:
        logger.exception(f"Audio preprocessing failed for {input_path}: {e}")
        raise


# Backwards-compatible alias used by pipeline.py
preprocess_audio = preprocess_audio_file
