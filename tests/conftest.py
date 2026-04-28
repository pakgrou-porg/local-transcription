import pytest
import tempfile
import os
from pathlib import Path
import numpy as np
from scipy.io import wavfile


@pytest.fixture
def tmp_audio_dir():
    """
    Create a temporary directory for audio files.
    
    Yields:
        Path: Path to temporary directory
        
    Cleanup occurs after test completion.
    """
    temp_dir = tempfile.mkdtemp(prefix="audio_test_")
    yield Path(temp_dir)
    
    # Cleanup
    import shutil
    if Path(temp_dir).exists():
        shutil.rmtree(temp_dir)


@pytest.fixture
def synthetic_wav(tmp_audio_dir):
    """
    Create a synthetic 3-second 16kHz mono WAV file.
    
    Uses scipy.io.wavfile to generate the WAV file.
    
    Yields:
        Path: Path to the generated WAV file
    """
    sample_rate = 16000
    duration = 3  # seconds
    frequency = 440  # A4 note, Hz
    
    # Generate audio samples: sine wave
    t = np.linspace(0, duration, int(sample_rate * duration), endpoint=False)
    samples = (0.3 * 32767 * np.sin(2 * np.pi * frequency * t)).astype(np.int16)
    
    output_path = tmp_audio_dir / "synthetic_test.wav"
    wavfile.write(str(output_path), sample_rate, samples)
    
    yield output_path


@pytest.fixture
def env_overrides(monkeypatch):
    """
    Provide a fixture for overriding environment variables.
    
    Yields:
        dict: Dictionary-like fixture for setting environment variables
    """
    def set_env(key, value):
        monkeypatch.setenv(key, value)
    
    yield set_env
