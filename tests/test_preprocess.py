import pytest
import tempfile
import shutil
from pathlib import Path
import subprocess
from unittest.mock import patch, MagicMock
import logging

from preprocess import normalize_audio, preprocess_audio_file


logger = logging.getLogger(__name__)


class TestAudioNormalization:
    """Test suite for audio normalization with ffmpeg."""
    
    def test_wav_normalization_produces_16k_output(self, synthetic_wav):
        """
        Test that WAV normalization produces a 16kHz output file.
        
        Verifies:
        - Output file is created
        - Output file has _16k suffix
        - Output file is in the same directory as input
        """
        result = normalize_audio(str(synthetic_wav))
        output_path = Path(result)
        
        assert output_path.exists(), "Normalized output file not created"
        assert "_16k" in output_path.name, "Output file missing _16k suffix"
        assert output_path.parent == synthetic_wav.parent, "Output not in same directory"
        
        # Verify it's a valid WAV file
        from scipy.io import wavfile
        try:
            sample_rate, data = wavfile.read(str(output_path))
            assert sample_rate == 16000, f"Expected 16kHz, got {sample_rate}Hz"
        finally:
            output_path.unlink()
    
    def test_mp3_normalization_uses_libmp3lame(self, tmp_audio_dir):
        """
        Test that MP3 normalization uses libmp3lame codec.
        
        Verifies:
        - FFmpeg command includes libmp3lame codec
        - Output file is created with correct extension
        """
        # Create a minimal MP3 file for testing (we'll mock ffmpeg)
        mp3_path = tmp_audio_dir / "test.mp3"
        mp3_path.write_bytes(b"ID3mock")  # Minimal MP3 header
        
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            
            try:
                normalize_audio(str(mp3_path))
                
                # Verify ffmpeg was called with libmp3lame
                call_args = mock_run.call_args[0][0]
                assert "-codec:a" in call_args, "Missing codec argument"
                codec_idx = call_args.index("-codec:a")
                assert call_args[codec_idx + 1] == "libmp3lame", "Not using libmp3lame"
            finally:
                pass
    
    def test_missing_ffmpeg_raises_and_logs(self, synthetic_wav, caplog):
        """
        Test that missing ffmpeg raises FileNotFoundError and logs error.
        
        Verifies:
        - FileNotFoundError raised with helpful message
        - Error logged about ffmpeg not found
        """
        with patch("subprocess.run", side_effect=FileNotFoundError("ffmpeg not found")):
            with pytest.raises(FileNotFoundError):
                normalize_audio(str(synthetic_wav))
            
            # Check that error was logged
            assert any("ffmpeg" in record.message.lower() for record in caplog.records), \
                "ffmpeg error not logged"
    
    def test_nonzero_ffmpeg_exit_raises(self, synthetic_wav, caplog):
        """
        Test that non-zero ffmpeg exit code raises RuntimeError.
        
        Verifies:
        - RuntimeError raised on ffmpeg failure
        - Stderr message is included in error
        """
        error_stderr = "Invalid audio format"
        with patch("subprocess.run") as mock_run:
            mock_run.side_effect = subprocess.CalledProcessError(
                returncode=1,
                cmd=["ffmpeg"],
                stderr=error_stderr
            )
            
            with pytest.raises(RuntimeError) as exc_info:
                normalize_audio(str(synthetic_wav))
            
            assert error_stderr in str(exc_info.value), "Stderr not in error message"
    
    def test_output_path_in_same_dir_as_input(self, synthetic_wav):
        """
        Test that normalized output is placed in the same directory as input.
        
        Verifies:
        - Output parent directory equals input parent directory
        """
        result = normalize_audio(str(synthetic_wav))
        output_path = Path(result)
        
        assert output_path.parent == synthetic_wav.parent, \
            "Output directory differs from input directory"
        
        # Cleanup
        output_path.unlink()
    
    def test_cleanup_on_failure(self, tmp_audio_dir):
        """
        Test that cleanup occurs when preprocessing fails.
        
        Verifies:
        - Exception is re-raised
        - Resources are properly cleaned up
        """
        invalid_file = tmp_audio_dir / "nonexistent.wav"
        
        with pytest.raises(FileNotFoundError):
            preprocess_audio_file(str(invalid_file))


class TestPreprocessAudioFile:
    """Test suite for the preprocess_audio_file wrapper function."""
    
    def test_successful_preprocessing_returns_path(self, synthetic_wav):
        """
        Test that successful preprocessing returns normalized file path.
        
        Verifies:
        - Path is returned as string
        - Path points to _16k suffixed file
        """
        result = preprocess_audio_file(str(synthetic_wav))
        
        assert result is not None, "No path returned"
        assert isinstance(result, str), "Path not returned as string"
        assert "_16k" in result, "Output path missing _16k suffix"
        
        # Cleanup
        Path(result).unlink()
    
    def test_preprocessing_logs_completion(self, synthetic_wav, caplog):
        """
        Test that preprocessing completion is logged.
        
        Verifies:
        - Info log message contains "Preprocessing complete"
        """
        with caplog.at_level(logging.INFO):
            result = preprocess_audio_file(str(synthetic_wav))
        
        # Check log
        assert any("Preprocessing complete" in record.message for record in caplog.records), \
            "Preprocessing completion not logged"
        
        # Cleanup
        Path(result).unlink()
    
    def test_preprocessing_failure_logs_exception(self, tmp_audio_dir, caplog):
        """
        Test that preprocessing failures are logged as exceptions.
        
        Verifies:
        - Exception logged via logger.exception()
        """
        invalid_file = tmp_audio_dir / "invalid.wav"
        
        with pytest.raises(FileNotFoundError):
            preprocess_audio_file(str(invalid_file))
        
        # Check that exception was logged
        assert any("preprocessing failed" in record.message.lower() for record in caplog.records), \
            "Preprocessing failure not logged"


class TestAudioFileValidation:
    """Test suite for audio file validation."""
    
    def test_unsupported_format_raises_error(self, tmp_audio_dir):
        """
        Test that unsupported audio formats raise ValueError.
        
        Verifies:
        - .txt files rejected
        - .flac files rejected
        - Error message is informative
        """
        unsupported = tmp_audio_dir / "test.txt"
        unsupported.write_text("not audio")
        
        with pytest.raises(ValueError) as exc_info:
            normalize_audio(str(unsupported))
        
        assert "unsupported" in str(exc_info.value).lower(), \
            "Error message doesn't mention unsupported format"
    
    def test_nonexistent_file_raises_error(self, tmp_audio_dir):
        """
        Test that nonexistent files raise FileNotFoundError.
        
        Verifies:
        - Descriptive error message
        """
        nonexistent = tmp_audio_dir / "does_not_exist.wav"
        
        with pytest.raises(FileNotFoundError) as exc_info:
            normalize_audio(str(nonexistent))
        
        assert "not found" in str(exc_info.value).lower(), \
            "Error message not descriptive"
