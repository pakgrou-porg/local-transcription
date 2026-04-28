import pytest
from unittest.mock import patch, MagicMock, mock_open
from pathlib import Path
import requests

from transcribe import (
    verify_transcript,
    transcribe_audio,
    transcribe_file,
    split_audio_for_upload,
    TranscriptionError
)


class TestTranscriptVerification:
    """Test suite for transcript verification logic."""
    
    def test_transcript_verification_passes_valid_transcript(self):
        """
        Test that valid transcript passes all verification checks.
        
        Verifies:
        - Length > 50 characters
        - Does not start with "{"
        - Does not contain summary markers
        """
        valid_transcript = (
            "This is a valid transcript from a real meeting. We discussed many topics "
            "including product strategy, customer feedback, and quarterly planning. "
            "The discussion was productive and everyone participated in the conversation."
        )
        
        assert verify_transcript(valid_transcript) is True
    
    def test_transcript_verification_rejects_short_text(self):
        """
        Test that transcripts shorter than 50 characters are rejected.
        
        Verifies:
        - Length check is enforced
        """
        short_transcript = "This is too short"
        
        assert verify_transcript(short_transcript) is False
    
    def test_transcript_verification_rejects_json(self):
        """
        Test that JSON-like text is rejected (starts with "{").
        
        Verifies:
        - JSON marker detection prevents malformed data
        """
        json_text = '{"meeting_subject": "test", "speakers": ["John"], "action_items": [{"assigned_to": "John", "action": "Follow up"}]}'
        
        assert verify_transcript(json_text) is False
    
    def test_transcript_verification_rejects_action_items_marker(self):
        """
        Test that text with "action items:" marker is rejected.
        
        Verifies:
        - Auto-generated summary marker detection
        """
        bad_transcript = (
            "This is a long enough transcript to pass length check. "
            "However, it contains the action items: marker which indicates "
            "it's an auto-generated summary rather than a real transcript. "
            "It should be rejected."
        )
        
        assert verify_transcript(bad_transcript) is False
    
    def test_transcript_verification_rejects_discussion_topics_marker(self):
        """
        Test that text with "discussion topics:" marker is rejected.
        
        Verifies:
        - Auto-generated summary marker detection
        """
        bad_transcript = (
            "This is a sufficiently long transcript that would normally pass. "
            "But it contains discussion topics: marker which indicates "
            "it's not a real meeting transcript but a summary document. "
            "It should fail verification."
        )
        
        assert verify_transcript(bad_transcript) is False
    
    def test_transcript_verification_rejects_meeting_subject_prefix(self):
        """
        Test that text starting with "meeting subject:" is rejected.
        
        Verifies:
        - Summary format prefix detection
        """
        bad_transcript = "Meeting subject: Q2 Strategy Discussion and this is extra text to make it long enough to pass the length check"
        
        assert verify_transcript(bad_transcript) is False
    
    def test_transcript_verification_case_insensitive(self):
        """
        Test that verification checks are case-insensitive.
        
        Verifies:
        - All marker checks work with mixed case
        """
        bad_upper = "This is a very long transcript with content. ACTION ITEMS: should be rejected even with uppercase markers"
        bad_mixed = "Long enough content for the check. Discussion Topics: are also rejected with mixed case handling"
        
        assert verify_transcript(bad_upper) is False
        assert verify_transcript(bad_mixed) is False
    
    def test_transcript_verification_with_whitespace(self):
        """
        Test that verification handles leading/trailing whitespace.
        
        Verifies:
        - Whitespace stripping is consistent
        """
        valid_with_spaces = "  \n  This is a valid transcript with enough content to pass the length check. It has proper content and no markers.  \n  "
        
        assert verify_transcript(valid_with_spaces) is True
    
    def test_transcript_verification_rejects_non_string(self):
        """
        Test that non-string inputs are rejected.
        
        Verifies:
        - Type checking prevents crashes
        """
        assert verify_transcript(None) is False
        assert verify_transcript(123) is False
        assert verify_transcript([]) is False


class TestTranscriptionService:
    """Test suite for transcription service integration."""
    
    def test_successful_transcription_returns_text(self, monkeypatch, synthetic_wav):
        """
        Test successful transcription service call.
        
        Mocks HTTP response with valid transcript JSON.
        
        Verifies:
        - Response parsed correctly
        - Transcript text extracted
        """
        monkeypatch.setenv("TRANSCRIBE_BASE_URL", "http://localhost:8101")
        monkeypatch.setenv("TRANSCRIBE_MODEL_ID", "test-model")
        monkeypatch.setenv("TRANSCRIBE_LANGUAGE", "en")
        
        mock_response = MagicMock()
        mock_response.json.return_value = {"text": "This is a valid test transcript with enough content to pass verification checks"}
        
        with patch("requests.post", return_value=mock_response):
            result = transcribe_audio(str(synthetic_wav))
            
            assert result is not None
            assert isinstance(result, str)
            assert "valid test transcript" in result
    
    def test_http_error_returns_none_and_logs(self, monkeypatch, synthetic_wav, caplog):
        """
        Test that HTTP errors return None.
        
        Verifies:
        - RequestException caught and logged
        - None returned on failure
        """
        monkeypatch.setenv("TRANSCRIBE_BASE_URL", "http://localhost:8101")
        monkeypatch.setenv("TRANSCRIBE_MODEL_ID", "test-model")
        monkeypatch.setenv("TRANSCRIBE_LANGUAGE", "en")
        
        with patch("requests.post", side_effect=requests.RequestException("Connection failed")):
            result = transcribe_audio(str(synthetic_wav))
            
            assert result is None
            assert any("error" in record.message.lower() for record in caplog.records)
    
    def test_timeout_returns_none_and_logs(self, monkeypatch, synthetic_wav, caplog):
        """
        Test that timeout errors return None.
        
        Verifies:
        - Timeout exception caught
        - None returned
        - Timeout logged
        """
        monkeypatch.setenv("TRANSCRIBE_BASE_URL", "http://localhost:8101")
        monkeypatch.setenv("TRANSCRIBE_MODEL_ID", "test-model")
        monkeypatch.setenv("TRANSCRIBE_LANGUAGE", "en")
        monkeypatch.setenv("TRANSCRIBE_TIMEOUT_SECONDS", "5")
        
        with patch("requests.post", side_effect=requests.Timeout("Request timed out")):
            result = transcribe_audio(str(synthetic_wav))
            
            assert result is None
            assert any("timed out" in record.message.lower() for record in caplog.records)
    
    def test_missing_transcribe_base_url_raises_error(self, monkeypatch, synthetic_wav):
        """
        Test that missing TRANSCRIBE_BASE_URL raises TranscriptionError.
        
        Verifies:
        - Configuration validation
        """
        monkeypatch.delenv("TRANSCRIBE_BASE_URL", raising=False)
        monkeypatch.setenv("TRANSCRIBE_MODEL_ID", "test-model")
        monkeypatch.setenv("TRANSCRIBE_LANGUAGE", "en")
        
        with pytest.raises(TranscriptionError):
            transcribe_audio(str(synthetic_wav))
    
    def test_invalid_response_json_returns_none(self, monkeypatch, synthetic_wav, caplog):
        """
        Test that invalid JSON response returns None.
        
        Verifies:
        - JSON parse error handled
        """
        monkeypatch.setenv("TRANSCRIBE_BASE_URL", "http://localhost:8101")
        monkeypatch.setenv("TRANSCRIBE_MODEL_ID", "test-model")
        monkeypatch.setenv("TRANSCRIBE_LANGUAGE", "en")
        
        mock_response = MagicMock()
        mock_response.json.side_effect = ValueError("Invalid JSON")
        
        with patch("requests.post", return_value=mock_response):
            result = transcribe_audio(str(synthetic_wav))
            
            assert result is None
    
    def test_empty_transcript_returns_none(self, monkeypatch, synthetic_wav, caplog):
        """
        Test that empty transcript text returns None.
        
        Verifies:
        - Empty response detection
        """
        monkeypatch.setenv("TRANSCRIBE_BASE_URL", "http://localhost:8101")
        monkeypatch.setenv("TRANSCRIBE_MODEL_ID", "test-model")
        monkeypatch.setenv("TRANSCRIBE_LANGUAGE", "en")
        
        mock_response = MagicMock()
        mock_response.json.return_value = {"text": ""}
        
        with patch("requests.post", return_value=mock_response):
            result = transcribe_audio(str(synthetic_wav))
            
            assert result is None

    def test_oversized_audio_is_split_before_transcription(self, monkeypatch, tmp_path):
        """
        Test that oversized audio is split before upload.

        Verifies:
        - ffprobe is used to read duration
        - ffmpeg is used to create chunks
        - returned chunk list contains generated files
        """
        monkeypatch.setenv("TRANSCRIBE_MAX_FILE_SIZE_MB", "1")
        source = tmp_path / "large.mp3"
        source.write_bytes(b"0" * 2 * 1024 * 1024)

        def fake_run(cmd, **kwargs):
            if cmd[0] == "ffprobe":
                response = MagicMock()
                response.stdout = "120.0\n"
                return response

            output_pattern = Path(cmd[-1])
            (output_pattern.parent / "large_part_000.mp3").write_bytes(b"chunk-1")
            (output_pattern.parent / "large_part_001.mp3").write_bytes(b"chunk-2")
            return MagicMock(returncode=0)

        with patch("transcribe.subprocess.run", side_effect=fake_run):
            chunks, temp_dir = split_audio_for_upload(source, max_size_mb=1)

        try:
            assert len(chunks) == 2
            assert all(Path(chunk).exists() for chunk in chunks)
        finally:
            import shutil
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_transcribe_audio_joins_chunk_transcripts(self, monkeypatch, synthetic_wav):
        """
        Test that chunked transcription returns one joined transcript.
        """
        monkeypatch.setenv("TRANSCRIBE_BASE_URL", "http://localhost:8101")
        monkeypatch.setenv("TRANSCRIBE_MODEL_ID", "test-model")
        monkeypatch.setenv("TRANSCRIBE_LANGUAGE", "en")

        with patch(
            "transcribe.split_audio_for_upload",
            return_value=(["/tmp/chunk1.mp3", "/tmp/chunk2.mp3"], "/tmp/chunks"),
        ), patch(
            "transcribe._post_transcription_request",
            side_effect=["First transcript part", "Second transcript part"],
        ), patch("transcribe.shutil.rmtree") as mock_cleanup:
            result = transcribe_audio(str(synthetic_wav))

        assert result == "First transcript part\n\nSecond transcript part"
        mock_cleanup.assert_called_once_with("/tmp/chunks", ignore_errors=True)


class TestTranscribeFile:
    """Test suite for high-level transcribe_file function."""
    
    def test_successful_transcription_and_verification(self, monkeypatch, synthetic_wav):
        """
        Test end-to-end transcription with verification.
        
        Verifies:
        - Transcription called
        - Verification applied
        - Valid transcript returned
        """
        monkeypatch.setenv("TRANSCRIBE_BASE_URL", "http://localhost:8101")
        monkeypatch.setenv("TRANSCRIBE_MODEL_ID", "test-model")
        monkeypatch.setenv("TRANSCRIBE_LANGUAGE", "en")
        
        valid_transcript = "This is a sufficiently long and valid transcript from our meeting discussion without any problematic markers or issues"
        
        mock_response = MagicMock()
        mock_response.json.return_value = {"text": valid_transcript}
        
        with patch("requests.post", return_value=mock_response):
            result = transcribe_file(str(synthetic_wav))
            
            assert result == valid_transcript
    
    def test_failed_transcription_returns_none(self, monkeypatch, synthetic_wav):
        """
        Test that transcription failure returns None.
        
        Verifies:
        - Service failure handled
        """
        monkeypatch.setenv("TRANSCRIBE_BASE_URL", "http://localhost:8101")
        monkeypatch.setenv("TRANSCRIBE_MODEL_ID", "test-model")
        monkeypatch.setenv("TRANSCRIBE_LANGUAGE", "en")
        
        with patch("requests.post", side_effect=requests.RequestException("Service unavailable")):
            result = transcribe_file(str(synthetic_wav))
            
            assert result is None
    
    def test_invalid_transcript_returns_none(self, monkeypatch, synthetic_wav):
        """
        Test that invalid transcript is rejected.
        
        Verifies:
        - Verification applied to result
        """
        monkeypatch.setenv("TRANSCRIBE_BASE_URL", "http://localhost:8101")
        monkeypatch.setenv("TRANSCRIBE_MODEL_ID", "test-model")
        monkeypatch.setenv("TRANSCRIBE_LANGUAGE", "en")
        
        # Short invalid transcript
        bad_transcript = "Short"
        
        mock_response = MagicMock()
        mock_response.json.return_value = {"text": bad_transcript}
        
        with patch("requests.post", return_value=mock_response):
            result = transcribe_file(str(synthetic_wav))
            
            assert result is None
