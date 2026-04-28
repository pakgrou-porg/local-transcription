import json
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

import pipeline


@pytest.fixture(autouse=True)
def pipeline_env(monkeypatch):
    monkeypatch.setenv("SUPABASE_URL", "https://test.supabase.co")
    monkeypatch.setenv("SUPABASE_SERVICE_KEY", "service-key")
    monkeypatch.setenv("SUPABASE_TABLE", "meetings")
    monkeypatch.setenv("DRIVE_SOURCE_FOLDER", "AudioMeetings")
    monkeypatch.setenv("DRIVE_ARCHIVE_FOLDER", "Archive")
    monkeypatch.setenv("GMAIL_DESTINATION_ADDRESS", "test@example.com")
    monkeypatch.setenv("TEST_MODE", "false")
    monkeypatch.setenv("SUBSTITUTIONS_FILE", "substitutions.txt")


class TestHousekeeping:
    def test_cleanup_local_artifacts_removes_archive_temp_directory(self):
        temp_dir = Path(tempfile.mkdtemp(prefix="archive_"))
        temp_file = temp_dir / "meeting_archive.mp3"
        wav_file = temp_dir / "meeting_archive_16k.mp3"
        temp_file.write_text("audio")
        wav_file.write_text("normalized")

        pipeline.cleanup_local_artifacts(str(temp_file), str(wav_file))

        assert not temp_dir.exists()

    def test_cleanup_local_artifacts_ignores_non_pipeline_paths(self, tmp_path):
        safe_dir = tmp_path / "persistent"
        safe_dir.mkdir()
        safe_file = safe_dir / "meeting.wav"
        safe_file.write_text("keep")

        pipeline.cleanup_local_artifacts(str(safe_file))

        assert safe_dir.exists()
        assert safe_file.exists()


class TestSubstitutions:
    @pytest.mark.asyncio
    async def test_apply_configured_substitutions_persists_updated_transcript(self):
        with patch("pipeline.os.path.exists", return_value=True), patch(
            "pipeline.substitute.load_substitutions", return_value={"Karl": ["Carl"]}
        ), patch(
            "pipeline.substitute.apply_substitutions", return_value="Karl attended."
        ), patch(
            "pipeline.supabase_db.update_transcript", new=AsyncMock(return_value=True)
        ) as mock_update:
            result = await pipeline.apply_configured_substitutions(
                "https://test.supabase.co",
                "service-key",
                "meetings",
                7,
                "Carl attended.",
                persist=True,
            )

        assert result == "Karl attended."
        mock_update.assert_awaited_once_with(
            "https://test.supabase.co",
            "service-key",
            "meetings",
            7,
            "Karl attended.",
        )


class TestTranscriptRebuildPolicy:
    def test_needs_transcript_rebuild_when_state_error(self):
        assert (
            pipeline.needs_transcript_rebuild(
                {"state": "error"},
                "Transcript content long enough for validation",
            )
            is True
        )

    def test_needs_transcript_rebuild_when_transcript_missing(self):
        assert pipeline.needs_transcript_rebuild({"state": "new"}, None) is True

    def test_needs_transcript_rebuild_when_transcript_invalid(self):
        assert pipeline.needs_transcript_rebuild(
            {"state": "transcribed"},
            '{"meeting_subject":"synthetic"}',
        ) is True


class TestRecoveryAndCleanup:
    @pytest.mark.asyncio
    async def test_resume_interrupted_job_rebuilds_summary_when_missing(self):
        jobs = [
            {
                "id": 12,
                "file_name": "meeting.mp3",
                "drive_file_id": "file-123",
                "transcript": "Transcript content long enough for validation",
                "summary": None,
                "html": None,
                "state": "transcribed",
            }
        ]
        summary_dict = {
            "meeting_subject": "Recovered",
            "speakers": ["Alice"],
            "action_items": [],
            "discussion_topics": [],
            "resourcing": [],
        }

        with patch(
            "pipeline.supabase_db.get_interrupted_jobs", new=AsyncMock(return_value=jobs)
        ), patch(
            "pipeline.drive.resolve_source_folder_id", return_value="source-folder"
        ), patch(
            "pipeline.os.path.exists", return_value=False
        ), patch(
            "pipeline.summarize.build_from_env"
        ) as mock_build, patch(
            "pipeline.supabase_db.update_summary", new=AsyncMock(return_value=True)
        ) as mock_update_summary, patch(
            "pipeline.render.render_summary_to_html", return_value="<html>summary</html>"
        ), patch(
            "pipeline.supabase_db.update_html", new=AsyncMock(return_value=True)
        ) as mock_update_html, patch(
            "pipeline.archive_drive_file", new=AsyncMock(return_value=True)
        ), patch(
            "pipeline.email_sender.send_summary_email"
        ), patch(
            "pipeline.supabase_db.update_state", new=AsyncMock(return_value=True)
        ):
            mock_build.return_value.summarize.return_value = summary_dict
            success = await pipeline.resume_interrupted_jobs(
                "https://test.supabase.co",
                "service-key",
                "meetings",
                object(),
                object(),
                "AudioMeetings",
                "Archive",
            )

        assert success is True
        mock_build.assert_called_once_with()
        assert json.loads(mock_update_summary.await_args.args[4]) == summary_dict
        mock_update_html.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_normal_pipeline_cleans_temp_directory_on_failure(self, tmp_path):
        temp_dir = Path(tempfile.mkdtemp(prefix="archive_"))
        temp_file = temp_dir / "meeting_archive.mp3"
        temp_file.write_text("audio")

        with patch(
            "pipeline.auth.load_or_refresh_credentials",
            return_value=(object(), object()),
        ), patch(
            "pipeline.supabase_db.get_interrupted_jobs", new=AsyncMock(return_value=[])
        ), patch(
            "pipeline.drive.resolve_source_folder_id", return_value="source-folder"
        ), patch(
            "pipeline.drive.list_audio_files",
            return_value=[{"id": "file-123", "name": "meeting.mp3", "size": 1024}],
        ), patch(
            "pipeline.supabase_db.insert_record", new=AsyncMock(return_value=22)
        ), patch(
            "pipeline.drive.download_file_from_archive", return_value=str(temp_file)
        ), patch(
            "pipeline.preprocess.preprocess_audio", side_effect=RuntimeError("ffmpeg failed")
        ), patch(
            "pipeline.supabase_db.update_state", new=AsyncMock(return_value=True)
        ):
            success = await pipeline.run_normal_pipeline()

        assert success is False
        assert not temp_dir.exists()
