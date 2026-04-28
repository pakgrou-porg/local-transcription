import json
from unittest.mock import AsyncMock, MagicMock, patch

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


@pytest.fixture
def google_services():
    return MagicMock(name="drive_service"), MagicMock(name="gmail_service")


@pytest.fixture
def summary_dict():
    return {
        "meeting_subject": "Q2 Planning",
        "speakers": ["Alice", "Bob"],
        "action_items": [{"assigned_to": "Alice", "action": "Follow up"}],
        "discussion_topics": ["Budget"],
        "resourcing": ["Team A"],
    }


def configure_pipeline_success_mocks(
    google_services,
    summary_dict,
    tmp_path,
    events,
):
    drive_service, gmail_service = google_services
    temp_dir = tmp_path / "archive_case"
    temp_dir.mkdir()
    temp_file = temp_dir / "meeting_archive.mp3"
    wav_file = temp_dir / "meeting_archive_16k.mp3"
    temp_file.write_text("audio")
    wav_file.write_text("normalized")

    patches = [
        patch("pipeline.auth.load_or_refresh_credentials", return_value=google_services),
        patch("pipeline.supabase_db.get_interrupted_jobs", new=AsyncMock(return_value=[])),
        patch("pipeline.drive.resolve_source_folder_id", return_value="source-folder"),
        patch(
            "pipeline.drive.list_audio_files",
            return_value=[{"id": "file-123", "name": "meeting.mp3", "size": 1024}],
        ),
        patch("pipeline.supabase_db.insert_record", new=AsyncMock(return_value=77)),
        patch("pipeline.drive.download_file_from_archive", return_value=str(temp_file)),
        patch("pipeline.preprocess.preprocess_audio", return_value=str(wav_file)),
        patch("pipeline.transcribe.transcribe_file", return_value="Transcript content long enough for validation"),
        patch("pipeline.supabase_db.update_transcript", new=AsyncMock(return_value=True)),
        patch("pipeline.os.path.exists", return_value=False),
        patch(
            "pipeline.supabase_db.update_state",
            new=AsyncMock(side_effect=lambda *args: events.append(f"state:{args[4]}") or True),
        ),
        patch("pipeline.summarize.build_from_env"),
        patch("pipeline.supabase_db.update_summary", new=AsyncMock(return_value=True)),
        patch("pipeline.render.render_summary_to_html", return_value="<html>summary</html>"),
        patch("pipeline.supabase_db.update_html", new=AsyncMock(return_value=True)),
        patch(
            "pipeline.archive_drive_file",
            new=AsyncMock(side_effect=lambda *args: events.append("archive") or True),
        ),
        patch(
            "pipeline.email_sender.send_summary_email",
            side_effect=lambda *args: events.append("email"),
        ),
        patch("pipeline.cleanup_local_artifacts"),
    ]

    stack = [p.start() for p in patches]
    stack[11].return_value.summarize.return_value = summary_dict
    return patches, stack


class TestNormalPipeline:
    @pytest.mark.asyncio
    async def test_normal_pipeline_archives_before_email_and_completion(
        self, google_services, summary_dict, tmp_path
    ):
        events = []
        patches, stack = configure_pipeline_success_mocks(
            google_services, summary_dict, tmp_path, events
        )
        try:
            success = await pipeline.run_normal_pipeline()
        finally:
            for p in reversed(patches):
                p.stop()

        assert success is True
        assert events.index("archive") < events.index("email")
        assert events.index("email") < events.index("state:html")
        stack[11].assert_called_once_with()
        stack[17].assert_called_once()

    @pytest.mark.asyncio
    async def test_normal_pipeline_uses_module_level_summarizer_factory(
        self, google_services, summary_dict, tmp_path
    ):
        events = []
        patches, stack = configure_pipeline_success_mocks(
            google_services, summary_dict, tmp_path, events
        )
        try:
            success = await pipeline.run_normal_pipeline()
        finally:
            for p in reversed(patches):
                p.stop()

        assert success is True
        stack[11].assert_called_once_with()


class TestRecoveryFlow:
    @pytest.mark.asyncio
    async def test_recovery_uses_saved_summary_and_html_when_present(self, google_services):
        drive_service, gmail_service = google_services
        saved_summary = {
            "meeting_subject": "Recovered Meeting",
            "speakers": ["Alice"],
            "action_items": [],
            "discussion_topics": [],
            "resourcing": [],
        }
        jobs = [
            {
                "id": 5,
                "file_name": "meeting.mp3",
                "drive_file_id": "file-123",
                "transcript": "Transcript already stored",
                "summary": json.dumps(saved_summary),
                "html": "<html>cached</html>",
                "state": "transcribed",
            }
        ]

        with patch(
            "pipeline.auth.load_or_refresh_credentials", return_value=google_services
        ), patch(
            "pipeline.supabase_db.get_interrupted_jobs", new=AsyncMock(return_value=jobs)
        ), patch(
            "pipeline.drive.resolve_source_folder_id", return_value="source-folder"
        ), patch(
            "pipeline.archive_drive_file", new=AsyncMock(return_value=True)
        ) as mock_archive, patch(
            "pipeline.email_sender.send_summary_email"
        ) as mock_email, patch(
            "pipeline.supabase_db.update_state", new=AsyncMock(return_value=True)
        ) as mock_update_state, patch(
            "pipeline.summarize.build_from_env"
        ) as mock_build, patch(
            "pipeline.supabase_db.update_summary", new=AsyncMock(return_value=True)
        ) as mock_update_summary, patch(
            "pipeline.supabase_db.update_html", new=AsyncMock(return_value=True)
        ) as mock_update_html:
            success = await pipeline.run_normal_pipeline()

        assert success is True
        mock_build.assert_not_called()
        mock_update_summary.assert_not_called()
        mock_update_html.assert_not_called()
        mock_archive.assert_called_once()
        mock_email.assert_called_once()
        mock_update_state.assert_any_call(
            "https://test.supabase.co", "service-key", "meetings", 5, "html"
        )

    @pytest.mark.asyncio
    async def test_recovery_archive_failure_blocks_completion_and_email(self, google_services):
        jobs = [
            {
                "id": 5,
                "file_name": "meeting.mp3",
                "drive_file_id": "file-123",
                "transcript": "Transcript already stored",
                "summary": json.dumps(
                    {
                        "meeting_subject": "Recovered Meeting",
                        "speakers": [],
                        "action_items": [],
                        "discussion_topics": [],
                        "resourcing": [],
                    }
                ),
                "html": "<html>cached</html>",
                "state": "transcribed",
            }
        ]

        with patch(
            "pipeline.auth.load_or_refresh_credentials", return_value=google_services
        ), patch(
            "pipeline.supabase_db.get_interrupted_jobs", new=AsyncMock(return_value=jobs)
        ), patch(
            "pipeline.drive.resolve_source_folder_id", return_value="source-folder"
        ), patch(
            "pipeline.archive_drive_file", new=AsyncMock(return_value=False)
        ), patch(
            "pipeline.email_sender.send_summary_email"
        ) as mock_email, patch(
            "pipeline.supabase_db.update_state", new=AsyncMock(return_value=True)
        ) as mock_update_state:
            success = await pipeline.run_normal_pipeline()

        assert success is False
        mock_email.assert_not_called()
        completed_calls = [call.args[4] for call in mock_update_state.await_args_list]
        assert "html" not in completed_calls

    @pytest.mark.asyncio
    async def test_recovery_falls_back_to_alternate_completed_state(self, google_services):
        jobs = [
            {
                "id": 5,
                "file_name": "meeting.mp3",
                "drive_file_id": "file-123",
                "transcript": "Transcript already stored",
                "summary": json.dumps(
                    {
                        "meeting_subject": "Recovered Meeting",
                        "speakers": [],
                        "action_items": [],
                        "discussion_topics": [],
                        "resourcing": [],
                    }
                ),
                "html": "<html>cached</html>",
                "state": "transcribed",
            }
        ]

        async def update_state_side_effect(_url, _key, _table, _id, state):
            return state == "complete"

        with patch(
            "pipeline.auth.load_or_refresh_credentials", return_value=google_services
        ), patch(
            "pipeline.supabase_db.get_interrupted_jobs", new=AsyncMock(return_value=jobs)
        ), patch(
            "pipeline.drive.resolve_source_folder_id", return_value="source-folder"
        ), patch(
            "pipeline.archive_drive_file", new=AsyncMock(return_value=True)
        ), patch(
            "pipeline.email_sender.send_summary_email"
        ) as mock_email, patch(
            "pipeline.supabase_db.update_state",
            new=AsyncMock(side_effect=update_state_side_effect),
        ) as mock_update_state:
            success = await pipeline.run_normal_pipeline()

        assert success is True
        mock_email.assert_called_once()
        attempted_states = [call.args[4] for call in mock_update_state.await_args_list]
        assert attempted_states[-3:] == ["html", "completed", "complete"]


class TestBatchPipeline:
    @pytest.mark.asyncio
    async def test_batch_pipeline_reprocesses_record(self, google_services, summary_dict):
        events = []
        records = [
            {
                "id": 9,
                "file_name": "meeting.mp3",
                "drive_file_id": "file-9",
                "transcript": "Transcript content long enough for validation and exceeds minimum transcript length requirements.",
            }
        ]

        with patch(
            "pipeline.auth.load_or_refresh_credentials", return_value=google_services
        ), patch(
            "pipeline.supabase_db.query_batch_by_ids", new=AsyncMock(return_value=records)
        ), patch(
            "pipeline.drive.resolve_source_folder_id", return_value="source-folder"
        ), patch(
            "pipeline.archive_drive_file",
            new=AsyncMock(side_effect=lambda *args: events.append("archive") or True),
        ), patch(
            "pipeline.summarize.build_from_env"
        ) as mock_build, patch(
            "pipeline.render.render_summary_to_html", return_value="<html>summary</html>"
        ), patch(
            "pipeline.os.path.exists", return_value=False
        ), patch(
            "pipeline.email_sender.send_summary_email"
        ) as mock_email, patch(
            "pipeline.supabase_db.update_summary", new=AsyncMock(return_value=True)
        ), patch(
            "pipeline.supabase_db.update_html", new=AsyncMock(return_value=True)
        ), patch(
            "pipeline.supabase_db.update_state", new=AsyncMock(return_value=True)
        ) as mock_update_state:
            mock_build.return_value.summarize.return_value = summary_dict
            processed = await pipeline.run_batch_pipeline("ids", "9")

        assert processed == 1
        mock_build.assert_called_once_with()
        mock_email.assert_called_once()
        assert "archive" in events
        mock_update_state.assert_any_call(
            "https://test.supabase.co", "service-key", "meetings", 9, "html"
        )

    @pytest.mark.asyncio
    async def test_batch_pipeline_invalid_ids_return_zero(self, google_services):
        with patch(
            "pipeline.auth.load_or_refresh_credentials", return_value=google_services
        ):
            processed = await pipeline.run_batch_pipeline("ids", "abc,def")

        assert processed == 0

    @pytest.mark.asyncio
    async def test_batch_pipeline_recent_reprocesses_records_with_or_without_transcript(
        self, google_services, summary_dict
    ):
        events = []
        recent_records = [
            {
                "id": 21,
                "file_name": "has-transcript.mp3",
                "drive_file_id": "file-21",
                "transcript": "Transcript content long enough for validation and exceeds minimum transcript length requirements.",
            },
            {
                "id": 22,
                "file_name": "no-transcript.mp3",
                "transcript": None,
                "state": "new",
                "drive_file_id": "file-22",
            },
        ]

        with patch(
            "pipeline.auth.load_or_refresh_credentials", return_value=google_services
        ), patch(
            "pipeline.supabase_db.query_batch_recent", new=AsyncMock(return_value=recent_records)
        ), patch(
            "pipeline.drive.resolve_source_folder_id", return_value="source-folder"
        ), patch(
            "pipeline.archive_drive_file",
            new=AsyncMock(side_effect=lambda *args: events.append("archive") or True),
        ), patch(
            "pipeline.drive.download_file_from_archive", return_value="/tmp/archive_case/no-transcript.mp3"
        ), patch(
            "pipeline.preprocess.preprocess_audio", return_value="/tmp/archive_case/no-transcript_16k.mp3"
        ), patch(
            "pipeline.transcribe.transcribe_file", return_value="Recovered transcript text long enough to pass validation"
        ), patch(
            "pipeline.supabase_db.update_transcript", new=AsyncMock(return_value=True)
        ), patch(
            "pipeline.cleanup_local_artifacts"
        ), patch(
            "pipeline.summarize.build_from_env"
        ) as mock_build, patch(
            "pipeline.render.render_summary_to_html", return_value="<html>summary</html>"
        ), patch(
            "pipeline.os.path.exists", return_value=False
        ), patch(
            "pipeline.email_sender.send_summary_email"
        ) as mock_email, patch(
            "pipeline.supabase_db.update_summary", new=AsyncMock(return_value=True)
        ), patch(
            "pipeline.supabase_db.update_html", new=AsyncMock(return_value=True)
        ), patch(
            "pipeline.supabase_db.update_state", new=AsyncMock(return_value=True)
        ):
            mock_build.return_value.summarize.return_value = summary_dict
            processed = await pipeline.run_batch_pipeline("recent", "20")

        assert processed == 2
        assert mock_email.call_count == 2
        assert events == ["archive", "archive"]
