from unittest.mock import MagicMock, Mock, patch

from drive import (
    resolve_source_folder_id,
    resolve_archive_folder_id,
    list_audio_files,
    download_file,
    move_file,
    get_file_parents,
    archive_file_if_needed,
    download_file_from_archive,
)


class TestFolderResolution:
    def test_resolve_source_folder_id_returns_first_match(self):
        mock_service = MagicMock()
        mock_service.files().list.return_value.execute.return_value = {
            "files": [{"id": "folder_123", "name": "AudioMeetings"}]
        }

        result = resolve_source_folder_id(mock_service, "AudioMeetings")

        assert result == "folder_123"

    def test_resolve_archive_folder_id_uses_source_parent(self):
        mock_service = MagicMock()
        mock_service.files().list.return_value.execute.return_value = {
            "files": [{"id": "archive_456", "name": "Archive"}]
        }

        result = resolve_archive_folder_id(mock_service, "source_123", "Archive")

        assert result == "archive_456"
        query = mock_service.files().list.call_args.kwargs["q"]
        assert "'source_123' in parents" in query


class TestListAudioFiles:
    def test_list_audio_files_orders_by_creation_time(self):
        mock_service = MagicMock()
        mock_service.files().list.return_value.execute.return_value = {"files": []}

        list_audio_files(mock_service, "folder_123")

        kwargs = mock_service.files().list.call_args.kwargs
        assert kwargs["orderBy"] == "createdTime desc"
        assert ".mp3" in kwargs["q"]
        assert ".wav" in kwargs["q"]


class TestDownloadFile:
    def test_download_file_returns_true_when_chunks_finish(self, tmp_path):
        destination = tmp_path / "audio.mp3"
        mock_service = MagicMock()
        mock_request = Mock()
        mock_service.files().get_media.return_value = mock_request

        with patch("googleapiclient.http.MediaIoBaseDownload") as mock_download:
            downloader = MagicMock()
            downloader.next_chunk.side_effect = [(None, False), (None, True)]
            mock_download.return_value = downloader

            result = download_file(mock_service, "file_123", str(destination))

        assert result is True
        assert destination.exists()

    def test_download_file_returns_false_on_api_error(self, tmp_path):
        mock_service = MagicMock()
        mock_service.files().get_media.side_effect = RuntimeError("boom")

        result = download_file(mock_service, "file_123", str(tmp_path / "audio.mp3"))

        assert result is False


class TestMoveAndArchive:
    def test_move_file_updates_parents(self):
        mock_service = MagicMock()
        mock_service.files().update.return_value.execute.return_value = {
            "id": "file_123",
            "parents": ["archive_456"],
        }

        result = move_file(mock_service, "file_123", "source_123", "archive_456")

        assert result is True
        kwargs = mock_service.files().update.call_args.kwargs
        assert kwargs["addParents"] == "archive_456"
        assert kwargs["removeParents"] == "source_123"

    def test_get_file_parents_reads_metadata(self):
        mock_service = MagicMock()
        mock_service.files().get.return_value.execute.return_value = {
            "id": "file_123",
            "parents": ["source_123"],
        }

        parents = get_file_parents(mock_service, "file_123")

        assert parents == ["source_123"]

    def test_archive_file_if_needed_is_noop_when_already_archived(self):
        mock_service = MagicMock()
        mock_service.files().get.return_value.execute.return_value = {
            "id": "file_123",
            "parents": ["archive_456"],
        }

        result = archive_file_if_needed(
            mock_service, "file_123", "source_123", "archive_456"
        )

        assert result is True
        mock_service.files().update.assert_not_called()

    def test_archive_file_if_needed_moves_from_current_parents(self):
        mock_service = MagicMock()
        mock_service.files().get.return_value.execute.return_value = {
            "id": "file_123",
            "parents": ["source_123"],
        }
        mock_service.files().update.return_value.execute.return_value = {
            "id": "file_123",
            "parents": ["archive_456"],
        }

        result = archive_file_if_needed(
            mock_service, "file_123", "source_123", "archive_456"
        )

        assert result is True
        kwargs = mock_service.files().update.call_args.kwargs
        assert kwargs["addParents"] == "archive_456"
        assert kwargs["removeParents"] == "source_123"


class TestDownloadFileFromArchive:
    def test_download_file_from_archive_returns_temp_path(self):
        mock_service = MagicMock()

        with patch("drive.download_file", return_value=True), patch(
            "drive.tempfile.mkdtemp", return_value="/tmp/archive_test"
        ):
            result = download_file_from_archive(mock_service, "file_123", "test.mp3")

        assert result is not None
        assert "archive" in result.lower()

    def test_download_file_from_archive_cleans_up_on_failure(self):
        mock_service = MagicMock()

        with patch("drive.download_file", return_value=False), patch(
            "drive.tempfile.mkdtemp", return_value="/tmp/archive_test"
        ), patch("shutil.rmtree") as mock_rmtree:
            result = download_file_from_archive(mock_service, "file_123", "test.mp3")

        assert result is None
        mock_rmtree.assert_called_once()
