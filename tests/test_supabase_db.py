from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import supabase_db


def make_query(result=None, error=None):
    query = MagicMock()
    if error is not None:
        query.execute = AsyncMock(side_effect=error)
    else:
        query.execute = AsyncMock(return_value=SimpleNamespace(data=result))
    query.insert.return_value = query
    query.update.return_value = query
    query.select.return_value = query
    query.eq.return_value = query
    query.is_.return_value = query
    query.order.return_value = query
    query.limit.return_value = query
    query.gte.return_value = query
    query.lt.return_value = query
    return query


class TestInsertRecord:
    @pytest.mark.asyncio
    async def test_insert_record_returns_inserted_id(self):
        query = make_query([{"id": 42}])
        client = MagicMock()
        client.table.return_value = query

        with patch("supabase_db.get_supabase_client", new=AsyncMock(return_value=client)):
            record_id = await supabase_db.insert_record(
                "https://test.supabase.co",
                "service-key",
                "meetings",
                "meeting.mp3",
                1024,
                "file-123",
            )

        assert record_id == 42
        inserted_data = query.insert.call_args.args[0]
        assert inserted_data["state"] == "new"
        assert inserted_data["drive_file_id"] == "file-123"

    @pytest.mark.asyncio
    async def test_insert_record_retries_transient_error(self):
        transient = Exception("temporary")
        transient.http_code = 503
        query = make_query([{"id": 42}])
        query.execute = AsyncMock(side_effect=[transient, SimpleNamespace(data=[{"id": 42}])])
        query.insert.return_value = query
        client = MagicMock()
        client.table.return_value = query

        with patch("supabase_db.get_supabase_client", new=AsyncMock(return_value=client)), patch(
            "supabase_db.sleep_with_jitter", new=AsyncMock(return_value=None)
        ) as mock_sleep:
            record_id = await supabase_db.insert_record(
                "https://test.supabase.co",
                "service-key",
                "meetings",
                "meeting.mp3",
            )

        assert record_id == 42
        mock_sleep.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_insert_record_fails_fast_on_non_transient_api_code(self):
        err = Exception({"code": "PGRST205", "message": "table missing"})
        query = make_query(error=err)
        query.insert.return_value = query
        client = MagicMock()
        client.table.return_value = query

        with patch("supabase_db.get_supabase_client", new=AsyncMock(return_value=client)), patch(
            "supabase_db.sleep_with_jitter", new=AsyncMock(return_value=None)
        ) as mock_sleep:
            record_id = await supabase_db.insert_record(
                "https://test.supabase.co",
                "service-key",
                "meetings",
                "meeting.mp3",
            )

        assert record_id is None
        mock_sleep.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_insert_record_fails_fast_on_constraint_violation_code(self):
        err = Exception({"code": "23514", "message": "check constraint violated"})
        query = make_query(error=err)
        query.insert.return_value = query
        client = MagicMock()
        client.table.return_value = query

        with patch("supabase_db.get_supabase_client", new=AsyncMock(return_value=client)), patch(
            "supabase_db.sleep_with_jitter", new=AsyncMock(return_value=None)
        ) as mock_sleep:
            record_id = await supabase_db.insert_record(
                "https://test.supabase.co",
                "service-key",
                "meetings",
                "meeting.mp3",
            )

        assert record_id is None
        mock_sleep.assert_not_awaited()


class TestUpdateHelpers:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("method_name", "field_name", "field_value"),
        [
            ("update_state", "state", "transcribed"),
            ("update_transcript", "transcript", "hello world"),
            ("update_summary", "summary", "{}"),
            ("update_html", "html", "<html></html>"),
        ],
    )
    async def test_update_methods_send_expected_payload(
        self, method_name, field_name, field_value
    ):
        query = make_query([])
        client = MagicMock()
        client.table.return_value = query

        method = getattr(supabase_db, method_name)

        with patch("supabase_db.get_supabase_client", new=AsyncMock(return_value=client)):
            success = await method(
                "https://test.supabase.co",
                "service-key",
                "meetings",
                5,
                field_value,
            )

        assert success is True
        payload = query.update.call_args.args[0]
        assert payload[field_name] == field_value
        assert "updated_at" in payload

    @pytest.mark.asyncio
    async def test_update_state_returns_false_on_non_transient_error(self):
        err = Exception("bad request")
        err.http_code = 400
        query = make_query(error=err)
        client = MagicMock()
        client.table.return_value = query

        with patch("supabase_db.get_supabase_client", new=AsyncMock(return_value=client)):
            success = await supabase_db.update_state(
                "https://test.supabase.co",
                "service-key",
                "meetings",
                5,
                "invalid_state",
            )

        assert success is False


class TestQueries:
    @pytest.mark.asyncio
    async def test_get_interrupted_jobs_returns_transcribed_and_resumable_error_records(self):
        transcribed = [
            {"id": 1, "state": "transcribed", "summary": None},
            {"id": 2, "state": "transcribed", "summary": "{}"},
        ]
        error_rows = [
            {"id": 3, "state": "error", "transcript": "ok", "summary": None, "html": None},
            {"id": 4, "state": "error", "transcript": None, "summary": None, "html": None, "drive_file_id": "drive-4"},
            {"id": 5, "state": "error", "transcript": "ok", "summary": "{}", "html": "<html/>"},
            {"id": 6, "state": "error", "transcript": None, "summary": None, "html": None},
        ]
        query = make_query([])
        query.execute = AsyncMock(
            side_effect=[
                SimpleNamespace(data=transcribed),
                SimpleNamespace(data=error_rows),
            ]
        )
        client = MagicMock()
        client.table.return_value = query

        with patch("supabase_db.get_supabase_client", new=AsyncMock(return_value=client)):
            jobs = await supabase_db.get_interrupted_jobs(
                "https://test.supabase.co", "service-key", "meetings"
            )

        assert [record["id"] for record in jobs] == [1, 2, 3, 4, 5]
        assert query.eq.call_args_list[0].args == ("state", "transcribed")
        assert query.eq.call_args_list[1].args == ("state", "error")

    @pytest.mark.asyncio
    async def test_query_batch_by_ids_fetches_each_requested_id(self):
        client = MagicMock()
        query_one = make_query([{"id": 1}])
        query_two = make_query([{"id": 3}])
        client.table.return_value = MagicMock()
        client.table.return_value.select.return_value = client.table.return_value
        client.table.return_value.eq.side_effect = [query_one, query_two]

        with patch("supabase_db.get_supabase_client", new=AsyncMock(return_value=client)):
            results = await supabase_db.query_batch_by_ids(
                "https://test.supabase.co", "service-key", "meetings", [1, 3]
            )

        assert [record["id"] for record in results] == [1, 3]

    @pytest.mark.asyncio
    async def test_query_batch_by_month_uses_month_range(self):
        query = make_query([{"id": 7}])
        client = MagicMock()
        client.table.return_value = query

        with patch("supabase_db.get_supabase_client", new=AsyncMock(return_value=client)):
            results = await supabase_db.query_batch_by_month(
                "https://test.supabase.co", "service-key", "meetings", "2026-04"
            )

        assert results == [{"id": 7}]
        assert query.gte.call_args.args[0] == "created_at"
        assert query.lt.call_args.args[0] == "created_at"

    @pytest.mark.asyncio
    async def test_query_batch_by_status_filters_by_state(self):
        expected = [{"id": 9, "state": "error"}]
        query = make_query(expected)
        client = MagicMock()
        client.table.return_value = query

        with patch("supabase_db.get_supabase_client", new=AsyncMock(return_value=client)):
            results = await supabase_db.query_batch_by_status(
                "https://test.supabase.co", "service-key", "meetings", "error"
            )

        assert results == expected
        query.eq.assert_called_once_with("state", "error")

    @pytest.mark.asyncio
    async def test_query_batch_recent_orders_desc_and_limits(self):
        expected = [{"id": 10}, {"id": 9}]
        query = make_query(expected)
        client = MagicMock()
        client.table.return_value = query

        with patch("supabase_db.get_supabase_client", new=AsyncMock(return_value=client)):
            results = await supabase_db.query_batch_recent(
                "https://test.supabase.co", "service-key", "meetings", 20
            )

        assert results == expected
        query.order.assert_called_once_with("created_at", desc=True)
        query.limit.assert_called_once_with(20)


class TestSyncWrappers:
    def test_run_insert_record_wraps_async_function(self):
        captured = {}

        def fake_asyncio_run(coro):
            captured["coro"] = coro
            try:
                coro.close()
            except Exception:
                pass
            return 42

        with patch("supabase_db.asyncio.run", side_effect=fake_asyncio_run) as mock_run:
            result = supabase_db.run_insert_record(
                "https://test.supabase.co", "service-key", "meetings", "meeting.mp3"
            )

        assert result == 42
        mock_run.assert_called_once()
        assert captured["coro"] is not None
