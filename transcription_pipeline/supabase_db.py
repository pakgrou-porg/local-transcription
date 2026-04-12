"""Supabase database operations — all CRUD for transcription_data table."""

import logging
from datetime import datetime, timezone
from typing import Any

from supabase import Client, create_client

logger = logging.getLogger(__name__)

TABLE = "transcription_data"


def _utcnow_iso() -> str:
    """Return current UTC time as an ISO 8601 string with Z suffix."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def get_client(url: str, service_key: str) -> Client:
    """Create and return a Supabase client.

    Parameters
    ----------
    url : str
        Supabase project URL.
    service_key : str
        Supabase service role key.

    Returns
    -------
    supabase.Client
    """
    client = create_client(url, service_key)
    logger.info("Supabase client initialized for %s", url)
    return client


def insert_record(
    client: Client,
    file_name: str,
    drive_file_id: str,
    file_size: int | None,
) -> int:
    """Insert a new transcription_data record with state='new'.

    Parameters
    ----------
    client : supabase.Client
    file_name : str
    drive_file_id : str
    file_size : int | None

    Returns
    -------
    int
        The id of the inserted row.
    """
    now = _utcnow_iso()
    data = {
        "file_name": file_name,
        "drive_file_id": drive_file_id,
        "file_size": file_size,
        "state": "new",
        "created_at": now,
        "updated_at": now,
    }
    response = client.table(TABLE).insert(data).execute()
    row = response.data[0]
    row_id = row["id"]
    logger.info("Inserted record id=%d for file '%s'", row_id, file_name)
    return row_id


def update_state(client: Client, row_id: int, state: str) -> None:
    """Update the state column and updated_at timestamp.

    Parameters
    ----------
    client : supabase.Client
    row_id : int
    state : str
        One of: new, transcribed, summarized, html, archived, error.
    """
    client.table(TABLE).update({
        "state": state,
        "updated_at": _utcnow_iso(),
    }).eq("id", row_id).execute()
    logger.info("Updated row %d state -> '%s'", row_id, state)


def update_transcript(client: Client, row_id: int, transcript: str) -> None:
    """Save transcript text and set state='transcribed'.

    Parameters
    ----------
    client : supabase.Client
    row_id : int
    transcript : str
    """
    client.table(TABLE).update({
        "transcript": transcript,
        "state": "transcribed",
        "updated_at": _utcnow_iso(),
    }).eq("id", row_id).execute()
    logger.info("Updated row %d with transcript (%d chars)", row_id, len(transcript))


def update_summary(client: Client, row_id: int, summary: str) -> None:
    """Save summary JSON string and set state='summarized'.

    Parameters
    ----------
    client : supabase.Client
    row_id : int
    summary : str
        JSON string of the structured summary.
    """
    client.table(TABLE).update({
        "summary": summary,
        "state": "summarized",
        "updated_at": _utcnow_iso(),
    }).eq("id", row_id).execute()
    logger.info("Updated row %d with summary (%d chars)", row_id, len(summary))


def update_html(client: Client, row_id: int, html: str) -> None:
    """Save rendered HTML and set state='html'.

    Parameters
    ----------
    client : supabase.Client
    row_id : int
    html : str
        Rendered HTML email body.
    """
    client.table(TABLE).update({
        "html": html,
        "state": "html",
        "updated_at": _utcnow_iso(),
    }).eq("id", row_id).execute()
    logger.info("Updated row %d with HTML (%d chars)", row_id, len(html))


def finalize_record(client: Client, row_id: int) -> None:
    """Set state='archived' to mark processing complete.

    Parameters
    ----------
    client : supabase.Client
    row_id : int
    """
    client.table(TABLE).update({
        "state": "archived",
        "updated_at": _utcnow_iso(),
    }).eq("id", row_id).execute()
    logger.info("Finalized row %d -> 'archived'", row_id)


def get_interrupted_jobs(client: Client) -> list[dict[str, Any]]:
    """Return rows where state='transcribed' and summary is NULL.

    These represent jobs that were interrupted between transcription and
    summarization on a prior run.

    Returns
    -------
    list[dict]
        Rows ordered by created_at ASC.
    """
    response = (
        client.table(TABLE)
        .select("*")
        .eq("state", "transcribed")
        .is_("summary", "null")
        .order("created_at", desc=False)
        .execute()
    )
    rows = response.data
    logger.info("Found %d interrupted job(s)", len(rows))
    return rows


def query_batch(
    client: Client,
    filter_type: str,
    filter_value: str,
) -> list[dict[str, Any]]:
    """Query rows for batch reprocessing based on filter criteria.

    Parameters
    ----------
    client : supabase.Client
    filter_type : str
        One of: 'ids', 'filename', 'status', 'month'.
    filter_value : str
        The filter value as passed from CLI.

    Returns
    -------
    list[dict]
        Matching rows.
    """
    query = client.table(TABLE).select("*")

    if filter_type == "ids":
        id_list = [int(x.strip()) for x in filter_value.split(",") if x.strip()]
        if len(id_list) == 1:
            query = query.eq("id", id_list[0])
        else:
            query = query.in_("id", id_list)

    elif filter_type == "filename":
        # Exact match or prefix match using ilike
        query = query.ilike("file_name", f"{filter_value}%")

    elif filter_type == "status":
        query = query.eq("state", filter_value)

    elif filter_type == "month":
        # Parse YYYY-MM and build date range
        parts = filter_value.split("-")
        year = int(parts[0])
        month = int(parts[1])
        start = f"{year:04d}-{month:02d}-01T00:00:00Z"
        # Calculate first day of next month
        if month == 12:
            next_year = year + 1
            next_month = 1
        else:
            next_year = year
            next_month = month + 1
        end = f"{next_year:04d}-{next_month:02d}-01T00:00:00Z"
        query = query.gte("created_at", start).lt("created_at", end)

    else:
        raise ValueError(f"Unknown filter_type: {filter_type}")

    response = query.order("created_at", desc=False).execute()
    rows = response.data
    logger.info(
        "Batch query filter_type='%s' filter_value='%s' matched %d row(s)",
        filter_type, filter_value, len(rows),
    )
    return rows
