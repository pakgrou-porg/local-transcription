import os
import asyncio
import logging
import json
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any
from supabase._async.client import AsyncClient
from supabase import create_async_client


logger = logging.getLogger(__name__)

# Exponential backoff retry configuration
MAX_RETRIES = 3
INITIAL_BACKOFF = 0.5  # seconds
MAX_BACKOFF = 5  # seconds

# Transient error codes (retry-able)
TRANSIENT_ERROR_CODES = {408, 429, 500, 502, 503, 504}

# Non-transient error codes (fail immediately)
NON_TRANSIENT_ERROR_CODES = {400, 401, 403, 404, 409}
# PostgREST / PostgreSQL API-level codes that should fail fast.
# PGRST205: table/schema metadata issue (won't recover by retrying)
# 23514: check constraint violation (invalid enum/state value)
NON_TRANSIENT_API_CODES = {"PGRST205", "23514"}


def utc_now_iso() -> str:
    """Return a timezone-aware UTC timestamp in ISO 8601 format."""
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


async def get_supabase_client(url: str, service_key: str) -> AsyncClient:
    """Return an async Supabase client."""
    return await create_async_client(url, service_key)


def extract_error_codes(exc: Exception) -> tuple[Optional[int], Optional[str]]:
    """Extract HTTP and API-level codes from Supabase/PostgREST exceptions."""
    http_code = getattr(exc, "http_code", None)
    api_code = getattr(exc, "code", None)

    if api_code is None and exc.args:
        first_arg = exc.args[0]
        if isinstance(first_arg, dict):
            api_code = first_arg.get("code")

    if api_code is not None:
        api_code = str(api_code)
    return http_code, api_code


async def sleep_with_jitter(base_delay: float) -> None:
    """Sleep for base_delay with random jitter."""
    import random
    jitter = random.uniform(0, 0.1 * base_delay)
    await asyncio.sleep(base_delay + jitter)


def get_retry_delay(attempt: int) -> float:
    """Calculate exponential backoff delay with max cap."""
    delay = INITIAL_BACKOFF * (2 ** attempt)
    return min(delay, MAX_BACKOFF)


async def insert_record(
    url: str,
    service_key: str,
    table: str,
    file_name: str,
    file_size: Optional[int] = None,
    drive_file_id: Optional[str] = None,
) -> Optional[int]:
    """
    Insert a new transcription record into Supabase.
    
    Creates a record with:
    - file_name, file_size, drive_file_id (provided)
    - state="new" (initial state)
    - created_at, updated_at (current time)
    - transcript, summary, html (NULL)
    
    Retries on transient errors with exponential backoff.
    
    Args:
        url (str): Supabase project URL
        service_key (str): Service role key
        table (str): Table name
        file_name (str): Audio file name
        file_size (int, optional): File size in bytes
        drive_file_id (str, optional): Google Drive file ID
        
    Returns:
        int: Inserted record ID, or None on failure
    """
    client = await get_supabase_client(url, service_key)
    
    now = utc_now_iso()
    
    data = {
        "file_name": file_name,
        "file_size": file_size,
        "drive_file_id": drive_file_id,
        "transcript": None,
        "summary": None,
        "html": None,
        "state": "new",
        "created_at": now,
        "updated_at": now,
    }
    
    for attempt in range(MAX_RETRIES):
        try:
            result = await client.table(table).insert(data).execute()
            
            if result.data and len(result.data) > 0:
                record_id = result.data[0].get("id")
                logger.info(f"Inserted record {record_id} for {file_name}")
                return record_id
            else:
                logger.error(f"Insert returned no data: {result}")
                return None
        
        except Exception as e:
            error_code, api_code = extract_error_codes(e)
            
            # Non-transient errors: fail immediately
            if (
                error_code in NON_TRANSIENT_ERROR_CODES
                or api_code in NON_TRANSIENT_API_CODES
            ):
                logger.error(
                    f"Non-transient error inserting record: "
                    f"http={error_code}, api={api_code} - {e}"
                )
                return None
            
            # Transient errors: retry
            if attempt < MAX_RETRIES - 1:
                delay = get_retry_delay(attempt)
                await sleep_with_jitter(delay)
                logger.warning(f"Insert attempt {attempt + 1} failed, retrying in {delay}s: {e}")
                continue
            else:
                logger.error(f"Insert failed after {MAX_RETRIES} attempts: {e}")
                return None


async def update_state(
    url: str,
    service_key: str,
    table: str,
    record_id: int,
    state: str,
) -> bool:
    """
    Update record state and updated_at timestamp.
    
    Args:
        url (str): Supabase project URL
        service_key (str): Service role key
        table (str): Table name
        record_id (int): Record ID to update
        state (str): New state value
        
    Returns:
        bool: True if successful, False otherwise
    """
    client = await get_supabase_client(url, service_key)
    
    now = utc_now_iso()
    
    data = {
        "state": state,
        "updated_at": now,
    }
    
    for attempt in range(MAX_RETRIES):
        try:
            result = await client.table(table).update(data).eq("id", record_id).execute()
            logger.info(f"Updated record {record_id} state to {state}")
            return True
        
        except Exception as e:
            error_code, api_code = extract_error_codes(e)
            
            if (
                error_code in NON_TRANSIENT_ERROR_CODES
                or api_code in NON_TRANSIENT_API_CODES
            ):
                logger.error(
                    f"Non-transient error updating state: "
                    f"http={error_code}, api={api_code} - {e}"
                )
                return False
            
            if attempt < MAX_RETRIES - 1:
                delay = get_retry_delay(attempt)
                await sleep_with_jitter(delay)
                logger.warning(f"Update state attempt {attempt + 1} failed, retrying: {e}")
                continue
            else:
                logger.error(f"Update state failed after {MAX_RETRIES} attempts: {e}")
                return False


async def update_transcript(
    url: str,
    service_key: str,
    table: str,
    record_id: int,
    transcript: str,
) -> bool:
    """Update record transcript field and updated_at."""
    client = await get_supabase_client(url, service_key)
    
    now = utc_now_iso()
    
    data = {
        "transcript": transcript,
        "updated_at": now,
    }
    
    for attempt in range(MAX_RETRIES):
        try:
            await client.table(table).update(data).eq("id", record_id).execute()
            logger.info(f"Updated record {record_id} transcript: {len(transcript)} chars")
            return True
        
        except Exception as e:
            error_code, api_code = extract_error_codes(e)
            
            if (
                error_code in NON_TRANSIENT_ERROR_CODES
                or api_code in NON_TRANSIENT_API_CODES
            ):
                logger.error(
                    f"Non-transient error updating transcript: "
                    f"http={error_code}, api={api_code} - {e}"
                )
                return False
            
            if attempt < MAX_RETRIES - 1:
                delay = get_retry_delay(attempt)
                await sleep_with_jitter(delay)
                logger.warning(f"Update transcript attempt {attempt + 1} failed, retrying: {e}")
                continue
            else:
                logger.error(f"Update transcript failed after {MAX_RETRIES} attempts: {e}")
                return False


async def update_summary(
    url: str,
    service_key: str,
    table: str,
    record_id: int,
    summary_json: str,
) -> bool:
    """Update record summary field (JSON string) and updated_at."""
    client = await get_supabase_client(url, service_key)
    
    now = utc_now_iso()
    
    data = {
        "summary": summary_json,
        "updated_at": now,
    }
    
    for attempt in range(MAX_RETRIES):
        try:
            await client.table(table).update(data).eq("id", record_id).execute()
            logger.info(f"Updated record {record_id} summary: {len(summary_json)} chars")
            return True
        
        except Exception as e:
            error_code, api_code = extract_error_codes(e)
            
            if (
                error_code in NON_TRANSIENT_ERROR_CODES
                or api_code in NON_TRANSIENT_API_CODES
            ):
                logger.error(
                    f"Non-transient error updating summary: "
                    f"http={error_code}, api={api_code} - {e}"
                )
                return False
            
            if attempt < MAX_RETRIES - 1:
                delay = get_retry_delay(attempt)
                await sleep_with_jitter(delay)
                logger.warning(f"Update summary attempt {attempt + 1} failed, retrying: {e}")
                continue
            else:
                logger.error(f"Update summary failed after {MAX_RETRIES} attempts: {e}")
                return False


async def update_html(
    url: str,
    service_key: str,
    table: str,
    record_id: int,
    html: str,
) -> bool:
    """Update record html field and updated_at."""
    client = await get_supabase_client(url, service_key)
    
    now = utc_now_iso()
    
    data = {
        "html": html,
        "updated_at": now,
    }
    
    for attempt in range(MAX_RETRIES):
        try:
            await client.table(table).update(data).eq("id", record_id).execute()
            logger.info(f"Updated record {record_id} html: {len(html)} chars")
            return True
        
        except Exception as e:
            error_code, api_code = extract_error_codes(e)
            
            if (
                error_code in NON_TRANSIENT_ERROR_CODES
                or api_code in NON_TRANSIENT_API_CODES
            ):
                logger.error(
                    f"Non-transient error updating html: "
                    f"http={error_code}, api={api_code} - {e}"
                )
                return False
            
            if attempt < MAX_RETRIES - 1:
                delay = get_retry_delay(attempt)
                await sleep_with_jitter(delay)
                logger.warning(f"Update html attempt {attempt + 1} failed, retrying: {e}")
                continue
            else:
                logger.error(f"Update html failed after {MAX_RETRIES} attempts: {e}")
                return False


async def get_interrupted_jobs(
    url: str,
    service_key: str,
    table: str,
) -> List[Dict[str, Any]]:
    """
    Get interrupted jobs that need recovery work.

    Recovery semantics:
    - Always include state='transcribed'
    - Also include resumable state='error' rows where transcript exists and
      summary/html delivery is incomplete. This lets startup recovery resume
      after downstream dependency outages (e.g., summarizer/email service).

    Orders by created_at ASC to process oldest first.
    
    Returns:
        List[Dict]: List of record dicts, or empty list on error
    """
    client = await get_supabase_client(url, service_key)
    
    try:
        transcribed_result = await (
            client.table(table)
            .select("*")
            .eq("state", "transcribed")
            .order("created_at", desc=False)
            .execute()
        )
        transcribed_records = transcribed_result.data or []

        error_result = await (
            client.table(table)
            .select("*")
            .eq("state", "error")
            .order("created_at", desc=False)
            .execute()
        )
        error_records = error_result.data or []

        resumable_error_records = [
            record
            for record in error_records
            if record.get("transcript")
            and (not record.get("summary") or not record.get("html"))
        ]

        records = transcribed_records + resumable_error_records
        records.sort(key=lambda r: r.get("created_at") or "")
        logger.info(
            f"Found {len(records)} interrupted jobs "
            f"({len(transcribed_records)} transcribed, "
            f"{len(resumable_error_records)} resumable_error)"
        )
        return records
    
    except Exception as e:
        logger.error(f"Failed to query interrupted jobs: {e}")
        return []


async def query_batch_by_ids(
    url: str,
    service_key: str,
    table: str,
    ids: List[int],
) -> List[Dict[str, Any]]:
    """
    Query records by list of IDs.
    
    Args:
        ids (List[int]): List of record IDs to fetch
        
    Returns:
        List[Dict]: Records matching any ID in the list
    """
    if not ids:
        return []
    
    client = await get_supabase_client(url, service_key)
    
    try:
        # Supabase doesn't have an IN operator, so we use multiple filters
        # For now, fetch each ID individually (not ideal but works)
        results = []
        for record_id in ids:
            result = await client.table(table).select("*").eq("id", record_id).execute()
            if result.data:
                results.extend(result.data)
        
        logger.info(f"Found {len(results)} records matching {len(ids)} IDs")
        return results
    
    except Exception as e:
        logger.error(f"Failed to query by IDs: {e}")
        return []


async def query_batch_by_month(
    url: str,
    service_key: str,
    table: str,
    year_month: str,  # format: YYYY-MM
) -> List[Dict[str, Any]]:
    """
    Query records created in a specific month.
    
    Args:
        year_month (str): Month in format YYYY-MM
        
    Returns:
        List[Dict]: Records created in that month
    """
    client = await get_supabase_client(url, service_key)
    
    # Parse year-month
    try:
        dt = datetime.strptime(year_month, "%Y-%m")
        month_start = dt.isoformat() + "Z"
        
        # Next month
        next_month = dt + timedelta(days=32)
        next_month = next_month.replace(day=1)
        month_end = next_month.isoformat() + "Z"
    except ValueError as e:
        logger.error(f"Invalid month format (use YYYY-MM): {year_month}")
        return []
    
    try:
        result = await (
            client.table(table)
            .select("*")
            .gte("created_at", month_start)
            .lt("created_at", month_end)
            .order("created_at", desc=False)
            .execute()
        )
        
        records = result.data or []
        logger.info(f"Found {len(records)} records in {year_month}")
        return records
    
    except Exception as e:
        logger.error(f"Failed to query by month: {e}")
        return []


async def query_batch_by_status(
    url: str,
    service_key: str,
    table: str,
    status: str,
) -> List[Dict[str, Any]]:
    """
    Query records by state value.
    
    Args:
        status (str): State value to match (e.g., 'error', 'transcribed')
        
    Returns:
        List[Dict]: Records matching that state
    """
    client = await get_supabase_client(url, service_key)
    
    try:
        result = await (
            client.table(table)
            .select("*")
            .eq("state", status)
            .order("created_at", desc=False)
            .execute()
        )
        
        records = result.data or []
        logger.info(f"Found {len(records)} records with state={status}")
        return records
    
    except Exception as e:
        logger.error(f"Failed to query by status: {e}")
        return []


async def query_batch_recent(
    url: str,
    service_key: str,
    table: str,
    limit: int,
) -> List[Dict[str, Any]]:
    """
    Query most recent records by created_at descending.

    Args:
        limit (int): Maximum number of records to return

    Returns:
        List[Dict]: Most recent records
    """
    if limit <= 0:
        return []

    client = await get_supabase_client(url, service_key)

    try:
        result = await (
            client.table(table)
            .select("*")
            .order("created_at", desc=True)
            .limit(limit)
            .execute()
        )

        records = result.data or []
        logger.info(f"Found {len(records)} recent records (limit={limit})")
        return records

    except Exception as e:
        logger.error(f"Failed to query recent records: {e}")
        return []


# ==================== SYNCHRONOUS WRAPPERS ====================
# These functions wrap the async methods for use in synchronous code

def run_insert_record(
    url: str,
    service_key: str,
    table: str,
    file_name: str,
    file_size: Optional[int] = None,
    drive_file_id: Optional[str] = None,
) -> Optional[int]:
    """Sync wrapper for insert_record()."""
    return asyncio.run(
        insert_record(url, service_key, table, file_name, file_size, drive_file_id)
    )


def run_update_state(
    url: str,
    service_key: str,
    table: str,
    record_id: int,
    state: str,
) -> bool:
    """Sync wrapper for update_state()."""
    return asyncio.run(update_state(url, service_key, table, record_id, state))


def run_update_transcript(
    url: str,
    service_key: str,
    table: str,
    record_id: int,
    transcript: str,
) -> bool:
    """Sync wrapper for update_transcript()."""
    return asyncio.run(update_transcript(url, service_key, table, record_id, transcript))


def run_update_summary(
    url: str,
    service_key: str,
    table: str,
    record_id: int,
    summary_json: str,
) -> bool:
    """Sync wrapper for update_summary()."""
    return asyncio.run(update_summary(url, service_key, table, record_id, summary_json))


def run_update_html(
    url: str,
    service_key: str,
    table: str,
    record_id: int,
    html: str,
) -> bool:
    """Sync wrapper for update_html()."""
    return asyncio.run(update_html(url, service_key, table, record_id, html))


def run_get_interrupted_jobs(
    url: str,
    service_key: str,
    table: str,
) -> List[Dict[str, Any]]:
    """Sync wrapper for get_interrupted_jobs()."""
    return asyncio.run(get_interrupted_jobs(url, service_key, table))


def run_query_batch_by_ids(
    url: str,
    service_key: str,
    table: str,
    ids: List[int],
) -> List[Dict[str, Any]]:
    """Sync wrapper for query_batch_by_ids()."""
    return asyncio.run(query_batch_by_ids(url, service_key, table, ids))


def run_query_batch_by_month(
    url: str,
    service_key: str,
    table: str,
    year_month: str,
) -> List[Dict[str, Any]]:
    """Sync wrapper for query_batch_by_month()."""
    return asyncio.run(query_batch_by_month(url, service_key, table, year_month))


def run_query_batch_by_status(
    url: str,
    service_key: str,
    table: str,
    status: str,
) -> List[Dict[str, Any]]:
    """Sync wrapper for query_batch_by_status()."""
    return asyncio.run(query_batch_by_status(url, service_key, table, status))


def run_query_batch_recent(
    url: str,
    service_key: str,
    table: str,
    limit: int,
) -> List[Dict[str, Any]]:
    """Sync wrapper for query_batch_recent()."""
    return asyncio.run(query_batch_recent(url, service_key, table, limit))
