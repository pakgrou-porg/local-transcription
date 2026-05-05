"""
Pipeline orchestration for audio meeting processing.

Normal pipeline (16 steps):
  1-4: Startup recovery (check for interrupted jobs)
  5-14: Process next file from source folder
  15-16: Log completion and sleep

Batch pipeline (reprocessing):
  Query records by filter (ids, filename, status, month)
  Re-summarize, re-render, re-send email for each
"""

import os
import asyncio
import logging
import json
import shutil
import tempfile
from pathlib import Path
from typing import Optional, List, Dict, Any

import auth
import preprocess
import transcribe
import substitute
import summarize
import render
import email_sender
import supabase_db
import drive

DEFAULT_COMPLETED_STATES = ("html", "completed", "complete")


def get_email_recipient() -> str:
    """
    Get email recipient based on TEST_MODE environment variable.
    
    TEST_MODE=true: Use GMAIL_TEST_DESTINATION_ADDRESS
    TEST_MODE=false or not set: Use GMAIL_DESTINATION_ADDRESS
    
    Returns:
        str: Email address to send summaries to
        
    Raises:
        ValueError: If required environment variable is missing
    """
    test_mode = os.getenv("TEST_MODE", "false").lower() == "true"
    
    if test_mode:
        recipient = os.getenv("GMAIL_TEST_DESTINATION_ADDRESS")
        if not recipient:
            raise ValueError("GMAIL_TEST_DESTINATION_ADDRESS required when TEST_MODE=true")
    else:
        recipient = os.getenv("GMAIL_DESTINATION_ADDRESS")
        if not recipient:
            raise ValueError("GMAIL_DESTINATION_ADDRESS required when TEST_MODE=false")
    
    return recipient


def get_summarizer_timeout() -> int:
    """
    Get summarizer timeout from environment variable.
    
    Returns:
        int: Timeout in seconds (default 120)
    """
    return int(os.getenv("SUMMARIZER_TIMEOUT_SECONDS", "120"))

logger = logging.getLogger(__name__)


def get_completed_states() -> List[str]:
    """
    Resolve candidate terminal states for successful processing.

    `PIPELINE_COMPLETED_STATES` can override defaults with a comma-separated
    list. Defaults keep backward compatibility while supporting schemas that
    use `complete` instead of `completed`.
    """
    raw = os.getenv("PIPELINE_COMPLETED_STATES", "")
    if raw.strip():
        candidates = [value.strip() for value in raw.split(",") if value.strip()]
    else:
        candidates = list(DEFAULT_COMPLETED_STATES)

    deduped = []
    seen = set()
    for state in candidates:
        if state not in seen:
            deduped.append(state)
            seen.add(state)
    return deduped


async def mark_record_completed(
    supabase_url: str,
    service_key: str,
    table: str,
    record_id: int,
) -> bool:
    """
    Persist terminal completed state with schema-compatible fallbacks.
    """
    for state in get_completed_states():
        if await supabase_db.update_state(
            supabase_url, service_key, table, record_id, state
        ):
            return True
        logger.warning(
            "Failed to set completed state '%s' for record %s; trying next candidate",
            state,
            record_id,
        )
    return False


def get_supabase_table_name() -> str:
    """
    Resolve the Supabase table name from environment variables.

    Priority:
    1) SUPABASE_TABLE (production/default runtime table)
    2) SUPABASE_TEST_TABLE (fallback for legacy/test deployments)
    3) meetings (hard fallback)
    """
    return (
        os.getenv("SUPABASE_TABLE")
        or os.getenv("SUPABASE_TEST_TABLE")
        or "meetings"
    )


def get_email_subject(file_name: str) -> str:
    """Build a stable email subject from the audio filename."""
    return file_name.replace(".mp3", "").replace(".wav", "")


def cleanup_local_artifacts(*paths: Optional[str]) -> None:
    """
    Remove temporary processing directories created for downloaded audio.

    The pipeline downloads Drive files into a temp directory prefixed with
    `archive_`; normalized audio is written into the same directory. Cleanup is
    restricted to those temp roots to avoid deleting unrelated files.
    """
    temp_root = Path(tempfile.gettempdir()).resolve()
    cleanup_dirs = set()

    for raw_path in paths:
        if not raw_path:
            continue
        try:
            path = Path(raw_path).resolve()
        except Exception:
            continue

        parent = path.parent
        if parent.parent == temp_root and parent.name.startswith("archive_"):
            cleanup_dirs.add(parent)

    for directory in cleanup_dirs:
        try:
            shutil.rmtree(directory, ignore_errors=True)
            logger.info(f"Cleaned temporary artifacts in {directory}")
        except Exception as e:
            logger.warning(f"Cleanup warning for {directory}: {e}")


async def apply_configured_substitutions(
    supabase_url: str,
    service_key: str,
    table: str,
    record_id: int,
    transcript: str,
    persist: bool = True,
) -> str:
    """Apply configured substitutions and optionally persist the result."""
    subs_file = os.getenv("SUBSTITUTIONS_FILE", "./substitutions.txt")
    if not os.path.exists(subs_file):
        return transcript

    substitutions = substitute.load_substitutions(subs_file)
    updated_transcript = substitute.apply_substitutions(transcript, substitutions)

    if persist and updated_transcript != transcript:
        await supabase_db.update_transcript(
            supabase_url, service_key, table, record_id, updated_transcript
        )

    logger.info("  Substitutions applied")
    return updated_transcript


async def archive_drive_file(
    drive_service,
    file_id: Optional[str],
    source_folder_id: str,
    archive_folder_name: str,
) -> bool:
    """Archive a processed Drive file if an ID is available."""
    if not file_id:
        logger.error("Cannot archive processed file: missing drive_file_id")
        return False

    archive_folder_id = drive.resolve_archive_folder_id(
        drive_service, source_folder_id, archive_folder_name
    )
    if not archive_folder_id:
        logger.error("Archive folder not found")
        return False

    return drive.archive_file_if_needed(
        drive_service, file_id, source_folder_id, archive_folder_id
    )


def parse_summary(summary_value: Any) -> Optional[Dict[str, Any]]:
    """Parse a saved summary field into a dictionary."""
    if not summary_value:
        return None
    if isinstance(summary_value, dict):
        return summary_value
    if isinstance(summary_value, str):
        try:
            return json.loads(summary_value)
        except json.JSONDecodeError as e:
            logger.warning(f"Invalid saved summary JSON: {e}")
            return None
    logger.warning(f"Unsupported summary type: {type(summary_value)}")
    return None


def get_summarizer_failure_reason(client: Any) -> str:
    """Return a useful reason when a summarizer call does not produce output."""
    return (
        getattr(client, "last_error", None)
        or "summarizer returned no summary and did not provide a failure reason"
    )


def summarize_transcript(client: Any, transcript: str, context: str) -> Optional[Dict[str, Any]]:
    """Summarize transcript and log the concrete failure reason when available."""
    try:
        summary_dict = client.summarize(transcript, timeout=get_summarizer_timeout())
    except Exception as e:
        logger.exception("%s summarization raised an exception: %s", context, e)
        return None

    if not summary_dict:
        logger.error("%s summarization failed: %s", context, get_summarizer_failure_reason(client))
        return None

    return summary_dict


def needs_transcript_rebuild(record: Dict[str, Any], transcript: Optional[str]) -> bool:
    """
    Determine whether a record should be rebuilt from archived audio.

    Rebuild conditions:
    - state='error'
    - transcript missing/empty
    - transcript fails validation (placeholder/corrupt)
    """
    if (record.get("state") or "").lower() == "error":
        return True
    if not transcript:
        return True
    return not transcribe.verify_transcript(transcript)


def needs_recovery_transcript_rebuild(
    transcript: Optional[str],
    summary_dict: Optional[Dict[str, Any]],
    html: Optional[str],
) -> bool:
    """Startup recovery rebuilds only when needed for the next incomplete stage."""
    if summary_dict and html:
        return False
    if summary_dict:
        return False
    if not transcript:
        return True
    return not transcribe.verify_transcript(transcript)


async def rebuild_transcript_from_archive(
    supabase_url: str,
    service_key: str,
    table: str,
    record_id: int,
    file_name: str,
    drive_file_id: Optional[str],
    drive_service,
) -> Optional[str]:
    """
    Rebuild transcript by downloading archived recording and re-transcribing it.
    """
    if not drive_file_id:
        logger.error("  Cannot rebuild transcript: missing drive_file_id")
        await supabase_db.update_state(
            supabase_url, service_key, table, record_id, "error"
        )
        return None

    temp_file = None
    wav_file = None

    try:
        temp_file = drive.download_file_from_archive(drive_service, drive_file_id, file_name)
        if not temp_file:
            logger.error("  Archive download failed")
            await supabase_db.update_state(
                supabase_url, service_key, table, record_id, "error"
            )
            return None

        wav_file = preprocess.preprocess_audio(temp_file)
        if not wav_file:
            logger.error("  Audio preprocessing failed")
            await supabase_db.update_state(
                supabase_url, service_key, table, record_id, "error"
            )
            return None

        transcript = transcribe.transcribe_file(wav_file)
        if not transcript:
            logger.error("  Re-transcription failed")
            await supabase_db.update_state(
                supabase_url, service_key, table, record_id, "error"
            )
            return None

        await supabase_db.update_transcript(
            supabase_url, service_key, table, record_id, transcript
        )
        await supabase_db.update_state(
            supabase_url, service_key, table, record_id, "transcribed"
        )
        logger.info(f"  Rebuilt transcript: {len(transcript)} characters")
        return transcript

    finally:
        cleanup_local_artifacts(temp_file, wav_file)


async def resume_interrupted_jobs(
    supabase_url: str,
    service_key: str,
    table: str,
    drive_service,
    gmail_service,
    source_folder_name: str,
    archive_folder_name: str,
) -> bool:
    """Resume any in-progress records that have not yet been completed."""
    logger.info("STEP[3-4]: Checking for interrupted jobs (startup recovery)")
    jobs = await supabase_db.get_interrupted_jobs(supabase_url, service_key, table)
    if not jobs:
        return False

    source_folder_id = drive.resolve_source_folder_id(drive_service, source_folder_name)
    if not source_folder_id:
        raise RuntimeError("Could not resolve source folder during recovery")

    logger.info(f"Found {len(jobs)} interrupted job(s), resuming delivery")
    succeeded = 0
    failed = 0

    for job in jobs:
        record_id = job["id"]
        file_name = job["file_name"]
        file_id = job.get("drive_file_id")
        transcript = job.get("transcript")
        summary_dict = parse_summary(job.get("summary"))
        html = job.get("html")

        try:
            logger.info(f"  RESUME: Job {record_id} ({file_name})")

            if needs_recovery_transcript_rebuild(transcript, summary_dict, html):
                logger.info("    Transcript missing/invalid; rebuilding from Drive...")
                transcript = await rebuild_transcript_from_archive(
                    supabase_url,
                    service_key,
                    table,
                    record_id,
                    file_name,
                    file_id,
                    drive_service,
                )
                if not transcript:
                    logger.error("    Transcript rebuild failed; job remains error")
                    failed += 1
                    continue
                summary_dict = None
                html = None

            if not summary_dict:
                if not transcript:
                    logger.error("    Missing transcript for interrupted job")
                    failed += 1
                    continue

                try:
                    logger.info("    Applying substitutions...")
                    transcript = await apply_configured_substitutions(
                        supabase_url,
                        service_key,
                        table,
                        record_id,
                        transcript,
                        persist=True,
                    )
                except Exception as e:
                    logger.warning(f"    Substitution warning (continuing): {e}")

                logger.info("    Summarizing transcript")
                client = summarize.build_from_env()
                summary_dict = summarize_transcript(
                    client, transcript, f"Recovery job {record_id}"
                )
                if not summary_dict:
                    failed += 1
                    continue

                await supabase_db.update_summary(
                    supabase_url, service_key, table, record_id, json.dumps(summary_dict)
                )

            if not html:
                logger.info("    Rendering summary to HTML")
                html = render.render_summary_to_html(summary_dict)
                await supabase_db.update_html(
                    supabase_url, service_key, table, record_id, html
                )

            logger.info("    Archiving processed file")
            if not await archive_drive_file(
                drive_service, file_id, source_folder_id, archive_folder_name
            ):
                logger.error("    Archive failed")
                failed += 1
                continue

            logger.info("    Sending summary email")
            recipient = get_email_recipient()
            try:
                email_sender.send_summary_email(
                    gmail_service, recipient, get_email_subject(file_name), html
                )
            except Exception as e:
                logger.exception(
                    "    Email delivery failed for job %s to %s: %s",
                    record_id,
                    recipient,
                    e,
                )
                await supabase_db.update_state(
                    supabase_url, service_key, table, record_id, "error"
                )
                failed += 1
                continue

            if not await mark_record_completed(
                supabase_url, service_key, table, record_id
            ):
                logger.error("    Failed to persist completed state")
                failed += 1
                continue
            logger.info(f"    Job {record_id} COMPLETED (resumed)")
            succeeded += 1

        except Exception as e:
            logger.error(f"    Error resuming job {record_id}: {e}")
            failed += 1

    logger.info(f"Resumed {succeeded}/{len(jobs)} job(s)")
    logger.info("=" * 70)
    return failed == 0


async def run_normal_pipeline() -> bool:
    """
    Normal cron pipeline: process next file in source folder.

    16-step pipeline:
    1-4: Load config, authenticate, check startup recovery
    5-9: Download, preprocess, transcribe, archive
    9-14: Archive, summarize, render, email, update state
    15-16: Log completion, return
    """
    logger.info("=" * 70)
    logger.info("STARTING NORMAL PIPELINE")
    logger.info("=" * 70)

    # STEP[1-2]: Load config and authenticate
    logger.info("STEP[1-2]: Loading config and authenticating")
    supabase_url = os.getenv("SUPABASE_URL")
    service_key = os.getenv("SUPABASE_SERVICE_KEY")
    table = get_supabase_table_name()
    source_folder_name = os.getenv("DRIVE_SOURCE_FOLDER", "Transcription Source")
    archive_folder_name = os.getenv(
        "DRIVE_ARCHIVE_FOLDER", "Transcription Archive"
    )

    if not all([supabase_url, service_key]):
        logger.error("Missing required env: SUPABASE_URL, SUPABASE_SERVICE_KEY")
        return False

    try:
        drive_service, gmail_service = auth.load_or_refresh_credentials()
        if not drive_service or not gmail_service:
            logger.error("Authentication failed")
            return False
    except Exception as e:
        logger.error(f"Authentication error: {e}")
        return False

    try:
        recovered = await resume_interrupted_jobs(
            supabase_url,
            service_key,
            table,
            drive_service,
            gmail_service,
            source_folder_name,
            archive_folder_name,
        )
        if recovered:
            logger.info("=" * 70)
            return True

    except Exception as e:
        logger.error(f"Startup recovery error: {e}")
        return False

    # STEP[5]: List source folder and select file
    logger.info("STEP[5]: Listing source folder")
    try:
        source_folder_id = drive.resolve_source_folder_id(
            drive_service, source_folder_name
        )
        if not source_folder_id:
            logger.error("Could not resolve source folder")
            return False

        files = drive.list_audio_files(drive_service, source_folder_id)
        if not files:
            logger.info("No audio files in source folder")
            logger.info("Pipeline complete (no files)")
            logger.info("=" * 70)
            return True

        file_to_process = files[0]
        file_id = file_to_process["id"]
        file_name = file_to_process["name"]
        file_size = file_to_process.get("size", 0)

        logger.info(f"  Selected: {file_name} ({file_size} bytes)")
    except Exception as e:
        logger.error(f"Source folder error: {e}")
        return False

    # Insert database record
    logger.info("STEP[5b]: Creating database record")
    try:
        record_id = await supabase_db.insert_record(
            supabase_url, service_key, table, file_name, file_size, file_id
        )
        if not record_id:
            logger.error("Failed to insert database record")
            return False
        logger.info(f"  Record created: ID {record_id}")
    except Exception as e:
        logger.error(f"Database error: {e}")
        return False

    temp_file = None
    wav_file = None

    try:
        # STEP[6]: Download file
        logger.info("STEP[6]: Downloading file from Drive")
        temp_file = drive.download_file_from_archive(
            drive_service, file_id, file_name
        )
        if not temp_file:
            logger.error("Download failed")
            await supabase_db.update_state(
                supabase_url, service_key, table, record_id, "error"
            )
            return False
        logger.info(f"  Downloaded: {temp_file}")

        # STEP[7]: Preprocess audio
        logger.info("STEP[7]: Preprocessing audio (ffmpeg)")
        wav_file = preprocess.preprocess_audio(temp_file)
        if not wav_file:
            logger.error("Preprocessing failed")
            await supabase_db.update_state(
                supabase_url, service_key, table, record_id, "error"
            )
            return False
        logger.info(f"  Preprocessed: {wav_file}")

        # STEP[8]: Transcribe
        logger.info("STEP[8]: Transcribing audio")
        transcript = transcribe.transcribe_file(wav_file)
        if not transcript:
            logger.error("Transcription failed")
            await supabase_db.update_state(
                supabase_url, service_key, table, record_id, "error"
            )
            return False
        logger.info(f"  Transcript: {len(transcript)} characters")

        await supabase_db.update_transcript(
            supabase_url, service_key, table, record_id, transcript
        )
        await supabase_db.update_state(
            supabase_url, service_key, table, record_id, "transcribed"
        )

        # STEP[9]: Move to archive after successful transcription
        logger.info("STEP[9]: Moving file to archive after transcription")
        if not await archive_drive_file(
            drive_service, file_id, source_folder_id, archive_folder_name
        ):
            logger.error("Archive failed")
            return False

        # STEP[10]: Apply substitutions
        logger.info("STEP[10]: Applying text substitutions")
        try:
            transcript = await apply_configured_substitutions(
                supabase_url, service_key, table, record_id, transcript, persist=True
            )
        except Exception as e:
            logger.warning(f"Substitution warning (continuing): {e}")

        # STEP[11]: Summarize
        logger.info("STEP[11]: Summarizing transcript")
        client = summarize.build_from_env()
        summary_dict = summarize_transcript(client, transcript, f"Record {record_id}")
        if not summary_dict:
            await supabase_db.update_state(
                supabase_url, service_key, table, record_id, "error"
            )
            return False
        logger.info(
            f"  Summary: {len(summary_dict.get('action_items', []))} action items"
        )

        await supabase_db.update_summary(
            supabase_url, service_key, table, record_id, json.dumps(summary_dict)
        )

        # STEP[12]: Render HTML
        logger.info("STEP[12]: Rendering summary to HTML")
        html = render.render_summary_to_html(summary_dict)
        if not html:
            logger.error("HTML rendering failed")
            await supabase_db.update_state(
                supabase_url, service_key, table, record_id, "error"
            )
            return False

        await supabase_db.update_html(
            supabase_url, service_key, table, record_id, html
        )

        # STEP[13]: Send email
        logger.info("STEP[13]: Sending summary email")
        recipient = get_email_recipient()
        try:
            email_sender.send_summary_email(
                gmail_service, recipient, get_email_subject(file_name), html
            )
        except Exception as e:
            logger.exception(
                "Email delivery failed for record %s to %s: %s",
                record_id,
                recipient,
                e,
            )
            await supabase_db.update_state(
                supabase_url, service_key, table, record_id, "error"
            )
            return False

        # STEP[14]: Update state
        logger.info("STEP[14]: Updating state to completed terminal value")
        if not await mark_record_completed(
            supabase_url, service_key, table, record_id
        ):
            logger.error("Failed to persist completed state")
            await supabase_db.update_state(
                supabase_url, service_key, table, record_id, "error"
            )
            return False
    except Exception as e:
        logger.error(f"Pipeline processing error: {e}")
        await supabase_db.update_state(
            supabase_url, service_key, table, record_id, "error"
        )
        return False
    finally:
        cleanup_local_artifacts(temp_file, wav_file)

    # STEP[15-16]: Log completion
    logger.info("STEP[15-16]: Pipeline complete")
    logger.info(f"  Record {record_id}: {file_name} → COMPLETED")
    logger.info("=" * 70)
    return True


async def count_source_audio_files() -> Optional[int]:
    """Return the current number of audio files waiting in the source folder."""
    source_folder_name = os.getenv("DRIVE_SOURCE_FOLDER", "Transcription Source")

    try:
        drive_service, _ = auth.load_or_refresh_credentials()
        if not drive_service:
            logger.error("Authentication failed while counting source audio files")
            return None

        source_folder_id = drive.resolve_source_folder_id(
            drive_service, source_folder_name
        )
        if not source_folder_id:
            logger.error("Could not resolve source folder while counting audio files")
            return None

        return len(drive.list_audio_files(drive_service, source_folder_id))
    except Exception as e:
        logger.error(f"Failed to count source audio files: {e}")
        return None


async def run_all_source_files(limit: Optional[int] = None) -> int:
    """
    Drain audio files from the source folder by repeatedly running normal mode.

    Returns the number of source files that were removed from the source folder.
    """
    logger.info("=" * 70)
    logger.info("STARTING SOURCE BACKLOG DRAIN")
    logger.info("=" * 70)

    processed = 0
    iterations = 0

    while limit is None or iterations < limit:
        before_count = await count_source_audio_files()
        if before_count is None:
            break
        if before_count == 0:
            logger.info("Source backlog is empty")
            break

        logger.info(
            "Source backlog drain iteration %s: %s audio file(s) remaining",
            iterations + 1,
            before_count,
        )

        success = await run_normal_pipeline()
        if not success:
            logger.error("Stopping source backlog drain after processing failure")
            break

        after_count = await count_source_audio_files()
        if after_count is None:
            break
        if after_count < before_count:
            processed += before_count - after_count
            if after_count == 0:
                logger.info("Source backlog is empty")
                break
        else:
            logger.warning(
                "Source backlog count did not decrease; stopping to avoid loop"
            )
            break

        iterations += 1

    logger.info("Source backlog drain complete: %s file(s) processed", processed)
    logger.info("=" * 70)
    return processed


async def run_batch_pipeline(filter_type: str, value: str) -> int:
    """
    Batch reprocess pipeline: re-summarize, re-render, re-send.

    Args:
        filter_type: 'ids', 'filename', 'status', 'month', 'recent'
        value: filter value (comma-separated ids, filename substring, status, YYYY-MM, count)

    Returns:
        Number of successfully processed records
    """
    logger.info("=" * 70)
    logger.info(f"STARTING BATCH PIPELINE ({filter_type}={value})")
    logger.info("=" * 70)

    # Load config
    supabase_url = os.getenv("SUPABASE_URL")
    service_key = os.getenv("SUPABASE_SERVICE_KEY")
    table = get_supabase_table_name()
    source_folder_name = os.getenv("DRIVE_SOURCE_FOLDER", "Transcription Source")
    archive_folder_name = os.getenv(
        "DRIVE_ARCHIVE_FOLDER", "Transcription Archive"
    )
    if not all([supabase_url, service_key]):
        logger.error("Missing required env: SUPABASE_URL, SUPABASE_SERVICE_KEY")
        return 0

    # Authenticate
    logger.info("Authenticating with Google API")
    try:
        drive_service, gmail_service = auth.load_or_refresh_credentials()
        if not drive_service or not gmail_service:
            logger.error("Authentication failed")
            return 0
    except Exception as e:
        logger.error(f"Authentication error: {e}")
        return 0

    source_folder_id = None

    # Query records
    logger.info(f"Querying records: {filter_type}={value}")
    try:
        if filter_type == "ids":
            ids = [int(x.strip()) for x in value.split(",")]
            records = await supabase_db.query_batch_by_ids(
                supabase_url, service_key, table, ids
            )
        elif filter_type == "filename":
            records = []
            for completed_state in get_completed_states():
                state_records = await supabase_db.query_batch_by_status(
                    supabase_url, service_key, table, completed_state
                )
                records.extend(state_records)

            unique_records = {}
            for record in records:
                unique_records[record.get("id")] = record

            records = [
                r
                for r in unique_records.values()
                if value.lower() in r.get("file_name", "").lower()
            ]
        elif filter_type == "status":
            records = await supabase_db.query_batch_by_status(
                supabase_url, service_key, table, value
            )
        elif filter_type == "month":
            records = await supabase_db.query_batch_by_month(
                supabase_url, service_key, table, value
            )
        elif filter_type == "recent":
            count = int(value)
            records = await supabase_db.query_batch_recent(
                supabase_url, service_key, table, count
            )
        else:
            logger.error(f"Unknown filter type: {filter_type}")
            return 0

        if not records:
            logger.info(f"No records found for {filter_type}={value}")
            logger.info("=" * 70)
            return 0

        logger.info(f"Found {len(records)} record(s) for reprocessing")
    except Exception as e:
        logger.error(f"Query error: {e}")
        return 0

    # Process each record
    processed_count = 0
    for record in records:
        try:
            record_id = record["id"]
            file_name = record["file_name"]
            drive_file_id = record.get("drive_file_id")
            transcript = record.get("transcript")

            logger.info(f"Reprocessing record {record_id}: {file_name}")

            if needs_transcript_rebuild(record, transcript):
                logger.info(
                    "  Transcript missing/invalid or state=error; rebuilding from archive..."
                )
                transcript = await rebuild_transcript_from_archive(
                    supabase_url,
                    service_key,
                    table,
                    record_id,
                    file_name,
                    drive_file_id,
                    drive_service,
                )
                if not transcript:
                    continue

            # Apply substitutions
            logger.info(f"  Applying substitutions...")
            try:
                transcript = await apply_configured_substitutions(
                    supabase_url,
                    service_key,
                    table,
                    record_id,
                    transcript,
                    persist=True,
                )
            except Exception as e:
                logger.warning(f"  Substitution warning (continuing): {e}")

            # Summarize
            logger.info(f"  Summarizing...")
            client = summarize.build_from_env()
            if not client:
                logger.error(f"  Summarizer client error")
                continue

            summary_dict = summarize_transcript(client, transcript, f"Batch record {record_id}")
            if not summary_dict:
                await supabase_db.update_state(
                    supabase_url, service_key, table, record_id, "error"
                )
                continue

            # Render HTML
            logger.info(f"  Rendering HTML...")
            html = render.render_summary_to_html(summary_dict)
            if not html:
                logger.error(f"  Rendering failed")
                continue

            # Send email
            logger.info(f"  Sending email...")
            recipient = get_email_recipient()
            try:
                email_sender.send_summary_email(
                    gmail_service, recipient, get_email_subject(file_name), html
                )
            except Exception as e:
                logger.exception(
                    "  Email delivery failed for record %s to %s: %s",
                    record_id,
                    recipient,
                    e,
                )
                await supabase_db.update_state(
                    supabase_url, service_key, table, record_id, "error"
                )
                continue

            # Archive the Drive file after successful downstream processing.
            if not source_folder_id:
                source_folder_id = drive.resolve_source_folder_id(
                    drive_service, source_folder_name
                )
                if not source_folder_id:
                    logger.error("  Could not resolve source folder for archive")
                    continue

            logger.info("  Archiving processed file...")
            if not await archive_drive_file(
                drive_service,
                drive_file_id,
                source_folder_id,
                archive_folder_name,
            ):
                logger.error(f"  Archive failed for record {record_id}")
                continue

            # Update database
            await supabase_db.update_summary(
                supabase_url, service_key, table, record_id,
                json.dumps(summary_dict),
            )
            await supabase_db.update_html(
                supabase_url, service_key, table, record_id, html
            )
            if not await mark_record_completed(
                supabase_url, service_key, table, record_id
            ):
                logger.error(f"  Failed to persist completed state for record {record_id}")
                continue

            logger.info(f"  ✓ Record {record_id} COMPLETED")
            processed_count += 1

        except Exception as e:
            logger.error(f"  Error processing record {record_id}: {e}")
            continue

    logger.info(
        f"Batch complete: {processed_count}/{len(records)} successfully processed"
    )
    logger.info("=" * 70)
    return processed_count


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "batch":
        # Batch mode: python pipeline.py batch <filter_type> <value>
        if len(sys.argv) < 4:
            print("Usage: python pipeline.py batch <filter_type> <value>")
            print("  filter_type: ids, filename, status, month")
            print("  value: comma-separated IDs, substring, status, or YYYY-MM")
            sys.exit(1)

        filter_type = sys.argv[2]
        value = sys.argv[3]
        processed = asyncio.run(run_batch_pipeline(filter_type, value))
        sys.exit(0 if processed > 0 else 1)
    else:
        # Normal mode
        success = asyncio.run(run_normal_pipeline())
        sys.exit(0 if success else 1)
