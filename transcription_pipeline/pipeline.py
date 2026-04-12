"""Pipeline orchestration — normal cron flow and batch reprocessing."""

import json
import logging
import os
import shutil
import tempfile
from typing import Any

from transcription_pipeline.drive import (
    archive_file,
    download_file,
    file_exists,
    list_audio_files,
    resolve_folder_id,
)
from transcription_pipeline.email_sender import EmailSendError, send_summary_email
from transcription_pipeline.preprocess import normalize_audio
from transcription_pipeline.render import render_html
from transcription_pipeline.substitute import apply_substitutions, load_substitutions
from transcription_pipeline.summarize import SummarizerError, call_summarizer
from transcription_pipeline.supabase_db import (
    finalize_record,
    get_interrupted_jobs,
    insert_record,
    query_batch,
    update_html,
    update_state,
    update_summary,
    update_transcript,
)
from transcription_pipeline.transcribe import (
    TranscriptionError,
    call_transcription_service,
    verify_transcript,
)

logger = logging.getLogger(__name__)


def _get_config(key: str, default: str = "") -> str:
    """Get a configuration value from environment."""
    return os.environ.get(key, default)


def _resolve_substitutions_path() -> str:
    """Resolve the substitutions file path relative to main.py location."""
    configured = _get_config("SUBSTITUTIONS_FILE", "substitutions.txt")
    if os.path.isabs(configured):
        return configured
    # Relative to the package directory (where main.py lives)
    package_dir = os.path.dirname(os.path.abspath(__file__))
    project_dir = os.path.dirname(package_dir)
    return os.path.join(project_dir, configured)


def _process_from_summarize(
    row_id: int,
    transcript: str,
    db_client,
    gmail_service,
    meeting_file_name: str,
) -> bool:
    """Run Steps 8–14: summarize, substitute, render, email, finalize.

    Parameters
    ----------
    row_id : int
        Supabase row ID.
    transcript : str
        The transcript text.
    db_client : supabase.Client
    gmail_service : Gmail service object.
    meeting_file_name : str
        File name for logging context.

    Returns
    -------
    bool
        True if processing completed successfully.
    """
    # --- Step 8: Summarize ---
    logger.info("[Row %d] Step 8: Summarizing transcript", row_id)
    try:
        summary_dict = call_summarizer(
            transcript=transcript,
            base_url=_get_config("SUMMARIZER_BASE_URL"),
            model=_get_config("SUMMARIZER_MODEL"),
            api_key=_get_config("SUMMARIZER_API_KEY", ""),
            timeout=int(_get_config("SUMMARIZER_TIMEOUT_SECONDS", "120")),
        )
    except SummarizerError as e:
        logger.error("[Row %d] Summarization failed: %s", row_id, e)
        update_state(db_client, row_id, "error")
        return False

    # --- Step 9: Text Substitution ---
    logger.info("[Row %d] Step 9: Applying text substitutions", row_id)
    subs_path = _resolve_substitutions_path()
    substitutions = load_substitutions(subs_path)
    summary_dict = apply_substitutions(summary_dict, substitutions)
    summary_json = json.dumps(summary_dict, ensure_ascii=False, indent=2)

    # --- Step 10: Save Summary ---
    logger.info("[Row %d] Step 10: Saving summary to Supabase", row_id)
    update_summary(db_client, row_id, summary_json)

    # --- Step 11: HTML Rendering ---
    logger.info("[Row %d] Step 11: Rendering HTML email", row_id)
    html_body = render_html(summary_dict)

    # --- Step 12: Save HTML ---
    logger.info("[Row %d] Step 12: Saving HTML to Supabase", row_id)
    update_html(db_client, row_id, html_body)

    # --- Step 13: Send Email ---
    logger.info("[Row %d] Step 13: Sending email", row_id)
    meeting_subject = summary_dict.get("meeting_subject", "Meeting Summary")
    dest_address = _get_config("GMAIL_DESTINATION_ADDRESS")
    if not dest_address:
        logger.error("[Row %d] GMAIL_DESTINATION_ADDRESS not configured", row_id)
        update_state(db_client, row_id, "error")
        return False

    try:
        send_summary_email(
            gmail_service=gmail_service,
            to_address=dest_address,
            meeting_subject=meeting_subject,
            html_body=html_body,
        )
    except EmailSendError as e:
        logger.error("[Row %d] Email send failed: %s", row_id, e)
        update_state(db_client, row_id, "error")
        return False

    # --- Step 14: Finalize ---
    logger.info("[Row %d] Step 14: Finalizing record", row_id)
    finalize_record(db_client, row_id)
    logger.info(
        "[Row %d] ✓ Processing complete for '%s'",
        row_id, meeting_file_name,
    )
    return True


def _download_and_transcribe(
    drive_service,
    drive_file_id: str,
    file_name: str,
    row_id: int,
    db_client,
) -> str | None:
    """Download, preprocess, and transcribe an audio file.

    Parameters
    ----------
    drive_service : Drive service object.
    drive_file_id : str
    file_name : str
    row_id : int
    db_client : supabase.Client

    Returns
    -------
    str | None
        Transcript text, or None on failure.
    """
    tmp_dir = tempfile.mkdtemp()
    try:
        # Download
        raw_path = os.path.join(tmp_dir, file_name)
        logger.info("[Row %d] Downloading '%s' to %s", row_id, file_name, raw_path)
        download_file(drive_service, drive_file_id, raw_path)

        # Preprocess
        logger.info("[Row %d] Preprocessing audio with ffmpeg", row_id)
        try:
            normalized_path = normalize_audio(raw_path, tmp_dir)
        except FileNotFoundError:
            logger.error("[Row %d] ffmpeg not found, cannot preprocess", row_id)
            update_state(db_client, row_id, "error")
            return None
        except Exception as e:
            logger.error("[Row %d] ffmpeg preprocessing failed: %s", row_id, e)
            update_state(db_client, row_id, "error")
            return None

        # Transcribe
        logger.info("[Row %d] Calling transcription service", row_id)
        try:
            transcript = call_transcription_service(
                audio_path=normalized_path,
                base_url=_get_config("TRANSCRIBE_BASE_URL"),
                model_id=_get_config("TRANSCRIBE_MODEL_ID"),
                language=_get_config("TRANSCRIBE_LANGUAGE", "en"),
                timeout=int(_get_config("TRANSCRIBE_TIMEOUT_SECONDS", "300")),
            )
        except TranscriptionError as e:
            logger.error("[Row %d] Transcription failed: %s", row_id, e)
            update_state(db_client, row_id, "error")
            return None

        # Verify transcript
        if not verify_transcript(transcript):
            logger.warning(
                "[Row %d] Transcript verification failed. First 200 chars: %s",
                row_id, transcript[:200],
            )
            update_state(db_client, row_id, "error")
            return None

        return transcript

    finally:
        # Always clean up temp files
        shutil.rmtree(tmp_dir, ignore_errors=True)
        logger.info("[Row %d] Cleaned up temp directory %s", row_id, tmp_dir)


def run_normal_pipeline(
    drive_service,
    gmail_service,
    db_client,
) -> None:
    """Execute the normal cron pipeline.

    1. Recover interrupted jobs (state='transcribed', summary IS NULL)
    2. Scan Drive for new audio files
    3. Process one file at a time through the full pipeline
    4. Loop until no files remain

    Parameters
    ----------
    drive_service : Drive v3 service object.
    gmail_service : Gmail v1 service object.
    db_client : supabase.Client
    """
    # ─── STARTUP: INTERRUPTED JOB RECOVERY ───
    logger.info("Checking for interrupted jobs...")
    interrupted = get_interrupted_jobs(db_client)

    for row in interrupted:
        row_id = row["id"]
        file_name = row.get("file_name", "unknown")
        transcript = row.get("transcript", "")

        if not transcript:
            logger.warning(
                "[Row %d] Interrupted job has no transcript, skipping", row_id
            )
            continue

        logger.info(
            "[Row %d] Recovering interrupted job for '%s'", row_id, file_name
        )
        _process_from_summarize(
            row_id=row_id,
            transcript=transcript,
            db_client=db_client,
            gmail_service=gmail_service,
            meeting_file_name=file_name,
        )

    # ─── MAIN LOOP ───
    source_folder_name = _get_config("GOOGLE_DRIVE_SOURCE_FOLDER")
    archive_folder_name = _get_config("GOOGLE_DRIVE_ARCHIVE_FOLDER")

    # Resolve folder IDs once
    logger.info("Resolving Drive folder IDs...")
    source_folder_id = resolve_folder_id(drive_service, source_folder_name)
    archive_folder_id = resolve_folder_id(
        drive_service, archive_folder_name, parent_id=source_folder_id
    )
    logger.info(
        "Source folder: %s (%s), Archive folder: %s (%s)",
        source_folder_name, source_folder_id,
        archive_folder_name, archive_folder_id,
    )

    while True:
        # --- Step 1: Google Drive Scan ---
        logger.info("Step 1: Scanning Drive for audio files...")
        audio_files = list_audio_files(drive_service, source_folder_id)

        if not audio_files:
            logger.info("No audio files found. Exiting.")
            return

        # --- Step 2: File Selection ---
        file_meta = audio_files[0]
        file_name = file_meta["name"]
        drive_file_id = file_meta["id"]
        file_size = int(file_meta.get("size", 0)) if file_meta.get("size") else None

        logger.info(
            "Step 2: Selected file '%s' (id=%s, size=%s)",
            file_name, drive_file_id, file_size,
        )

        row_id = None
        try:
            # --- Step 3: Supabase Record Creation ---
            logger.info("Step 3: Creating Supabase record for '%s'", file_name)
            row_id = insert_record(
                db_client,
                file_name=file_name,
                drive_file_id=drive_file_id,
                file_size=file_size,
            )

            # --- Steps 4–5: Download, Preprocess, Transcribe ---
            logger.info("[Row %d] Steps 4–5: Download, preprocess, transcribe", row_id)
            transcript = _download_and_transcribe(
                drive_service=drive_service,
                drive_file_id=drive_file_id,
                file_name=file_name,
                row_id=row_id,
                db_client=db_client,
            )
            if transcript is None:
                continue  # Error state already set, try next file on next loop

            # --- Step 6: Save Transcript ---
            logger.info("[Row %d] Step 6: Saving transcript", row_id)
            update_transcript(db_client, row_id, transcript)

            # --- Step 7: Archive Audio on Drive ---
            logger.info("[Row %d] Step 7: Archiving file on Drive", row_id)
            archive_file(
                drive_service,
                drive_file_id,
                source_folder_id,
                archive_folder_id,
            )

            # --- Steps 8–14: Summarize through Finalize ---
            _process_from_summarize(
                row_id=row_id,
                transcript=transcript,
                db_client=db_client,
                gmail_service=gmail_service,
                meeting_file_name=file_name,
            )

        except Exception as e:
            logger.error(
                "[Row %s] Unhandled exception processing '%s': %s",
                row_id or "?", file_name, e,
                exc_info=True,
            )
            if row_id:
                try:
                    update_state(db_client, row_id, "error")
                except Exception:
                    logger.error(
                        "[Row %d] Failed to set error state in Supabase", row_id
                    )
            # Exit process — next cron run retries
            raise


def run_batch_pipeline(
    filter_type: str,
    filter_value: str,
    drive_service,
    gmail_service,
    db_client,
) -> None:
    """Execute the batch reprocessing pipeline.

    Parameters
    ----------
    filter_type : str
        One of: 'ids', 'filename', 'status', 'month'.
    filter_value : str
        The CLI filter value.
    drive_service : Drive v3 service object.
    gmail_service : Gmail v1 service object.
    db_client : supabase.Client
    """
    logger.info(
        "Batch mode: filter_type='%s', filter_value='%s'",
        filter_type, filter_value,
    )

    rows = query_batch(db_client, filter_type, filter_value)
    if not rows:
        logger.info("No rows matched batch filter. Exiting.")
        return

    logger.info("Batch processing %d row(s)", len(rows))

    # Resolve archive folder for downloads (files are in archive)
    source_folder_name = _get_config("GOOGLE_DRIVE_SOURCE_FOLDER")
    archive_folder_name = _get_config("GOOGLE_DRIVE_ARCHIVE_FOLDER")
    source_folder_id = resolve_folder_id(drive_service, source_folder_name)
    archive_folder_id = resolve_folder_id(
        drive_service, archive_folder_name, parent_id=source_folder_id
    )

    for row in rows:
        row_id = row["id"]
        file_name = row.get("file_name", "unknown")
        drive_file_id = row.get("drive_file_id")

        logger.info(
            "[Row %d] Batch processing '%s' (drive_file_id=%s)",
            row_id, file_name, drive_file_id,
        )

        # Step 1: Validate drive_file_id
        if not drive_file_id:
            logger.error(
                "[Row %d] No drive_file_id for '%s', skipping", row_id, file_name
            )
            update_state(db_client, row_id, "error")
            continue

        # Step 2: Check file exists on Drive
        if not file_exists(drive_service, drive_file_id):
            logger.error(
                "[Row %d] File not found on Drive (id=%s, name='%s'), skipping",
                row_id, drive_file_id, file_name,
            )
            update_state(db_client, row_id, "error")
            continue

        try:
            # Steps 3–5: Download, preprocess, transcribe
            transcript = _download_and_transcribe(
                drive_service=drive_service,
                drive_file_id=drive_file_id,
                file_name=file_name,
                row_id=row_id,
                db_client=db_client,
            )
            if transcript is None:
                continue  # Error state already set

            # Step 6: Update transcript
            update_transcript(db_client, row_id, transcript)

            # Step 7: SKIP archive move — file stays in archive
            logger.info("[Row %d] Batch mode: skipping archive move", row_id)

            # Steps 8–14: Summarize through Finalize
            _process_from_summarize(
                row_id=row_id,
                transcript=transcript,
                db_client=db_client,
                gmail_service=gmail_service,
                meeting_file_name=file_name,
            )

        except Exception as e:
            logger.error(
                "[Row %d] Unhandled exception in batch processing '%s': %s",
                row_id, file_name, e,
                exc_info=True,
            )
            try:
                update_state(db_client, row_id, "error")
            except Exception:
                logger.error(
                    "[Row %d] Failed to set error state in Supabase", row_id
                )
            # Continue to next row in batch mode (don't exit)
            continue

    logger.info("Batch processing complete. Processed %d row(s).", len(rows))
