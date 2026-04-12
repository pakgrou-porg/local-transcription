#!/usr/bin/env python3
"""Transcription Pipeline — Entry Point.

Sequential, event-driven audio processing pipeline that:
  - Processes audio files from Google Drive
  - Transcribes via Cohere Transcribe HTTP service
  - Summarizes via Nemotron-Nano LLM HTTP service
  - Persists state in Supabase
  - Sends HTML email summaries via Gmail API

System dependencies (not pip-installable):
  - ffmpeg must be on PATH: apt install ffmpeg
  - Python 3.10+

Usage:
  Normal cron mode:   python -m transcription_pipeline
  Batch by IDs:       python -m transcription_pipeline --ids 1,3,5
  Batch by filename:  python -m transcription_pipeline --filename 2026_04_01
  Batch by status:    python -m transcription_pipeline --status summarized
  Batch by month:     python -m transcription_pipeline --month 2025-09
"""

import argparse
import logging
import os
import sys
from logging.handlers import TimedRotatingFileHandler

from dotenv import load_dotenv


def _setup_logging() -> None:
    """Configure logging with rotating file handler and stdout."""
    log_dir = os.environ.get("LOG_DIR", "./logs")

    # Resolve relative to the project directory (where main.py's package lives)
    if not os.path.isabs(log_dir):
        project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        log_dir = os.path.join(project_dir, log_dir)

    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, "pipeline.log")

    # Root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    # Formatter
    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # File handler — midnight rotation, keep 10 days
    file_handler = TimedRotatingFileHandler(
        filename=log_file,
        when="midnight",
        backupCount=10,
        encoding="utf-8",
    )
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(fmt)
    root_logger.addHandler(file_handler)

    # Stdout handler
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(fmt)
    root_logger.addHandler(stream_handler)

    logging.info("Logging initialized. Log file: %s", log_file)


def _parse_args() -> argparse.Namespace:
    """Parse CLI arguments for normal or batch mode."""
    parser = argparse.ArgumentParser(
        description="Transcription Pipeline — Audio processing automation",
    )

    batch_group = parser.add_mutually_exclusive_group()
    batch_group.add_argument(
        "--ids",
        type=str,
        help="Batch reprocess: single ID or comma-separated list (e.g., 214 or 1,3,5,26,42)",
    )
    batch_group.add_argument(
        "--filename",
        type=str,
        help="Batch reprocess: exact or prefix match on file_name (e.g., 2026_04_01)",
    )
    batch_group.add_argument(
        "--status",
        type=str,
        help="Batch reprocess: match rows by state value (e.g., summarized)",
    )
    batch_group.add_argument(
        "--month",
        type=str,
        help="Batch reprocess: match rows by month (e.g., 2025-09)",
    )

    return parser.parse_args()


def _determine_batch_mode(args: argparse.Namespace) -> tuple[str, str] | None:
    """Determine if batch mode is active and return (filter_type, filter_value).

    Returns None if no batch arguments were provided (normal mode).
    """
    if args.ids:
        return ("ids", args.ids)
    elif args.filename:
        return ("filename", args.filename)
    elif args.status:
        return ("status", args.status)
    elif args.month:
        return ("month", args.month)
    return None


def _validate_env() -> None:
    """Validate that required environment variables are set."""
    required = [
        "GOOGLE_CLIENT_SECRETS_FILE",
        "GOOGLE_DRIVE_SOURCE_FOLDER",
        "GOOGLE_DRIVE_ARCHIVE_FOLDER",
        "SUPABASE_URL",
        "SUPABASE_SERVICE_KEY",
        "TRANSCRIBE_BASE_URL",
        "TRANSCRIBE_MODEL_ID",
        "SUMMARIZER_BASE_URL",
        "SUMMARIZER_MODEL",
        "GMAIL_DESTINATION_ADDRESS",
    ]
    missing = [k for k in required if not os.environ.get(k)]
    if missing:
        logging.error("Missing required environment variables: %s", ", ".join(missing))
        sys.exit(1)


def main() -> None:
    """Main entry point."""
    # Load .env from the project directory
    project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    env_path = os.path.join(project_dir, ".env")
    load_dotenv(env_path)

    # Setup logging
    _setup_logging()
    logger = logging.getLogger(__name__)
    logger.info("═" * 60)
    logger.info("Transcription Pipeline starting")
    logger.info("═" * 60)

    # Parse CLI args
    args = _parse_args()
    batch_mode = _determine_batch_mode(args)

    if batch_mode:
        logger.info("Mode: BATCH (filter_type=%s, filter_value=%s)", *batch_mode)
    else:
        logger.info("Mode: NORMAL (cron pipeline)")

    # Validate environment
    _validate_env()

    # Late imports to avoid loading heavy modules before config is ready
    from transcription_pipeline.auth import (
        build_drive_service,
        build_gmail_service,
        get_credentials,
    )
    from transcription_pipeline.pipeline import run_batch_pipeline, run_normal_pipeline
    from transcription_pipeline.supabase_db import get_client as get_db_client

    # Authenticate
    logger.info("Authenticating with Google...")
    client_secrets = os.environ["GOOGLE_CLIENT_SECRETS_FILE"]
    token_file = os.environ.get("GOOGLE_TOKEN_FILE", "token.json")

    # Resolve relative paths to project directory
    if not os.path.isabs(client_secrets):
        client_secrets = os.path.join(project_dir, client_secrets)
    if not os.path.isabs(token_file):
        token_file = os.path.join(project_dir, token_file)

    creds = get_credentials(client_secrets, token_file)
    drive_service = build_drive_service(creds)
    gmail_service = build_gmail_service(creds)

    # Initialize Supabase client
    logger.info("Connecting to Supabase...")
    db_client = get_db_client(
        url=os.environ["SUPABASE_URL"],
        service_key=os.environ["SUPABASE_SERVICE_KEY"],
    )

    # Run pipeline
    try:
        if batch_mode:
            filter_type, filter_value = batch_mode
            run_batch_pipeline(
                filter_type=filter_type,
                filter_value=filter_value,
                drive_service=drive_service,
                gmail_service=gmail_service,
                db_client=db_client,
            )
        else:
            run_normal_pipeline(
                drive_service=drive_service,
                gmail_service=gmail_service,
                db_client=db_client,
            )
    except Exception as e:
        logger.error("Pipeline failed with unhandled exception: %s", e, exc_info=True)
        sys.exit(1)

    logger.info("═" * 60)
    logger.info("Transcription Pipeline finished")
    logger.info("═" * 60)


if __name__ == "__main__":
    main()
