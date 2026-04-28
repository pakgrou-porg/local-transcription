import os
import sys
import logging
import asyncio
from pathlib import Path
from logging.handlers import TimedRotatingFileHandler
import argparse
from dotenv import load_dotenv

import pipeline


def setup_logging():
    """Initialize logging configuration with file and console handlers."""
    log_dir = Path(os.getenv("LOG_DIR", "./logs"))
    log_dir.mkdir(parents=True, exist_ok=True)
    
    log_file = log_dir / "pipeline.log"
    
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # Avoid duplicate handlers when main() is invoked repeatedly in-process.
    if logger.handlers:
        logger.handlers.clear()
    
    file_handler = TimedRotatingFileHandler(
        filename=str(log_file),
        when="midnight",
        backupCount=10
    )
    file_handler.setLevel(logging.INFO)
    
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(logging.INFO)
    
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    file_handler.setFormatter(formatter)
    stream_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    
    return logger


def create_parser():
    """Create and return argument parser for CLI."""
    parser = argparse.ArgumentParser(
        description="Audio Meeting Processing Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    subparsers = parser.add_subparsers(dest="command", help="Command to execute")
    
    # Normal pipeline command
    normal_parser = subparsers.add_parser(
        "normal",
        help="Run normal cron-driven pipeline (process first file in source folder)"
    )
    
    # Batch reprocess command
    batch_parser = subparsers.add_parser(
        "batch",
        help="Reprocess existing records from Supabase"
    )
    
    batch_group = batch_parser.add_mutually_exclusive_group(required=True)
    batch_group.add_argument(
        "--ids",
        type=str,
        help="Comma-separated list of record IDs (e.g., '214' or '1,3,5,26,42')"
    )
    batch_group.add_argument(
        "--filename",
        type=str,
        help="Exact or prefix match on file_name column"
    )
    batch_group.add_argument(
        "--status",
        type=str,
        help="Match by state column value (e.g., 'error', 'transcribed')"
    )
    batch_group.add_argument(
        "--month",
        type=str,
        help="Process files from a specific month (format: YYYY-MM)"
    )
    batch_group.add_argument(
        "--recent",
        type=int,
        help="Process most recent N records (e.g., 20)"
    )
    
    return parser


def main():
    """Entry point for the pipeline."""
    load_dotenv()
    logger = setup_logging()
    
    logger.info("Audio Meeting Processing Pipeline starting")
    
    parser = create_parser()
    args = parser.parse_args()
    
    if args.command is None:
        parser.print_help()
        logger.info("No command specified. Use 'normal' or 'batch' with appropriate options.")
        sys.exit(0)
    
    if args.command == "normal":
        logger.info("Normal pipeline mode selected")
        try:
            success = asyncio.run(pipeline.run_normal_pipeline())
            if success:
                logger.info("Normal pipeline executed successfully")
                sys.exit(0)
            else:
                logger.error("Normal pipeline failed")
                sys.exit(1)
        except Exception as e:
            logger.error(f"Pipeline error: {e}", exc_info=True)
            sys.exit(1)
    
    elif args.command == "batch":
        logger.info(f"Batch mode selected")
        try:
            # Determine filter type and value
            filter_type = None
            filter_value = None
            
            if args.ids:
                filter_type = "ids"
                filter_value = args.ids
            elif args.filename:
                filter_type = "filename"
                filter_value = args.filename
            elif args.status:
                filter_type = "status"
                filter_value = args.status
            elif args.month:
                filter_type = "month"
                filter_value = args.month
            elif args.recent is not None:
                filter_type = "recent"
                filter_value = str(args.recent)
            
            logger.info(f"Batch filter: {filter_type}={filter_value}")
            
            processed = asyncio.run(
                pipeline.run_batch_pipeline(filter_type, filter_value)
            )
            
            if processed > 0:
                logger.info(f"Batch pipeline processed {processed} record(s)")
                sys.exit(0)
            else:
                logger.warning("Batch pipeline completed with no records processed")
                sys.exit(1)
        except Exception as e:
            logger.error(f"Batch pipeline error: {e}", exc_info=True)
            sys.exit(1)
    
    logger.info("Audio Meeting Processing Pipeline completed")



if __name__ == "__main__":
    main()
