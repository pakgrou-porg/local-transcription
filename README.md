# Audio Meeting Processing Pipeline

Production-grade Python package for processing audio meeting recordings from Google Drive, transcribing them via HTTP, summarizing with LLM, and delivering results via Gmail.

## Project Status
- **Sprint 1-4 complete**: foundation, transcription, substitution, summarization, email rendering, Supabase persistence, Google Drive integration
- **Sprint 5 complete**: pipeline orchestration, normal mode, batch reprocessing, integration and functional tests

## Core Modules
- **main.py** — CLI entry point, logging, `normal` and `batch` modes
- **pipeline.py** — Orchestrates the normal cron pipeline and batch reprocessing
- **auth.py** — Google OAuth2 authentication with token persistence and refresh
- **preprocess.py** — Audio normalization to 16kHz mono using ffmpeg
- **transcribe.py** — HTTP transcription service client and transcript validation
- **substitute.py** — Name/term substitution support for transcripts and summary JSON
- **summarize.py** — Multi-provider summarization client for Docker, LMStudio, OpenRouter
- **render.py** — Summary-to-HTML renderer for Gmail-safe email content
- **email_sender.py** — Gmail API email sender with HTML body encoding
- **supabase_db.py** — Async Supabase persistence layer with retry and batch queries
- **drive.py** — Google Drive folder resolution, file listing/download/move operations

## Setup Instructions

### 1. Install Dependencies
```powershell
pip install -r requirements.txt
pip install -r requirements-test.txt
```

### 2. Configure Environment
```powershell
copy .env.example .env
# Edit .env with your credentials:
# - GOOGLE_CLIENT_SECRETS_FILE
# - GOOGLE_TOKEN_FILE
# - SUPABASE_URL
# - SUPABASE_SERVICE_KEY
# - SUPABASE_TABLE
# - EMAIL_RECIPIENT
# - Transcription and summarization provider configuration
```

### 3. Run Tests
```powershell
python -m pytest tests/test_pipeline_integration.py -q
python -m pytest tests/test_pipeline_functional.py -q
python -m pytest tests/ -q
```

## Deployment Instructions

### Prerequisites
- **Python 3.10+**: Ensure Python is installed and available in PATH.
- **ffmpeg**: Required for audio preprocessing.
  - **Linux**: `sudo apt install ffmpeg` (Ubuntu/Debian) or equivalent for your distro.
  - **Windows**: Download from [ffmpeg.org](https://ffmpeg.org/download.html) and add to PATH, or use `winget install ffmpeg`.
- **Google OAuth2 Credentials**: Download client secrets JSON from Google Cloud Console.
- **Supabase Account**: Set up database and obtain URL/service key.
- **Network Access**: Ensure endpoints for transcription, summarization, and Gmail API are reachable.

### Installation
1. Clone or download the repository to your system.
2. Navigate to the project directory.
3. Install dependencies as shown in Setup Instructions above.

### Configuration
1. Copy `.env.example` to `.env`.
2. Edit `.env` with your actual credentials and endpoints.
3. Ensure `GOOGLE_CLIENT_SECRETS_FILE` points to your downloaded OAuth2 client secrets file.
4. Test authentication by running `python main.py normal` once to trigger OAuth2 flow.

### Running the Pipeline
All production and maintenance flows run through `main.py`.

- **Normal mode**: `python main.py normal`
- **Process all queued source files**: `python main.py normal --all`
- **Process a capped source backlog**: `python main.py normal --all --limit 13`
- **Batch by IDs**: `python main.py batch --ids 1,2,3`
- **Batch by state**: `python main.py batch --status error`
- **Batch by recent records**: `python main.py batch --recent 20`
- **Batch by month**: `python main.py batch --month 2026-04`

- **From Scripts**:
  - Create a batch script (Windows) or shell script (Linux) to run the pipeline.
  - Example Windows batch file (`run_pipeline.bat`):
    ```
    @echo off
    cd /d "C:\path\to\Transcription"
    python main.py normal
    ```
  - Example Linux shell script (`run_pipeline.sh`):
    ```
    #!/bin/bash
    cd /path/to/Transcription
    python main.py normal
    ```
    Make executable: `chmod +x run_pipeline.sh`

### Scheduling as Jobs
- **Linux (cron)**:
  - Edit crontab: `crontab -e`
  - Add line for daily execution at 9 AM: `0 9 * * * /path/to/run_pipeline.sh`
  - For hourly: `0 * * * * /path/to/run_pipeline.sh`

- **Windows (Task Scheduler)**:
  - Open Task Scheduler.
  - Create new task: Action > Start a program.
  - Program/script: `C:\path\to\python.exe`
  - Arguments: `main.py normal`
  - Start in: `C:\path\to\Transcription`
  - Set triggers (e.g., daily at 9 AM).
  - Configure user account and permissions.

Ensure the scheduled job has access to the .env file and necessary permissions for file operations and network access.

## CLI Usage

### Normal mode
Process the first audio file in the source folder and execute the end-to-end pipeline.
```powershell
python main.py normal
python main.py normal --all
python main.py normal --all --limit 13
```

### Batch reprocessing
Reprocess existing records from Supabase by filter.
```powershell
python main.py batch --ids 1,2,3
python main.py batch --filename "meeting"
python main.py batch --status error
python main.py batch --status new
python main.py batch --status transcribed
python main.py batch --month 2026-04
python main.py batch --recent 20
```

Rows with state `error` or `new`, missing transcript text, or invalid transcript text are rebuilt from the stored Google Drive file ID. The pipeline downloads the recording from `ai_sources` or `archive`, preprocesses it, retranscribes it, applies substitutions, summarizes it, renders HTML, sends email, archives the Drive file, and updates Supabase.

## Pipeline Behavior
- `pipeline.py` performs startup recovery for interrupted jobs
- downloads audio from Drive
- preprocesses to 16kHz mono
- transcribes and validates transcript text
- archives newly discovered audio after successful transcription
- applies substitutions
- summarizes with LLM
- renders HTML
- sends email via Gmail
- updates Supabase state at each stage

## Shared Summary Schema
All summary modules honor this structure:
```json
{
  "meeting_subject": "string",
  "speakers": ["string"],
  "action_items": [{"assigned_to": "string", "action": "string"}],
  "discussion_topics": ["string"],
  "resourcing": ["string"]
}
```

## Notes
- Secrets must only be stored in `.env`
- Pipeline tests use mocked Drive/Gmail services and live-style orchestration flows
- Supabase persistence uses async retries for transient failures
- Email rendering is Gmail-compatible with inline CSS

## Recommended Next Step
Run the normal pipeline with your configured `.env` file:
```powershell
python main.py normal
```
