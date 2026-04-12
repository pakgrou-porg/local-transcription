# Transcription Pipeline

Sequential, event-driven audio processing pipeline that transcribes meeting
audio files from Google Drive, generates structured summaries via LLM, and
delivers HTML email reports.

## Architecture

```
Google Drive (audio files)
    │
    ▼
┌──────────────────────────────────────────┐
│  Transcription Pipeline (this package)   │
│                                          │
│  1. Download audio from Drive            │
│  2. Normalize with ffmpeg (16kHz mono)   │
│  3. Transcribe via Cohere HTTP service   │
│  4. Summarize via Nemotron LLM service   │
│  5. Apply text substitutions             │
│  6. Render HTML email                    │
│  7. Send via Gmail API                   │
│  8. Archive file on Drive                │
└──────────────────────────────────────────┘
    │               │               │
    ▼               ▼               ▼
 Supabase       Gmail API      Google Drive
 (state DB)     (email)        (archive)
```

## Prerequisites

### System Dependencies

```bash
# ffmpeg must be on PATH
sudo apt install ffmpeg

# Python 3.10+
python3 --version
```

### Python Dependencies

```bash
pip install -r requirements.txt
```

### Google Cloud Setup

1. Create a project in [Google Cloud Console](https://console.cloud.google.com/)
2. Enable the **Google Drive API** and **Gmail API**
3. Create OAuth 2.0 credentials (Desktop application)
4. Download the `client_secrets.json` file to the project root
5. On first run, the pipeline will open a browser for OAuth consent

### Supabase Setup

1. Create a Supabase project
2. Run the migration SQL in the Supabase SQL Editor:

```bash
# Copy the contents of migration.sql and execute in Supabase SQL Editor
cat migration.sql
```

### AI Services

The pipeline connects to two HTTP services (OpenAI-compatible APIs):

| Service | Default URL | Purpose |
|---------|-------------|----------|
| Cohere Transcribe | `http://10.116.2.56:8101` | Audio → text transcription |
| Nemotron-Nano-30B | `http://10.116.2.56:8100` | Transcript → structured summary |

## Configuration

```bash
# Copy the example and fill in values
cp .env.example .env
nano .env
```

See `.env.example` for all configuration keys and documentation.

## Usage

### Normal Mode (Cron)

Processes all audio files in the Drive source folder one at a time:

```bash
# Run directly
python -m transcription_pipeline

# Or via main.py
python transcription_pipeline/main.py
```

### Batch Reprocessing Mode

Reprocess existing records from Supabase:

```bash
# Single row by ID
python -m transcription_pipeline --ids 214

# Multiple rows by ID
python -m transcription_pipeline --ids 1,3,5,26,42

# By filename (exact or prefix match)
python -m transcription_pipeline --filename 2026_04_01

# By state
python -m transcription_pipeline --status summarized

# By month
python -m transcription_pipeline --month 2025-09
```

## Cron Schedule

To run the pipeline automatically, add a crontab entry.

### Recommended Schedule

```bash
# Edit crontab
crontab -e
```

#### Every 15 minutes (recommended for active use)
```cron
*/15 * * * * cd /path/to/transcription-pipeline && /path/to/python -m transcription_pipeline >> /dev/null 2>&1
```

#### Every hour
```cron
0 * * * * cd /path/to/transcription-pipeline && /path/to/python -m transcription_pipeline >> /dev/null 2>&1
```

#### Every 30 minutes during business hours (Mon–Fri, 8am–6pm)
```cron
*/30 8-18 * * 1-5 cd /path/to/transcription-pipeline && /path/to/python -m transcription_pipeline >> /dev/null 2>&1
```

#### Example with virtual environment
```cron
*/15 * * * * cd /opt/transcription-pipeline && /opt/transcription-pipeline/venv/bin/python -m transcription_pipeline >> /dev/null 2>&1
```

> **Note:** The pipeline handles its own logging to `{LOG_DIR}/pipeline.log`
> with midnight rotation (10-day retention). Cron output can safely be
> redirected to `/dev/null`.

## Pipeline States

Each file progresses through these states in Supabase:

| State | Description |
|-------|-------------|
| `new` | Record created, download starting |
| `transcribed` | Audio transcribed successfully |
| `summarized` | LLM summary generated |
| `html` | HTML email rendered |
| `archived` | Email sent, processing complete |
| `error` | Processing failed at some step |

## Recovery

The pipeline automatically recovers from interrupted runs:

- On startup, it checks for rows with `state='transcribed'` and `summary IS NULL`
- These interrupted jobs resume from the summarization step
- No re-download or re-transcription is needed
- Unrecoverable errors set `state='error'` and the pipeline moves on

## Text Substitutions

Edit `substitutions.txt` to normalize names in LLM output:

```
# Format: CanonicalName=regex_alt1|regex_alt2
Karl=Carl|Carul|Kharel
Michael=Micheal|Mikael|Mikhail
```

Substitutions are applied via `re.sub()` (case-insensitive) to the JSON
summary output — never by the LLM itself.

## Project Structure

```
transcription-pipeline/
├── transcription_pipeline/        # Python package
│   ├── __init__.py
│   ├── __main__.py                # python -m support
│   ├── main.py                    # Entry point, config, logging, CLI
│   ├── auth.py                    # Google OAuth2
│   ├── drive.py                   # Drive folder/file operations
│   ├── supabase_db.py             # All Supabase CRUD
│   ├── preprocess.py              # ffmpeg normalization
│   ├── transcribe.py              # Transcription HTTP client
│   ├── summarize.py               # LLM summarization client
│   ├── substitute.py              # Regex text substitution
│   ├── render.py                  # HTML email renderer
│   ├── email_sender.py            # Gmail API sender
│   └── pipeline.py                # Orchestration logic
├── migration.sql                  # Supabase DDL
├── requirements.txt               # Python dependencies
├── .env.example                   # Configuration template
├── substitutions.txt              # Name substitution rules
├── .gitignore
└── README.md
```

## Security

- All secrets loaded from `.env` (never hardcoded)
- `token.json` and `.env` are in `.gitignore`
- Temp files cleaned up in `finally` blocks
- `subprocess` calls use list form (never `shell=True`)
- Supabase uses service role key (server-side only)
- No credential values are ever logged
