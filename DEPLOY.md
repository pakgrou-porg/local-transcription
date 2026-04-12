# Deployment Guide — Transcription Pipeline on Linux

Complete step-by-step instructions for deploying on a fresh Linux host.

---

## 1. System Requirements

| Requirement | Minimum |
|-------------|----------|
| OS | Ubuntu 22.04+ / Debian 12+ / RHEL 9+ |
| Python | 3.10+ |
| RAM | 512 MB (pipeline only, AI services run remotely) |
| Disk | 1 GB (for temp audio processing) |
| Network | Outbound HTTPS to Google APIs, Supabase; HTTP to AI service hosts |

## 2. Install System Dependencies

```bash
# Update packages
sudo apt update && sudo apt upgrade -y

# Install Python 3, pip, venv, and ffmpeg
sudo apt install -y python3 python3-pip python3-venv ffmpeg

# Verify installations
python3 --version    # Should be 3.10+
ffmpeg -version      # Should print version info
```

## 3. Create a Service User (Recommended)

```bash
# Create a dedicated user for running the pipeline
sudo useradd -r -m -s /bin/bash transcribe
sudo su - transcribe
```

## 4. Deploy the Application

### Option A: From ZIP file

```bash
# Copy the zip to the host and extract
cd /opt
sudo unzip /path/to/local-transcription.zip -d /opt/
sudo chown -R transcribe:transcribe /opt/local-transcription

# Switch to service user
sudo su - transcribe
cd /opt/local-transcription
```

### Option B: From GitHub

```bash
sudo su - transcribe
cd /opt
git clone https://github.com/pakgrou-porg/local-transcription.git
cd local-transcription
```

## 5. Create Python Virtual Environment

```bash
cd /opt/local-transcription

# Create virtual environment
python3 -m venv venv

# Activate it
source venv/bin/activate

# Upgrade pip
pip install --upgrade pip

# Install dependencies
pip install -r requirements.txt

# Verify installation
python -c "import supabase; import google.auth; import requests; print('All dependencies OK')"
```

## 6. Configure Environment

```bash
# Copy the example environment file
cp .env.example .env

# Edit with your credentials
nano .env
```

### Required values to fill in:

```ini
# Google OAuth2 — place your client_secrets.json in the project root
GOOGLE_CLIENT_SECRETS_FILE=client_secrets.json
GOOGLE_TOKEN_FILE=token.json
GOOGLE_DRIVE_SOURCE_FOLDER=ai_sources
GOOGLE_DRIVE_ARCHIVE_FOLDER=archive

# Supabase — from your Supabase project settings
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_SERVICE_KEY=eyJ...your-service-role-key

# Transcription service
TRANSCRIBE_BASE_URL=http://10.116.2.56:8101
TRANSCRIBE_MODEL_ID=CohereLabs/cohere-transcribe-03-2026
TRANSCRIBE_LANGUAGE=en
TRANSCRIBE_TIMEOUT_SECONDS=300

# Summarizer LLM service
SUMMARIZER_BASE_URL=http://10.116.2.56:8100
SUMMARIZER_API_KEY=
SUMMARIZER_MODEL=nvidia/nemotron-nano-30b
SUMMARIZER_TIMEOUT_SECONDS=120

# Email recipient
GMAIL_DESTINATION_ADDRESS=you@example.com

# Logging
LOG_DIR=./logs
```

## 7. Set Up Google OAuth2 Credentials

### 7a. Get client_secrets.json

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create or select a project
3. Enable **Google Drive API** and **Gmail API**:
   - APIs & Services → Library → search and enable each
4. Create OAuth 2.0 credentials:
   - APIs & Services → Credentials → Create Credentials → OAuth client ID
   - Application type: **Desktop app**
   - Download the JSON file
5. Copy it to the project root:

```bash
# From your local machine
scp client_secrets.json transcribe@your-host:/opt/local-transcription/
```

### 7b. Initial Authentication (requires browser access)

The first run needs an interactive browser session for OAuth consent:

```bash
cd /opt/local-transcription
source venv/bin/activate

# If the host has a browser:
python -m transcription_pipeline

# If the host is headless, use SSH port forwarding from your local machine:
# On your local machine:
ssh -L 8080:localhost:8080 transcribe@your-host

# Then on the remote host:
python -m transcription_pipeline
# Copy the URL shown, open in your local browser, complete consent
```

After consent, a `token.json` file is created. Subsequent runs use this
token automatically (it refreshes via the refresh token).

## 8. Set Up Supabase Database

1. Log into your [Supabase Dashboard](https://supabase.com/dashboard)
2. Open the **SQL Editor**
3. Paste and run the contents of `migration.sql`:

```bash
# View the migration
cat migration.sql
```

This creates:
- `transcription_data` table with state check constraint
- Indexes on `state`, `created_at`, `file_name`
- Auto-update trigger for `updated_at`

## 9. Set Up Google Drive Folders

In your Google Drive:

1. Create a folder named `ai_sources` (or whatever `GOOGLE_DRIVE_SOURCE_FOLDER` is set to)
2. Inside that folder, create a subfolder named `archive` (or `GOOGLE_DRIVE_ARCHIVE_FOLDER`)
3. Upload audio files (.mp3 or .wav) to the `ai_sources` folder

## 10. Test the Pipeline

```bash
cd /opt/local-transcription
source venv/bin/activate

# Run manually first to verify everything works
python -m transcription_pipeline

# Check the logs
tail -f logs/pipeline.log
```

Expected output for a successful run:
```
Transcription Pipeline starting
Mode: NORMAL (cron pipeline)
Checking for interrupted jobs...
Resolving Drive folder IDs...
Step 1: Scanning Drive for audio files...
Step 2: Selected file 'meeting_2026_04_12.mp3' ...
...
Processing complete for 'meeting_2026_04_12.mp3'
No audio files found. Exiting.
Transcription Pipeline finished
```

## 11. Configure Cron Schedule

```bash
# Edit crontab for the transcribe user
sudo -u transcribe crontab -e
```

Add one of these schedules:

```cron
# Every 15 minutes (recommended)
*/15 * * * * cd /opt/local-transcription && /opt/local-transcription/venv/bin/python -m transcription_pipeline >> /dev/null 2>&1

# Every 30 minutes during business hours (Mon-Fri, 8am-6pm)
*/30 8-18 * * 1-5 cd /opt/local-transcription && /opt/local-transcription/venv/bin/python -m transcription_pipeline >> /dev/null 2>&1

# Every hour
0 * * * * cd /opt/local-transcription && /opt/local-transcription/venv/bin/python -m transcription_pipeline >> /dev/null 2>&1
```

Verify the crontab was saved:

```bash
sudo -u transcribe crontab -l
```

## 12. Log Monitoring

```bash
# Follow live logs
tail -f /opt/local-transcription/logs/pipeline.log

# Check for errors
grep ERROR /opt/local-transcription/logs/pipeline.log

# Logs auto-rotate at midnight, keeping 10 days
ls -la /opt/local-transcription/logs/
```

## 13. Batch Reprocessing (Ad-Hoc)

```bash
cd /opt/local-transcription
source venv/bin/activate

# Reprocess specific row(s)
python -m transcription_pipeline --ids 42
python -m transcription_pipeline --ids 1,3,5,26,42

# Reprocess by filename
python -m transcription_pipeline --filename 2026_04_01

# Reprocess by state
python -m transcription_pipeline --status error

# Reprocess an entire month
python -m transcription_pipeline --month 2025-09
```

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| `ffmpeg not found` | `sudo apt install ffmpeg` |
| `ModuleNotFoundError` | Activate venv: `source venv/bin/activate` |
| OAuth consent fails | Ensure Drive + Gmail APIs are enabled in Google Cloud |
| `Token refresh failed` | Delete `token.json` and re-authenticate interactively |
| Transcription timeout | Increase `TRANSCRIBE_TIMEOUT_SECONDS` in `.env` |
| Supabase connection error | Verify `SUPABASE_URL` and `SUPABASE_SERVICE_KEY` |
| Email not sending | Check `GMAIL_DESTINATION_ADDRESS`; verify Gmail API is enabled |
| `state = 'error'` rows | Check logs for the specific error, fix, then batch reprocess |

## Security Checklist

- [ ] `.env` file permissions: `chmod 600 .env`
- [ ] `token.json` permissions: `chmod 600 token.json`
- [ ] `client_secrets.json` permissions: `chmod 600 client_secrets.json`
- [ ] Service user has minimal system permissions
- [ ] `.env` and `token.json` are not in version control (`.gitignore` handles this)
- [ ] Supabase service key is the **service_role** key, not the anon key

```bash
# Set secure permissions
chmod 600 /opt/local-transcription/.env
chmod 600 /opt/local-transcription/token.json
chmod 600 /opt/local-transcription/client_secrets.json
```
