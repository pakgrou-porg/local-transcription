"""Google OAuth2 authentication — token load, save, refresh, service builders."""

import json
import logging
import os

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

logger = logging.getLogger(__name__)

SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/gmail.send",
]


def _load_credentials(token_file: str) -> Credentials | None:
    """Load credentials from a token JSON file, or return None."""
    if not os.path.exists(token_file):
        return None
    try:
        creds = Credentials.from_authorized_user_file(token_file, SCOPES)
        logger.info("Loaded existing credentials from %s", token_file)
        return creds
    except Exception:
        logger.warning("Failed to load credentials from %s, will re-authenticate", token_file)
        return None


def _save_credentials(creds: Credentials, token_file: str) -> None:
    """Persist credentials to a token JSON file."""
    token_dir = os.path.dirname(token_file)
    if token_dir:
        os.makedirs(token_dir, exist_ok=True)
    with open(token_file, "w") as f:
        f.write(creds.to_json())
    logger.info("Saved credentials to %s", token_file)


def get_credentials(client_secrets_file: str, token_file: str) -> Credentials:
    """Return valid Google OAuth2 credentials, launching auth flow if needed.

    Parameters
    ----------
    client_secrets_file : str
        Path to the OAuth2 client_secrets.json from Google Cloud Console.
    token_file : str
        Path where the token JSON is stored / will be stored.

    Returns
    -------
    google.oauth2.credentials.Credentials
        Valid (possibly freshly-refreshed) credentials.
    """
    creds = _load_credentials(token_file)

    if creds and creds.valid:
        return creds

    if creds and creds.expired and creds.refresh_token:
        try:
            creds.refresh(Request())
            logger.info("Refreshed expired credentials")
            _save_credentials(creds, token_file)
            return creds
        except Exception:
            logger.warning("Token refresh failed, will re-authenticate")

    # Full OAuth2 flow — first run or refresh failure
    logger.info("Starting OAuth2 installed-app flow")
    flow = InstalledAppFlow.from_client_secrets_file(
        client_secrets_file,
        scopes=SCOPES,
    )
    creds = flow.run_local_server(
        port=0,
        access_type="offline",
        prompt="consent",
    )
    _save_credentials(creds, token_file)
    return creds


def build_drive_service(creds: Credentials):
    """Build and return a Google Drive v3 service object."""
    service = build("drive", "v3", credentials=creds)
    logger.info("Built Google Drive v3 service")
    return service


def build_gmail_service(creds: Credentials):
    """Build and return a Gmail v1 service object."""
    service = build("gmail", "v1", credentials=creds)
    logger.info("Built Gmail v1 service")
    return service
