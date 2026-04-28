import os
import sys
import json
import logging
from pathlib import Path
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.exceptions import RefreshError
from googleapiclient.discovery import build


logger = logging.getLogger(__name__)

SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/gmail.send",
]


def authenticate():
    """
    Authenticate with Google Drive and Gmail using OAuth2.
    
    Returns:
        tuple: (drive_service, gmail_service)
        
    Raises:
        FileNotFoundError: If client_secrets.json file not found
        Exception: On authentication failure
    """
    def _get_os_name() -> str:
        if sys.platform.startswith("win"):
            return "WINDOWS"
        if sys.platform.startswith("linux"):
            return "LINUX"
        if sys.platform.startswith("darwin"):
            return "MACOS"
        return sys.platform.upper()

    def _get_os_specific_env(key: str, default=None):
        os_name = _get_os_name()
        specific_key = f"{key}_{os_name}"
        return os.getenv(specific_key, os.getenv(key, default))

    client_secrets_file = _get_os_specific_env("GOOGLE_CLIENT_SECRETS_FILE")
    token_file = _get_os_specific_env("GOOGLE_TOKEN_FILE", "token.json")
    
    if not client_secrets_file:
        raise ValueError("GOOGLE_CLIENT_SECRETS_FILE or OS-specific variant not set in .env")
    
    if not Path(client_secrets_file).exists():
        raise FileNotFoundError(f"Client secrets file not found: {client_secrets_file}")
    
    credentials = None
    
    # Try to load existing token
    if Path(token_file).exists():
        try:
            credentials = Credentials.from_authorized_user_file(token_file, SCOPES)
            logger.info("Loaded credentials from token file")
        except Exception as e:
            logger.warning(f"Failed to load token file: {e}")
            credentials = None
    
    # Refresh token if it exists and is expired
    if credentials and credentials.expired and credentials.refresh_token:
        try:
            credentials.refresh(Request())
            logger.info("Refreshed expired credentials")
        except RefreshError as e:
            logger.warning(f"Failed to refresh credentials: {e}")
            credentials = None
    
    # If no valid credentials, run OAuth2 flow
    if not credentials or not credentials.valid:
        logger.info("Starting OAuth2 flow for new credentials")
        try:
            flow = InstalledAppFlow.from_client_secrets_file(
                client_secrets_file,
                SCOPES,
                redirect_uri="http://localhost:8088/"
            )
            credentials = flow.run_local_server(
                port=8088,
                open_browser=False,
                prompt="consent"
            )
            
            # Save token for future use
            with open(token_file, "w") as f:
                f.write(credentials.to_json())
            logger.info(f"Saved new credentials to {token_file}")
        except Exception as e:
            logger.error(f"OAuth2 authentication failed: {e}")
            raise
    
    # Build service clients
    drive_service = build("drive", "v3", credentials=credentials)
    gmail_service = build("gmail", "v1", credentials=credentials)
    
    logger.info("Successfully authenticated with Google Drive and Gmail APIs")
    return drive_service, gmail_service


def load_or_refresh_credentials():
    """Return authenticated Drive and Gmail service clients."""
    return authenticate()
