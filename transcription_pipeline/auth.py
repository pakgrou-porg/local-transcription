"""Google OAuth2 authentication — token load, save, refresh, service builders."""

import json
import logging
import os
from urllib.parse import parse_qs, urlparse

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

    This function supports headless servers by:
    1. Printing the authorization URL for manual copy/paste
    2. Accepting the authorization code from the redirect URL
    3. Exchanging the code for tokens

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

    # Generate authorization URL with PKCE (no redirect_uri needed for desktop apps)
    auth_url, _ = flow.authorization_url(
        access_type="offline",
        prompt="consent",
        include_granted_scopes="true",
    )

    # Print the URL for manual authorization
    print("\n" + "=" * 70)
    print("AUTHORIZATION REQUIRED")
    print("=" * 70)
    print("\nPlease complete these steps:\n")
    print("1. Copy this URL into your browser:")
    print("\n" + auth_url + "\n")
    print("2. Sign in and click 'Allow' to authorize the app")
    print("3. After authorizing, you will be redirected to a localhost URL")
    print("   (you may see 'Connection refused' or 'Page not found' — that's OK!)")
    print("4. Copy the FULL redirect URL from your browser's address bar")
    print("   (starts with http://localhost or http://127.0.0.1)")
    print("5. Paste it back into this terminal\n")
    print("=" * 70)

    # Get the redirect URL from user input
    redirect_url = input("Paste the redirect URL here: ").strip()

    if not redirect_url or not redirect_url.startswith("http://localhost") and not redirect_url.startswith("http://127.0.0.1"):
        print("\nError: Invalid redirect URL. Must start with http://localhost or http://127.0.0.1")
        raise ValueError("Invalid redirect URL provided")

    # Extract the authorization code from the redirect URL
    parsed = urlparse(redirect_url)
    params = parse_qs(parsed.query)
    auth_code = params.get("code", [None])[0]

    if not auth_code:
        print("\nError: No authorization code found in the redirect URL.")
        print(f"URL provided: {redirect_url[:100]}...")
        raise ValueError("No authorization code in redirect URL")

    # Exchange the authorization code for tokens
    flow.fetch_token(code=auth_code)
    creds = flow.credentials
    _save_credentials(creds, token_file)

    print("\n✓ Authorization successful! Credentials saved.")
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
