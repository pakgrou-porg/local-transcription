"""Google OAuth2 authentication — token load, save, refresh, service builders."""

import http.server
import json
import logging
import os
import socket
import socketserver
import threading
import webbrowser
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


class _OAuthRedirectHandler(http.server.BaseHTTPRequestHandler):
    """HTTP request handler that captures the OAuth redirect with auth code."""

    auth_code = None
    error = None

    def do_GET(self):
        parsed = urlparse(self.path)
        params = parse_qs(parsed.query)

        if "code" in params:
            _OAuthRedirectHandler.auth_code = params["code"][0]
            self.send_response(200)
            self.send_header("Content-type", "text/html")
            self.end_headers()
            self.wfile.write(
                b"<html><body><h1>Success!</h1>"
                b"<p>Authorization complete. You can close this window.</p>"
                b"<script>setTimeout(() => window.close(), 2000);</script></body></html>"
            )
        elif "error" in params:
            _OAuthRedirectHandler.error = params.get("error", ["unknown"])[0]
            self.send_response(400)
            self.send_header("Content-type", "text/html")
            self.end_headers()
            self.wfile.write(
                f"<html><body><h1>Error</h1><p>{_OAuthRedirectHandler.error}</p></body></html>".encode()
            )
        else:
            self.send_response(400)
            self.send_header("Content-type", "text/html")
            self.end_headers()
            self.wfile.write(
                b"<html><body><h1>Bad Request</h1><p>No authorization code received.</p></body></html>"
            )

    def log_message(self, format, *args):
        """Suppress HTTP server log messages."""
        pass


def _find_open_port(start_port: int, max_attempts: int = 10) -> int:
    """Find an open port starting from start_port."""
    for port in range(start_port, start_port + max_attempts):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.bind(("", port))
                return port
            except OSError:
                continue
    raise OSError(f"Could not find open port in range {start_port}-{start_port + max_attempts}")


def get_credentials(client_secrets_file: str, token_file: str) -> Credentials:
    """Return valid Google OAuth2 credentials, launching auth flow if needed.

    This function supports headless servers by starting a local HTTP server
    to capture the OAuth redirect automatically.

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

    # Find an open port and start HTTP server to capture redirect
    port = _find_open_port(8080)
    handler = _OAuthRedirectHandler

    with socketserver.TCPServer(("", port), handler) as httpd:
        # Generate authorization URL pointing to our local server
        auth_url, _ = flow.authorization_url(
            access_type="offline",
            prompt="consent",
            include_granted_scopes="true",
        )

        # Replace the redirect port in the URL with our actual port
        parsed_auth = urlparse(auth_url)
        auth_params = parse_qs(parsed_auth.query)
        auth_params["redirect_uri"] = [f"http://localhost:{port}"]

        # Rebuild the URL with the correct redirect_uri
        from urllib.parse import urlencode

        new_query = urlencode(auth_params, doseq=True)
        auth_url = f"{parsed_auth.scheme}://{parsed_auth.netloc}{parsed_auth.path}?{new_query}"

        # Print the URL for manual authorization
        print("\n" + "=" * 70)
        print("AUTHORIZATION REQUIRED")
        print("=" * 70)
        print("\nPlease complete these steps:\n")
        print("1. Copy this URL into your browser:")
        print("\n" + auth_url + "\n")
        print("2. Sign in and click 'Allow' to authorize the app")
        print("3. After authorizing, you'll see a success page")
        print("4. This script will automatically continue...\n")
        print("=" * 70)

        # Start HTTP server in a thread
        server_thread = threading.Thread(target=httpd.serve_forever)
        server_thread.daemon = True
        server_thread.start()

        print(f"\nWaiting for authorization (server listening on port {port})...")

        # Wait for the auth code to be received
        timeout_seconds = 300  # 5 minutes
        start_time = __import__("time").time()

        while handler.auth_code is None and handler.error is None:
            if __import__("time").time() - start_time > timeout_seconds:
                httpd.shutdown()
                raise TimeoutError("Authorization timeout - took too long to complete")
            __import__("time").sleep(0.5)

        httpd.shutdown()

        if handler.error:
            raise ValueError(f"OAuth error: {handler.error}")

        # Exchange the authorization code for tokens
        print("Authorization code received! Exchanging for tokens...")
        flow.fetch_token(code=handler.auth_code)
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
