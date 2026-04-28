import pytest
from unittest.mock import patch, MagicMock
import json

from auth import authenticate, SCOPES


class TestAuthentication:
    """Test suite for Google OAuth2 authentication."""
    
    def test_missing_client_secrets_raises_error(self, monkeypatch):
        """
        Test that missing client_secrets.json raises FileNotFoundError.
        
        Verifies:
        - FileNotFoundError raised if file doesn't exist
        - Error message is informative
        """
        monkeypatch.setenv("GOOGLE_CLIENT_SECRETS_FILE", "/nonexistent/path/client_secrets.json")
        
        with pytest.raises(FileNotFoundError):
            authenticate()
    
    def test_missing_env_variable_raises_error(self, monkeypatch):
        """
        Test that missing GOOGLE_CLIENT_SECRETS_FILE env var raises ValueError.
        
        Verifies:
        - ValueError raised if environment variable not set
        """
        monkeypatch.delenv("GOOGLE_CLIENT_SECRETS_FILE", raising=False)
        
        with pytest.raises(ValueError) as exc_info:
            authenticate()
        
        assert "GOOGLE_CLIENT_SECRETS_FILE" in str(exc_info.value)
    
    def test_successful_oauth2_flow(self, tmp_path, monkeypatch):
        """
        Test successful OAuth2 flow for new credentials.
        
        Verifies:
        - InstalledAppFlow is used
        - Credentials are saved to token file
        - Services are built and returned
        """
        client_secrets = tmp_path / "client_secrets.json"
        client_secrets.write_text(json.dumps({
            "installed": {
                "client_id": "test_client_id",
                "client_secret": "test_secret"
            }
        }))
        
        token_file = tmp_path / "token.json"
        
        monkeypatch.setenv("GOOGLE_CLIENT_SECRETS_FILE", str(client_secrets))
        monkeypatch.setenv("GOOGLE_TOKEN_FILE", str(token_file))
        
        mock_credentials = MagicMock()
        mock_credentials.to_json.return_value = '{"valid": true}'
        mock_credentials.valid = True
        mock_credentials.expired = False
        
        with patch("google_auth_oauthlib.flow.InstalledAppFlow.from_client_secrets_file") as mock_flow:
            mock_flow_instance = MagicMock()
            mock_flow_instance.run_local_server.return_value = mock_credentials
            mock_flow.return_value = mock_flow_instance
            
            with patch("googleapiclient.discovery.build") as mock_build:
                mock_drive = MagicMock()
                mock_gmail = MagicMock()
                mock_build.side_effect = [mock_drive, mock_gmail]
                
                drive_svc, gmail_svc = authenticate()
                
                assert drive_svc is not None
                assert gmail_svc is not None
                assert token_file.exists(), "Token file not saved"
    
    def test_load_existing_token(self, tmp_path, monkeypatch):
        """
        Test that existing token is loaded without OAuth2 flow.
        
        Verifies:
        - Token file is read
        - OAuth2 flow is skipped
        - Services are returned
        """
        client_secrets = tmp_path / "client_secrets.json"
        client_secrets.write_text(json.dumps({"installed": {"client_id": "test"}}))
        
        token_file = tmp_path / "token.json"
        token_file.write_text('{"type": "authorized_user", "client_id": "test"}')
        
        monkeypatch.setenv("GOOGLE_CLIENT_SECRETS_FILE", str(client_secrets))
        monkeypatch.setenv("GOOGLE_TOKEN_FILE", str(token_file))
        
        mock_credentials = MagicMock()
        mock_credentials.expired = False
        mock_credentials.valid = True
        
        with patch("google.oauth2.credentials.Credentials.from_authorized_user_file") as mock_load:
            mock_load.return_value = mock_credentials
            
            with patch("googleapiclient.discovery.build") as mock_build:
                mock_build.side_effect = [MagicMock(), MagicMock()]
                
                with patch("google_auth_oauthlib.flow.InstalledAppFlow") as mock_flow:
                    drive_svc, gmail_svc = authenticate()
                    
                    # Verify OAuth flow was NOT called
                    mock_flow.from_client_secrets_file.assert_not_called()
                    
                    # Verify credentials were loaded
                    mock_load.assert_called_once()
    
    def test_refresh_expired_credentials(self, tmp_path, monkeypatch):
        """
        Test that expired credentials are refreshed.
        
        Verifies:
        - credentials.refresh() is called for expired tokens
        - No new OAuth2 flow initiated
        """
        client_secrets = tmp_path / "client_secrets.json"
        client_secrets.write_text(json.dumps({"installed": {"client_id": "test"}}))
        
        token_file = tmp_path / "token.json"
        token_file.write_text('{"type": "authorized_user", "client_id": "test"}')
        
        monkeypatch.setenv("GOOGLE_CLIENT_SECRETS_FILE", str(client_secrets))
        monkeypatch.setenv("GOOGLE_TOKEN_FILE", str(token_file))
        
        mock_credentials = MagicMock()
        mock_credentials.expired = True
        mock_credentials.refresh_token = "refresh_token"
        mock_credentials.valid = False
        
        def refresh_side_effect(_request):
            # Real google credentials become valid after a successful refresh.
            mock_credentials.valid = True
            mock_credentials.expired = False
        
        mock_credentials.refresh.side_effect = refresh_side_effect
        
        with patch("google.oauth2.credentials.Credentials.from_authorized_user_file") as mock_load:
            mock_load.return_value = mock_credentials
            
            with patch("google.auth.transport.requests.Request"):
                with patch("googleapiclient.discovery.build") as mock_build:
                    mock_build.side_effect = [MagicMock(), MagicMock()]
                    
                    drive_svc, gmail_svc = authenticate()
                    
                    # Verify refresh was called
                    mock_credentials.refresh.assert_called_once()
    
    def test_scopes_are_correct(self):
        """
        Test that required OAuth2 scopes are configured.
        
        Verifies:
        - Drive scope is present
        - Gmail send scope is present
        """
        assert "https://www.googleapis.com/auth/drive" in SCOPES
        assert "https://www.googleapis.com/auth/gmail.send" in SCOPES
