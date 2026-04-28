import pytest
import json
import os
from unittest.mock import patch, MagicMock
import requests

from summarize import SummarizerClient, build_from_env


class TestSummarizerClientInit:
    """Test suite for SummarizerClient initialization."""
    
    def test_docker_provider_initialization(self):
        """
        Test initialization of docker provider client.
        
        Verifies:
        - Client created successfully
        - Provider set correctly
        """
        client = SummarizerClient(provider="docker", base_url="http://localhost:8100", model="neural-chat")
        
        assert client.provider == "docker"
        assert client.model == "neural-chat"
    
    def test_lmstudio_provider_initialization(self):
        """Test initialization of lmstudio provider client."""
        client = SummarizerClient(provider="lmstudio", base_url="http://localhost:1234", model="llama2")
        
        assert client.provider == "lmstudio"
        assert client.model == "llama2"
    
    def test_openrouter_provider_initialization(self):
        """Test initialization of openrouter provider client."""
        client = SummarizerClient(provider="openrouter", api_key="test_key", model="openai/gpt-4")
        
        assert client.provider == "openrouter"
        assert client.api_key == "test_key"
        assert client.model == "openai/gpt-4"
    
    def test_invalid_provider_raises_error(self):
        """Test that invalid provider raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            SummarizerClient(provider="invalid", model="test")
        
        assert "unknown provider" in str(exc_info.value).lower()
    
    def test_provider_case_insensitive(self):
        """Test that provider name is case-insensitive."""
        client = SummarizerClient(provider="DOCKER", base_url="http://localhost:8100", model="test")
        
        assert client.provider == "docker"


class TestEndpointDetection:
    """Test suite for dynamic endpoint detection."""
    
    def test_endpoint_bare_host(self):
        """
        Test endpoint detection for bare host (no /v1 suffix).
        
        Verifies:
        - /v1/chat/completions appended
        """
        client = SummarizerClient(provider="docker", base_url="http://localhost:8100", model="test")
        endpoint = client._get_endpoint()
        
        assert endpoint == "http://localhost:8100/v1/chat/completions"
    
    def test_endpoint_v1_suffix(self):
        """
        Test endpoint detection for /v1 suffix.
        
        Verifies:
        - /chat/completions appended
        """
        client = SummarizerClient(provider="docker", base_url="http://localhost:8100/v1", model="test")
        endpoint = client._get_endpoint()
        
        assert endpoint == "http://localhost:8100/v1/chat/completions"
    
    def test_endpoint_full_path(self):
        """
        Test endpoint detection for full path.
        
        Verifies:
        - Used as-is
        """
        client = SummarizerClient(provider="docker", base_url="http://localhost:8100/v1/chat/completions", model="test")
        endpoint = client._get_endpoint()
        
        assert endpoint == "http://localhost:8100/v1/chat/completions"
    
    def test_openrouter_endpoint(self):
        """Test that openrouter uses correct endpoint."""
        client = SummarizerClient(provider="openrouter", api_key="key", model="test")
        endpoint = client._get_endpoint()
        
        assert endpoint == "https://openrouter.ai/api/v1/chat/completions"
    
    def test_missing_base_url_raises_error(self):
        """Test that missing base_url for docker raises error."""
        client = SummarizerClient(provider="docker", model="test")
        
        with pytest.raises(ValueError):
            client._get_endpoint()


class TestRequestBodyBuilding:
    """Test suite for request body construction."""
    
    def test_docker_provider_no_stream_field(self):
        """
        Test that docker provider doesn't include stream field.
        
        Verifies:
        - request_format included
        - stream field absent
        """
        client = SummarizerClient(provider="docker", base_url="http://localhost:8100", model="test")
        body = client._build_request_body("Test transcript")
        
        assert "response_format" in body
        assert body["response_format"]["type"] == "json_object"
        assert "stream" not in body
    
    def test_lmstudio_provider_stream_false(self):
        """
        Test that lmstudio provider includes stream=false.
        
        Verifies:
        - stream field set to false
        """
        client = SummarizerClient(provider="lmstudio", base_url="http://localhost:1234", model="llama2")
        body = client._build_request_body("Test transcript")
        
        assert body["stream"] is False
        assert "response_format" in body
    
    def test_openrouter_provider_stream_false(self):
        """Test that openrouter includes stream=false."""
        client = SummarizerClient(provider="openrouter", api_key="key", model="test")
        body = client._build_request_body("Test transcript")
        
        assert body["stream"] is False
        assert "response_format" in body
    
    def test_request_body_has_required_fields(self):
        """
        Test that request body includes all required fields.
        
        Verifies:
        - model, messages, response_format
        """
        client = SummarizerClient(provider="docker", base_url="http://localhost:8100", model="test-model")
        body = client._build_request_body("Test transcript content")
        
        assert body["model"] == "test-model"
        assert "messages" in body
        assert len(body["messages"]) == 2
        assert body["messages"][0]["role"] == "system"
        assert body["messages"][1]["role"] == "user"
        assert "response_format" in body


class TestHeadersBuilding:
    """Test suite for HTTP headers construction."""
    
    def test_docker_provider_no_auth_header(self):
        """Test that docker provider doesn't include auth header."""
        client = SummarizerClient(provider="docker", base_url="http://localhost:8100", model="test")
        headers = client._build_headers()
        
        assert "Authorization" not in headers
        assert "Content-Type" in headers
    
    def test_lmstudio_provider_no_auth_header(self):
        """Test that lmstudio provider doesn't include auth header."""
        client = SummarizerClient(provider="lmstudio", base_url="http://localhost:1234", model="llama2")
        headers = client._build_headers()
        
        assert "Authorization" not in headers
    
    def test_openrouter_provider_auth_header(self):
        """
        Test that openrouter provider includes Authorization header.
        
        Verifies:
        - Bearer token format
        """
        client = SummarizerClient(provider="openrouter", api_key="test_key_123", model="openai/gpt-4")
        headers = client._build_headers()
        
        assert "Authorization" in headers
        assert headers["Authorization"] == "Bearer test_key_123"
    
    def test_openrouter_missing_api_key_raises_error(self):
        """Test that openrouter without api_key raises error."""
        client = SummarizerClient(provider="openrouter", model="test")
        
        with pytest.raises(ValueError) as exc_info:
            client._build_headers()
        
        assert "api_key" in str(exc_info.value).lower()


class TestSummarization:
    """Test suite for summarization functionality."""
    
    def test_successful_summarization(self):
        """
        Test successful summarization with valid response.
        
        Mocks HTTP response with valid OpenAI-compatible format.
        """
        client = SummarizerClient(provider="docker", base_url="http://localhost:8100", model="test")
        
        summary_json = {
            "meeting_subject": "Q2 Planning",
            "speakers": ["John", "Sarah"],
            "action_items": [{"assigned_to": "John", "action": "Follow up"}],
            "discussion_topics": ["Budget"],
            "resourcing": ["Team A"]
        }
        
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "choices": [
                {
                    "message": {
                        "content": json.dumps(summary_json)
                    }
                }
            ]
        }
        
        with patch("requests.post", return_value=mock_response):
            result = client.summarize("Test transcript with enough content for summarization")
            
            assert result is not None
            assert result["meeting_subject"] == "Q2 Planning"
            assert len(result["speakers"]) == 2
    
    def test_http_error_returns_none(self):
        """Test that HTTP errors return None."""
        client = SummarizerClient(provider="docker", base_url="http://localhost:8100", model="test")
        
        with patch("requests.post", side_effect=requests.RequestException("Connection failed")):
            result = client.summarize("Test transcript")
            
            assert result is None
    
    def test_timeout_error_returns_none(self):
        """Test that timeout errors return None."""
        client = SummarizerClient(provider="docker", base_url="http://localhost:8100", model="test")
        
        with patch("requests.post", side_effect=requests.Timeout("Timed out")):
            result = client.summarize("Test transcript", timeout=5)
            
            assert result is None
    
    def test_invalid_json_response_returns_none(self):
        """Test that invalid JSON in response returns None."""
        client = SummarizerClient(provider="docker", base_url="http://localhost:8100", model="test")
        
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "choices": [
                {
                    "message": {
                        "content": "Not valid JSON"
                    }
                }
            ]
        }
        
        with patch("requests.post", return_value=mock_response):
            result = client.summarize("Test transcript")
            
            assert result is None
    
    def test_empty_message_returns_none(self):
        """Test that empty message content returns None."""
        client = SummarizerClient(provider="docker", base_url="http://localhost:8100", model="test")
        
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "choices": [
                {
                    "message": {
                        "content": ""
                    }
                }
            ]
        }
        
        with patch("requests.post", return_value=mock_response):
            result = client.summarize("Test transcript")
            
            assert result is None


class TestBuildFromEnv:
    """Test suite for build_from_env() factory function."""
    
    def test_docker_from_env(self, monkeypatch):
        """Test building docker client from environment."""
        monkeypatch.setenv("SUMMARIZER_PROVIDER", "docker")
        monkeypatch.setenv("SUMMARIZER_BASE_URL", "http://localhost:8100")
        monkeypatch.setenv("SUMMARIZER_MODEL", "neural-chat")
        
        client = build_from_env()
        
        assert client.provider == "docker"
        assert client.base_url == "http://localhost:8100"
        assert client.model == "neural-chat"
    
    def test_lmstudio_from_env(self, monkeypatch):
        """Test building lmstudio client from environment."""
        monkeypatch.setenv("SUMMARIZER_PROVIDER", "lmstudio")
        monkeypatch.setenv("SUMMARIZER_BASE_URL", "http://localhost:1234")
        monkeypatch.setenv("SUMMARIZER_MODEL", "llama2")
        
        client = build_from_env()
        
        assert client.provider == "lmstudio"
    
    def test_openrouter_from_env(self, monkeypatch):
        """Test building openrouter client from environment."""
        monkeypatch.setenv("SUMMARIZER_PROVIDER", "openrouter")
        monkeypatch.setenv("SUMMARIZER_API_KEY", "test_key")
        monkeypatch.setenv("OPENROUTER_MODEL", "openai/gpt-4")
        
        client = build_from_env()
        
        assert client.provider == "openrouter"
        assert client.api_key == "test_key"
        assert client.model == "openai/gpt-4"
    
    def test_missing_provider_raises_error(self, monkeypatch):
        """Test that missing SUMMARIZER_PROVIDER raises error."""
        monkeypatch.delenv("SUMMARIZER_PROVIDER", raising=False)
        
        with pytest.raises(ValueError):
            build_from_env()
    
    def test_docker_missing_base_url_raises_error(self, monkeypatch):
        """Test that docker without SUMMARIZER_BASE_URL raises error."""
        monkeypatch.setenv("SUMMARIZER_PROVIDER", "docker")
        monkeypatch.delenv("SUMMARIZER_BASE_URL", raising=False)
        
        with pytest.raises(ValueError):
            build_from_env()
    
    def test_openrouter_missing_api_key_raises_error(self, monkeypatch):
        """Test that openrouter without API key raises error."""
        monkeypatch.setenv("SUMMARIZER_PROVIDER", "openrouter")
        monkeypatch.delenv("SUMMARIZER_API_KEY", raising=False)
        
        with pytest.raises(ValueError):
            build_from_env()
