import os
import json
import logging
import requests


logger = logging.getLogger(__name__)


def extract_model_ids(payload):
    """Extract model IDs from common OpenAI-compatible /v1/models responses."""
    if isinstance(payload, dict):
        models = payload.get("data", [])
    elif isinstance(payload, list):
        models = payload
    else:
        return []

    model_ids = []
    for model in models:
        if isinstance(model, dict):
            model_id = model.get("id") or model.get("name")
        else:
            model_id = str(model)
        if model_id:
            model_ids.append(model_id)
    return model_ids


def looks_like_model_not_found(status_code, body_text, exception_text=""):
    """Return True when a service response indicates the requested model is unavailable."""
    text = f"{body_text or ''} {exception_text or ''}".lower()
    if status_code not in {400, 404, 422}:
        return False
    return "model" in text and any(
        marker in text
        for marker in [
            "not found",
            "does not exist",
            "not exist",
            "not served",
            "not available",
            "unknown model",
        ]
    )


class SummarizerClient:
    """
    Client for interacting with summarization LLM services.
    
    Supports three providers:
    - docker: local Ollama/docker container with libcudart
    - lmstudio: local LM Studio application
    - openrouter: OpenRouter API (https://openrouter.ai)
    
    All providers must support OpenAI-compatible /v1/chat/completions endpoint.
    """
    
    def __init__(self, provider, base_url=None, api_key=None, model=None):
        """
        Initialize summarizer client.
        
        Args:
            provider (str): One of 'docker', 'lmstudio', 'openrouter'
            base_url (str, optional): For docker/lmstudio: http://host:port
                                      For openrouter: ignored (always https://openrouter.ai/api)
            api_key (str, optional): Bearer token for openrouter only
            model (str, optional): Model name to use
        """
        self.provider = provider.lower()
        self.base_url = base_url.rstrip("/") if base_url else None
        self.api_key = api_key
        self.model = model
        self.last_error = None
        
        if self.provider not in ["docker", "lmstudio", "openrouter"]:
            raise ValueError(f"Unknown provider: {provider}. Must be docker, lmstudio, or openrouter.")
        
        logger.info(f"Initialized SummarizerClient: provider={self.provider}, model={self.model}")

    def _record_error(self, message):
        """Save and log the latest summarization failure reason."""
        self.last_error = message
        logger.error(message)

    def _get_models_endpoint(self):
        """Return the OpenAI-compatible models endpoint for this provider."""
        if self.provider == "openrouter":
            return "https://openrouter.ai/api/v1/models"

        if not self.base_url:
            raise ValueError(f"{self.provider} provider requires base_url")

        if self.base_url.endswith("/v1/chat/completions"):
            return self.base_url[: -len("/chat/completions")] + "/models"
        if self.base_url.endswith("/v1"):
            return f"{self.base_url}/models"
        return f"{self.base_url}/v1/models"

    def list_available_models(self, timeout=30):
        """List available model IDs from the configured provider."""
        endpoint = self._get_models_endpoint()
        headers = self._build_headers()
        try:
            response = requests.get(endpoint, headers=headers, timeout=timeout)
            response.raise_for_status()
            model_ids = extract_model_ids(response.json())
        except Exception as e:
            self._record_error(f"Unable to list summarizer models from {endpoint}: {e}")
            return []

        if not model_ids:
            self._record_error(f"No summarizer models returned from {endpoint}")
            return []

        logger.info(f"Available summarizer models: {model_ids}")
        return model_ids

    def _fallback_models(self, timeout):
        """Return candidate fallback models excluding the currently failing model."""
        return [model for model in self.list_available_models(timeout=timeout) if model != self.model]
    
    def _get_endpoint(self):
        """
        Get the chat completions endpoint for this provider.
        
        DYNAMIC ENDPOINT DETECTION (docker + lmstudio only):
          base = SUMMARIZER_BASE_URL.rstrip("/")
          if   base.endswith("/v1/chat/completions") → use as-is
          elif base.endswith("/v1")                  → append "/chat/completions"
          else                                       → append "/v1/chat/completions"
        
        Returns:
            str: Full URL to /v1/chat/completions endpoint
        """
        if self.provider == "openrouter":
            return "https://openrouter.ai/api/v1/chat/completions"
        
        # docker or lmstudio
        if not self.base_url:
            raise ValueError(f"{self.provider} provider requires base_url")
        
        # Dynamic endpoint detection
        if self.base_url.endswith("/v1/chat/completions"):
            return self.base_url
        elif self.base_url.endswith("/v1"):
            return f"{self.base_url}/chat/completions"
        else:
            return f"{self.base_url}/v1/chat/completions"
    
    def _build_request_body(self, transcript):
        """
        Build the chat completions request body.
        
        All providers: include response_format: {"type": "json_object"}
        Docker: no stream field
        LMStudio: include "stream": false
        OpenRouter: include "stream": false
        
        Args:
            transcript (str): Transcript text to summarize
            
        Returns:
            dict: Request body for POST
        """
        body = {
            "model": self.model,
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "You are an expert meeting summarizer. "
                        "Analyze the provided meeting transcript and generate a structured summary in JSON format. "
                        "Return ONLY valid JSON with no markdown or code fences."
                    )
                },
                {
                    "role": "user",
                    "content": (
                        f"Please summarize this meeting transcript and return JSON with these exact keys:\n"
                        f"meeting_subject (string), speakers (array of strings), "
                        f"action_items (array of objects with 'assigned_to' and 'action' fields), "
                        f"discussion_topics (array of strings), resourcing (array of strings).\n\n"
                        f"Transcript:\n{transcript}"
                    )
                }
            ],
            "response_format": {"type": "json_object"}
        }
        
        # Conditional fields based on provider
        if self.provider in ["lmstudio", "openrouter"]:
            body["stream"] = False
        
        return body
    
    def _build_headers(self):
        """
        Build HTTP headers for request.
        
        Docker: no auth header
        LMStudio: no auth header
        OpenRouter: Authorization: Bearer {api_key}
        
        Returns:
            dict: Headers for request
        """
        headers = {
            "Content-Type": "application/json",
        }
        
        if self.provider == "openrouter":
            if not self.api_key:
                raise ValueError("OpenRouter requires SUMMARIZER_API_KEY")
            headers["Authorization"] = f"Bearer {self.api_key}"
        
        return headers
    
    def summarize(self, transcript, timeout=120):
        """
        Summarize a transcript using the configured provider.
        
        Args:
            transcript (str): Meeting transcript text
            timeout (int): Request timeout in seconds (default: 120)
            
        Returns:
            dict: Parsed summary with keys: meeting_subject, speakers, action_items,
                  discussion_topics, resourcing
                  or None if request failed
        """
        endpoint = self._get_endpoint()
        headers = self._build_headers()
        body = self._build_request_body(transcript)
        self.last_error = None
        
        logger.info(f"Sending summarization request to {self.provider} ({endpoint})")
        
        try:
            response = requests.post(
                endpoint,
                json=body,
                headers=headers,
                timeout=timeout
            )
            response.raise_for_status()
            
        except requests.Timeout as e:
            self._record_error(
                f"Summarization timed out after {timeout}s calling {endpoint}: {e}"
            )
            return None
        
        except requests.RequestException as e:
            response = getattr(e, "response", None)
            if response is not None:
                status = getattr(response, "status_code", "unknown")
                body_text = getattr(response, "text", "") or ""
                if looks_like_model_not_found(status, body_text, str(e)):
                    logger.warning(
                        "Summarizer model %s was not found; attempting model discovery",
                        self.model,
                    )
                    for fallback_model in self._fallback_models(timeout=min(timeout, 30)):
                        logger.info("Retrying summarization with discovered model: %s", fallback_model)
                        self.model = fallback_model
                        body = self._build_request_body(transcript)
                        try:
                            response = requests.post(
                                endpoint,
                                json=body,
                                headers=headers,
                                timeout=timeout,
                            )
                            response.raise_for_status()
                            break
                        except requests.RequestException as fallback_error:
                            self._record_error(
                                "Summarization fallback model failed: "
                                f"model={fallback_model}, endpoint={endpoint}, exception={fallback_error}"
                            )
                    else:
                        return None
                else:
                    self._record_error(
                        "Summarization service error: "
                        f"provider={self.provider}, endpoint={endpoint}, "
                        f"status={status}, body={body_text[:500]}, exception={e}"
                    )
                    return None
            else:
                self._record_error(
                    "Summarization service error: "
                    f"provider={self.provider}, endpoint={endpoint}, exception={e}"
                )
                return None
        
        # Parse response
        try:
            result = response.json()
            
            # Extract message content (OpenAI-compatible format)
            if "choices" not in result or not result["choices"]:
                self._record_error(
                    "Invalid summarization response: missing choices array"
                )
                return None
            
            message = result["choices"][0].get("message", {}).get("content", "")
            
            if not message:
                self._record_error("Invalid summarization response: empty message content")
                return None
            
            # Parse JSON from message content
            try:
                summary_dict = json.loads(message)
                logger.info("Summarization successful")
                return summary_dict
            
            except json.JSONDecodeError as e:
                self._record_error(
                    f"Failed to parse summary JSON: {e}; "
                    f"content_preview={message[:500]}"
                )
                logger.debug(f"Message content: {message[:200]}")
                return None
        
        except Exception as e:
            self.last_error = f"Unexpected error during summarization: {e}"
            logger.exception(self.last_error)
            return None


def build_from_env():
    """
    Create a SummarizerClient from environment variables.
    
    Environment variables required:
    - SUMMARIZER_PROVIDER: docker | lmstudio | openrouter
    - SUMMARIZER_BASE_URL: for docker/lmstudio (host:port format)
    - SUMMARIZER_API_KEY: for openrouter only
    - SUMMARIZER_MODEL: model name for docker/lmstudio
    - OPENROUTER_MODEL: routing string for openrouter only
    - SUMMARIZER_TIMEOUT_SECONDS: optional, default 120
    
    Returns:
        SummarizerClient: Configured client instance
        
    Raises:
        ValueError: If required environment variables are missing or invalid
    """
    provider = os.getenv("SUMMARIZER_PROVIDER")
    if not provider:
        raise ValueError("SUMMARIZER_PROVIDER not set in .env")
    
    provider = provider.lower()
    
    if provider in ["docker", "lmstudio"]:
        base_url = os.getenv("SUMMARIZER_BASE_URL")
        if not base_url:
            raise ValueError(f"{provider} provider requires SUMMARIZER_BASE_URL")
        
        model = os.getenv("SUMMARIZER_MODEL")
        if not model:
            raise ValueError(f"{provider} provider requires SUMMARIZER_MODEL")
        
        return SummarizerClient(
            provider=provider,
            base_url=base_url,
            model=model
        )
    
    elif provider == "openrouter":
        api_key = os.getenv("SUMMARIZER_API_KEY")
        if not api_key:
            raise ValueError("openrouter provider requires SUMMARIZER_API_KEY")
        
        model = os.getenv("OPENROUTER_MODEL")
        if not model:
            raise ValueError("openrouter provider requires OPENROUTER_MODEL")
        
        return SummarizerClient(
            provider="openrouter",
            api_key=api_key,
            model=model
        )
    
    else:
        raise ValueError(f"Unknown SUMMARIZER_PROVIDER: {provider}")
