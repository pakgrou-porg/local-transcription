"""Summarizer LLM service client — dynamic endpoint, HTTP call, JSON parsing."""

import json
import logging

import requests

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = (
    "You are a meeting analyst. Extract structured information from the "
    "following meeting transcript. Return ONLY valid JSON with no markdown "
    "fencing and no additional text. The JSON must contain exactly these keys:\n"
    "  meeting_subject   : string — short descriptive title for the meeting\n"
    "  speakers          : array of strings — speaker names or identifiers found in transcript\n"
    "  action_items      : array of objects with keys:\n"
    "                        action      : string\n"
    "                        assigned_to : string\n"
    "  discussion_topics : array of strings — detailed topic descriptions\n"
    "  resourcing        : array of strings — any resource, budget, staffing, or tool mentions"
)


def _resolve_endpoint(base_url: str) -> str:
    """Resolve the chat completions endpoint from the configured base URL.

    Parameters
    ----------
    base_url : str
        The SUMMARIZER_BASE_URL from configuration.

    Returns
    -------
    str
        Full endpoint URL for chat completions.
    """
    base = base_url.rstrip("/")
    if base.endswith("/v1/chat/completions"):
        return base
    elif base.endswith("/v1"):
        return f"{base}/chat/completions"
    else:
        return f"{base}/v1/chat/completions"


def call_summarizer(
    transcript: str,
    base_url: str,
    model: str,
    api_key: str = "",
    timeout: int = 120,
) -> dict:
    """Send a transcript to the summarizer LLM and return the parsed summary.

    Parameters
    ----------
    transcript : str
        Full transcript text.
    base_url : str
        Base URL of the summarizer service.
    model : str
        Model name/identifier for the request body.
    api_key : str
        API key (omit Authorization header if empty).
    timeout : int
        HTTP timeout in seconds.

    Returns
    -------
    dict
        Parsed summary JSON with keys: meeting_subject, speakers,
        action_items, discussion_topics, resourcing.

    Raises
    ------
    SummarizerError
        On HTTP errors, timeouts, or JSON parsing failures.
    """
    endpoint = _resolve_endpoint(base_url)
    logger.info("Calling summarizer: %s (model=%s)", endpoint, model)

    headers = {"Content-Type": "application/json"}
    if api_key and api_key.strip():
        headers["Authorization"] = f"Bearer {api_key.strip()}"

    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": transcript},
        ],
        "response_format": {"type": "json_object"},
    }

    try:
        response = requests.post(
            endpoint,
            headers=headers,
            json=payload,
            timeout=timeout,
        )
    except requests.exceptions.Timeout:
        raise SummarizerError(f"Summarizer request timed out after {timeout}s")
    except requests.exceptions.ConnectionError as e:
        raise SummarizerError(f"Connection error to summarizer service: {e}")

    if response.status_code != 200:
        body_preview = response.text[:500] if response.text else "(empty)"
        raise SummarizerError(
            f"Summarizer HTTP {response.status_code}: {body_preview}"
        )

    # Parse the outer response
    try:
        result = response.json()
    except (ValueError, requests.exceptions.JSONDecodeError) as e:
        raise SummarizerError(
            f"Failed to parse summarizer response JSON: {e}. "
            f"Response: {response.text[:500]}"
        )

    # Extract content from choices
    try:
        content = result["choices"][0]["message"]["content"]
    except (KeyError, IndexError, TypeError) as e:
        raise SummarizerError(
            f"Unexpected summarizer response structure: {e}. "
            f"Response: {json.dumps(result)[:500]}"
        )

    # Parse the inner JSON content
    try:
        summary = json.loads(content)
    except json.JSONDecodeError as e:
        raise SummarizerError(
            f"Failed to parse summary content as JSON: {e}. "
            f"Content: {content[:500]}"
        )

    # Validate expected keys
    expected_keys = {
        "meeting_subject", "speakers", "action_items",
        "discussion_topics", "resourcing",
    }
    missing = expected_keys - set(summary.keys())
    if missing:
        logger.warning("Summary JSON missing keys: %s", missing)

    logger.info(
        "Summary received: subject='%s', %d speakers, %d action items, "
        "%d topics, %d resourcing items",
        summary.get("meeting_subject", "(unknown)"),
        len(summary.get("speakers", [])),
        len(summary.get("action_items", [])),
        len(summary.get("discussion_topics", [])),
        len(summary.get("resourcing", [])),
    )
    return summary


class SummarizerError(Exception):
    """Raised when summarization fails for any reason."""
    pass
