"""Gmail API email sender — MIME construction, base64 encoding, send."""

import base64
import logging
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

logger = logging.getLogger(__name__)


def send_summary_email(
    gmail_service,
    to_address: str,
    meeting_subject: str,
    html_body: str,
) -> str:
    """Send an HTML email summary via the Gmail API.

    Parameters
    ----------
    gmail_service : googleapiclient.discovery.Resource
        Authenticated Gmail v1 service object.
    to_address : str
        Recipient email address.
    meeting_subject : str
        The meeting subject extracted from the summary (used in email subject).
    html_body : str
        Full HTML email body string.

    Returns
    -------
    str
        The Gmail message ID of the sent email.

    Raises
    ------
    EmailSendError
        If the email fails to send.
    """
    time_stamp = datetime.now().strftime("%H:%M")
    email_subject = f"[{time_stamp}] - {meeting_subject}"

    logger.info(
        "Preparing email: subject='%s', to='%s'",
        email_subject, to_address,
    )

    # Build MIME message
    message = MIMEMultipart("alternative")
    message["To"] = to_address
    message["Subject"] = email_subject

    # Attach HTML body
    html_part = MIMEText(html_body, "html")
    message.attach(html_part)

    # Encode to base64url
    raw_bytes = message.as_bytes()
    encoded = base64.urlsafe_b64encode(raw_bytes).decode("utf-8")

    try:
        result = gmail_service.users().messages().send(
            userId="me",
            body={"raw": encoded},
        ).execute()

        message_id = result.get("id", "unknown")
        logger.info(
            "Email sent successfully: message_id=%s, subject='%s'",
            message_id, email_subject,
        )
        return message_id

    except Exception as e:
        raise EmailSendError(f"Failed to send email: {e}") from e


class EmailSendError(Exception):
    """Raised when email sending fails."""
    pass
