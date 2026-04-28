import os
import logging
import base64
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime


logger = logging.getLogger(__name__)


def send_summary_email(gmail_service, recipient_email, meeting_subject, html_body):
    """
    Send summary email via Gmail API.
    
    MIME CONSTRUCTION:
      TYPE: MIMEMultipart("alternative")
      BODY: MIMEText(html_body, "html", "utf-8")
      HEADERS: To, From, Subject
      ENCODE: base64.urlsafe_b64encode → strip trailing =
      SEND: gmail_service.users().messages().send(userId="me", body={"raw": encoded}).execute()
    
    Subject line format: f"[{datetime.now().strftime('%H:%M')}] - {meeting_subject}"
    From: Gmail account authenticated user (always "me")
    To: recipient_email (from GMAIL_DESTINATION_ADDRESS)
    
    Args:
        gmail_service: Authenticated Gmail service object
        recipient_email (str): Email address to send to
        meeting_subject (str): Meeting subject for email subject line
        html_body (str): HTML content of email body
        
    Returns:
        dict: Gmail API response containing message ID
        
    Raises:
        ValueError: If required parameters missing or invalid
        Exception: On Gmail API failure
    """
    if not gmail_service:
        raise ValueError("Gmail service not initialized")
    
    if not recipient_email:
        raise ValueError("recipient_email required")
    
    if not meeting_subject:
        raise ValueError("meeting_subject required")
    
    if not html_body:
        raise ValueError("html_body required")
    
    # Build email subject with timestamp
    time_str = datetime.now().strftime("%H:%M")
    subject = f"[{time_str}] - {meeting_subject}"
    
    # Create message
    message = MIMEMultipart("alternative")
    message["To"] = recipient_email
    message["From"] = "me"
    message["Subject"] = subject
    
    # Add HTML body
    html_part = MIMEText(html_body, "html", "utf-8")
    message.attach(html_part)
    
    # Encode message
    raw_message = base64.urlsafe_b64encode(message.as_bytes()).decode("utf-8")
    # Strip trailing = padding (Gmail API requirement)
    raw_message = raw_message.rstrip("=")
    
    try:
        result = gmail_service.users().messages().send(
            userId="me",
            body={"raw": raw_message}
        ).execute()
        
        message_id = result.get("id", "unknown")
        logger.info(f"Email sent to {recipient_email}: {subject} (ID: {message_id})")
        return result
    
    except Exception as e:
        logger.exception(f"Failed to send email: {e}")
        raise


def send_email(gmail_service, to_address, subject, html_body):
    """
    Send a generic HTML email via Gmail API.
    
    Args:
        gmail_service: Authenticated Gmail service object
        to_address (str): Recipient email address
        subject (str): Email subject line
        html_body (str): HTML content for email body
        
    Returns:
        dict: Gmail API response
        
    Raises:
        ValueError: If parameters invalid
        Exception: On Gmail API failure
    """
    if not gmail_service:
        raise ValueError("Gmail service not initialized")
    
    if not to_address:
        raise ValueError("to_address required")
    
    if not subject:
        raise ValueError("subject required")
    
    if not html_body:
        raise ValueError("html_body required")
    
    # Create message
    message = MIMEMultipart("alternative")
    message["To"] = to_address
    message["From"] = "me"
    message["Subject"] = subject
    
    # Add HTML body
    html_part = MIMEText(html_body, "html", "utf-8")
    message.attach(html_part)
    
    # Encode message
    raw_message = base64.urlsafe_b64encode(message.as_bytes()).decode("utf-8")
    raw_message = raw_message.rstrip("=")
    
    try:
        result = gmail_service.users().messages().send(
            userId="me",
            body={"raw": raw_message}
        ).execute()
        
        message_id = result.get("id", "unknown")
        logger.info(f"Email sent to {to_address}: {subject} (ID: {message_id})")
        return result
    
    except Exception as e:
        logger.exception(f"Failed to send email: {e}")
        raise
