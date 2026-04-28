import logging


logger = logging.getLogger(__name__)


def render_summary_to_html(summary_dict):
    """
    Render a summary dictionary to an HTML email body.
    
    STRUCTURE: full <html><head><body> document
    CSS: inline only, Gmail-safe, no external stylesheets
    FONT: Arial, sans-serif
    BACKGROUND: #f9f9f9
    WIDTH: max-width 700px
    
    SECTIONS (in order):
      <h2> meeting_subject
      <h3> Speakers       → <ul><li> per speaker
      <h3> Action Items   → <ul><li><strong>assigned_to</strong>: action</li>
      <h3> Discussion Topics → <ul><li> per topic
      <h3> Resourcing     → <ul><li> per item
    
    Args:
        summary_dict (dict): Summary with keys: meeting_subject, speakers,
                            action_items, discussion_topics, resourcing
        
    Returns:
        str: Full HTML document as string
    """
    # Extract data with defaults
    meeting_subject = summary_dict.get("meeting_subject", "Meeting Summary")
    speakers = summary_dict.get("speakers", [])
    action_items = summary_dict.get("action_items", [])
    discussion_topics = summary_dict.get("discussion_topics", [])
    resourcing = summary_dict.get("resourcing", [])
    
    # Escape HTML special characters
    def escape_html(text):
        if not isinstance(text, str):
            text = str(text)
        return (text
                .replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace('"', "&quot;")
                .replace("'", "&#39;"))
    
    # Build HTML
    html_parts = [
        "<!DOCTYPE html>",
        "<html>",
        "<head>",
        '<meta charset="UTF-8">',
        '<meta name="viewport" content="width=device-width, initial-scale=1.0">',
        "<style>",
        "body { font-family: Arial, sans-serif; background-color: #f9f9f9; }",
        ".container { max-width: 700px; margin: 0 auto; padding: 20px; background-color: #ffffff; border-radius: 5px; }",
        "h2 { color: #333333; border-bottom: 2px solid #007bff; padding-bottom: 10px; }",
        "h3 { color: #555555; margin-top: 20px; }",
        "ul { list-style-type: disc; margin: 10px 0; padding-left: 20px; }",
        "li { margin: 8px 0; line-height: 1.6; }",
        "strong { color: #333333; }",
        "</style>",
        "</head>",
        "<body>",
        '<div class="container">',
    ]
    
    # Meeting subject
    html_parts.append(f"<h2>{escape_html(meeting_subject)}</h2>")
    
    # Speakers
    if speakers:
        html_parts.append("<h3>Speakers</h3>")
        html_parts.append("<ul>")
        for speaker in speakers:
            html_parts.append(f"<li>{escape_html(speaker)}</li>")
        html_parts.append("</ul>")
    
    # Action Items
    if action_items:
        html_parts.append("<h3>Action Items</h3>")
        html_parts.append("<ul>")
        for item in action_items:
            assigned_to = escape_html(item.get("assigned_to", "Unassigned"))
            action = escape_html(item.get("action", ""))
            html_parts.append(f"<li><strong>{assigned_to}</strong>: {action}</li>")
        html_parts.append("</ul>")
    
    # Discussion Topics
    if discussion_topics:
        html_parts.append("<h3>Discussion Topics</h3>")
        html_parts.append("<ul>")
        for topic in discussion_topics:
            html_parts.append(f"<li>{escape_html(topic)}</li>")
        html_parts.append("</ul>")
    
    # Resourcing
    if resourcing:
        html_parts.append("<h3>Resourcing</h3>")
        html_parts.append("<ul>")
        for item in resourcing:
            html_parts.append(f"<li>{escape_html(item)}</li>")
        html_parts.append("</ul>")
    
    # Close HTML
    html_parts.extend([
        "</div>",
        "</body>",
        "</html>",
    ])
    
    html = "\n".join(html_parts)
    logger.info(f"Rendered HTML for meeting: {meeting_subject}")
    return html
