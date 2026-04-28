import pytest
from render import render_summary_to_html


class TestHTMLRendering:
    """Test suite for HTML rendering from summary dict."""
    
    def test_renders_valid_html_structure(self):
        """
        Test that output is valid HTML document.
        
        Verifies:
        - Contains <html>, <head>, <body> tags
        - Full document structure
        """
        summary = {
            "meeting_subject": "Test Meeting",
            "speakers": ["John"],
            "action_items": [],
            "discussion_topics": [],
            "resourcing": []
        }
        
        html = render_summary_to_html(summary)
        
        assert html.startswith("<!DOCTYPE html>")
        assert "<html>" in html
        assert "<head>" in html
        assert "<body>" in html
        assert "</body>" in html
        assert "</html>" in html
    
    def test_meeting_subject_in_h2(self):
        """
        Test that meeting subject is in <h2> tag.
        
        Verifies:
        - H2 contains exact text from meeting_subject
        """
        summary = {
            "meeting_subject": "Q2 Strategic Planning",
            "speakers": [],
            "action_items": [],
            "discussion_topics": [],
            "resourcing": []
        }
        
        html = render_summary_to_html(summary)
        
        assert "<h2>" in html
        assert "Q2 Strategic Planning" in html
    
    def test_speakers_section_renders(self):
        """
        Test that speakers section renders correctly.
        
        Verifies:
        - H3 with "Speakers" text
        - Each speaker in <li>
        """
        summary = {
            "meeting_subject": "Test",
            "speakers": ["John Smith", "Sarah Johnson"],
            "action_items": [],
            "discussion_topics": [],
            "resourcing": []
        }
        
        html = render_summary_to_html(summary)
        
        assert "<h3>Speakers</h3>" in html
        assert "John Smith" in html
        assert "Sarah Johnson" in html
        assert html.count("<li>") >= 2
    
    def test_action_items_bold_assigned_to(self):
        """
        Test that action items have assigned_to in bold.
        
        Verifies:
        - <strong> tag wraps assigned_to
        - Format: <strong>assigned_to</strong>: action
        """
        summary = {
            "meeting_subject": "Test",
            "speakers": [],
            "action_items": [
                {"assigned_to": "John", "action": "Follow up on budget"}
            ],
            "discussion_topics": [],
            "resourcing": []
        }
        
        html = render_summary_to_html(summary)
        
        assert "<h3>Action Items</h3>" in html
        assert "<strong>John</strong>: Follow up on budget" in html
    
    def test_multiple_action_items(self):
        """Test that multiple action items render correctly."""
        summary = {
            "meeting_subject": "Test",
            "speakers": [],
            "action_items": [
                {"assigned_to": "John", "action": "Task 1"},
                {"assigned_to": "Sarah", "action": "Task 2"},
                {"assigned_to": "Mike", "action": "Task 3"}
            ],
            "discussion_topics": [],
            "resourcing": []
        }
        
        html = render_summary_to_html(summary)
        
        assert html.count("<strong>") == 3
        assert "<strong>John</strong>: Task 1" in html
        assert "<strong>Sarah</strong>: Task 2" in html
        assert "<strong>Mike</strong>: Task 3" in html
    
    def test_discussion_topics_section(self):
        """
        Test that discussion topics section renders.
        
        Verifies:
        - H3 with "Discussion Topics"
        - Each topic in <li>
        """
        summary = {
            "meeting_subject": "Test",
            "speakers": [],
            "action_items": [],
            "discussion_topics": ["Budget planning", "Team expansion", "Q3 roadmap"],
            "resourcing": []
        }
        
        html = render_summary_to_html(summary)
        
        assert "<h3>Discussion Topics</h3>" in html
        assert "Budget planning" in html
        assert "Team expansion" in html
        assert "Q3 roadmap" in html
    
    def test_resourcing_section(self):
        """
        Test that resourcing section renders.
        
        Verifies:
        - H3 with "Resourcing"
        - Each item in <li>
        """
        summary = {
            "meeting_subject": "Test",
            "speakers": [],
            "action_items": [],
            "discussion_topics": [],
            "resourcing": ["2 engineers", "Budget: $50K", "Consultant"]
        }
        
        html = render_summary_to_html(summary)
        
        assert "<h3>Resourcing</h3>" in html
        assert "2 engineers" in html
        assert "Budget: $50K" in html
        assert "Consultant" in html
    
    def test_empty_lists_render_without_error(self):
        """
        Test that empty lists don't cause errors or display sections.
        
        Verifies:
        - No sections rendered for empty lists
        - No spurious <h3> tags
        """
        summary = {
            "meeting_subject": "Test",
            "speakers": [],
            "action_items": [],
            "discussion_topics": [],
            "resourcing": []
        }
        
        html = render_summary_to_html(summary)
        
        # Should only have one h3 if any (the structure might vary)
        assert "<h3>Speakers</h3>" not in html
        assert "<h3>Action Items</h3>" not in html
        assert "<h3>Discussion Topics</h3>" not in html
        assert "<h3>Resourcing</h3>" not in html
    
    def test_partial_empty_sections(self):
        """
        Test that only populated sections render.
        
        Verifies:
        - Empty sections omitted
        - Populated sections appear
        """
        summary = {
            "meeting_subject": "Test",
            "speakers": ["John"],
            "action_items": [],
            "discussion_topics": ["Budget"],
            "resourcing": []
        }
        
        html = render_summary_to_html(summary)
        
        assert "<h3>Speakers</h3>" in html
        assert "<h3>Action Items</h3>" not in html
        assert "<h3>Discussion Topics</h3>" in html
        assert "<h3>Resourcing</h3>" not in html
    
    def test_inline_css_only(self):
        """
        Test that only inline CSS is used (no external stylesheets).
        
        Verifies:
        - No <link> tags
        - Styles in <style> tag
        - No external imports
        """
        summary = {
            "meeting_subject": "Test",
            "speakers": [],
            "action_items": [],
            "discussion_topics": [],
            "resourcing": []
        }
        
        html = render_summary_to_html(summary)
        
        assert "<link" not in html
        assert "@import" not in html
        assert "<style>" in html
        assert "</style>" in html
    
    def test_html_special_characters_escaped(self):
        """
        Test that HTML special characters are escaped.
        
        Verifies:
        - <, >, &, ", ' are escaped
        """
        summary = {
            "meeting_subject": "Test < > & \" '",
            "speakers": ["John & Jane"],
            "action_items": [
                {
                    "assigned_to": "Bob <Developer>",
                    "action": "Review code & tests"
                }
            ],
            "discussion_topics": [],
            "resourcing": []
        }
        
        html = render_summary_to_html(summary)
        
        # Special characters should be escaped
        assert "&lt;" in html or "Test < > &" not in html
        assert "&gt;" in html or "Test < > &" not in html
        assert "&amp;" in html or "John & Jane" not in html
    
    def test_missing_keys_handled_gracefully(self):
        """
        Test that missing keys don't cause errors.
        
        Verifies:
        - Works with empty dict
        - Works with partial dict
        """
        # Empty dict
        summary = {}
        html = render_summary_to_html(summary)
        assert html is not None
        assert len(html) > 0
        
        # Partial dict
        summary = {
            "meeting_subject": "Test",
            "speakers": ["John"]
        }
        html = render_summary_to_html(summary)
        assert "Test" in html
    
    def test_default_title_for_missing_subject(self):
        """
        Test that default title is used when meeting_subject missing.
        
        Verifies:
        - "Meeting Summary" used as fallback
        """
        summary = {
            "speakers": [],
            "action_items": [],
            "discussion_topics": [],
            "resourcing": []
        }
        
        html = render_summary_to_html(summary)
        
        assert "Meeting Summary" in html
    
    def test_max_width_constraint(self):
        """
        Test that CSS includes max-width constraint for mobile Gmail.
        
        Verifies:
        - max-width: 700px in CSS
        """
        summary = {
            "meeting_subject": "Test",
            "speakers": [],
            "action_items": [],
            "discussion_topics": [],
            "resourcing": []
        }
        
        html = render_summary_to_html(summary)
        
        assert "max-width: 700px" in html
    
    def test_background_color_set(self):
        """
        Test that background colors are configured.
        
        Verifies:
        - #f9f9f9 background
        - #ffffff container background
        """
        summary = {
            "meeting_subject": "Test",
            "speakers": [],
            "action_items": [],
            "discussion_topics": [],
            "resourcing": []
        }
        
        html = render_summary_to_html(summary)
        
        assert "#f9f9f9" in html
        assert "#ffffff" in html
    
    def test_font_family_arial_sans_serif(self):
        """
        Test that font family is Arial, sans-serif (Gmail-safe).
        
        Verifies:
        - Arial is specified
        - has fallback to sans-serif
        """
        summary = {
            "meeting_subject": "Test",
            "speakers": [],
            "action_items": [],
            "discussion_topics": [],
            "resourcing": []
        }
        
        html = render_summary_to_html(summary)
        
        assert "Arial" in html
        assert "sans-serif" in html
