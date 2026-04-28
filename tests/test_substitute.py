import pytest
import json
import tempfile
from pathlib import Path
from unittest.mock import patch

from substitute import (
    load_substitutions,
    apply_substitutions,
    apply_substitutions_to_summary,
    apply_substitutions_to_json_string
)


class TestLoadSubstitutions:
    """Test suite for loading substitution rules from file."""
    
    def test_load_substitutions_from_file(self, tmp_path):
        """
        Test loading substitutions from a valid file.
        
        Verifies:
        - File parsing works correctly
        - Rules are extracted correctly
        """
        sub_file = tmp_path / "substitutions.txt"
        sub_file.write_text(
            "Karl=Carl|Carul|Kharel\n"
            "Supabase=Supa Base|Supa-Base|SupaBase\n"
        )
        
        result = load_substitutions(str(sub_file))
        
        assert "Karl" in result
        assert "Supabase" in result
        assert result["Karl"] == ["Carl", "Carul", "Kharel"]
        assert result["Supabase"] == ["Supa Base", "Supa-Base", "SupaBase"]
    
    def test_load_substitutions_skips_comments_and_blanks(self, tmp_path):
        """
        Test that comments and blank lines are skipped.
        
        Verifies:
        - Line starting with # ignored
        - Blank lines ignored
        """
        sub_file = tmp_path / "substitutions.txt"
        sub_file.write_text(
            "# This is a comment\n"
            "\n"
            "Karl=Carl|Carul\n"
            "# Another comment\n"
            "\n"
            "Oracle=Oricle|Oracal\n"
        )
        
        result = load_substitutions(str(sub_file))
        
        assert len(result) == 2
        assert "Karl" in result
        assert "Oracle" in result
    
    def test_load_substitutions_invalid_line_skipped(self, tmp_path, caplog):
        """
        Test that invalid lines are skipped with warning.
        
        Verifies:
        - Missing = separator handled
        - Warning logged
        """
        sub_file = tmp_path / "substitutions.txt"
        sub_file.write_text(
            "Karl=Carl\n"
            "InvalidLineNoEquals\n"
            "Oracle=Oricle\n"
        )
        
        result = load_substitutions(str(sub_file))
        
        assert len(result) == 2
        assert "Karl" in result
        assert "Oracle" in result
        assert any("invalid" in record.message.lower() for record in caplog.records)
    
    def test_load_substitutions_empty_canonical_name_skipped(self, tmp_path, caplog):
        """
        Test that lines with empty canonical name are skipped.
        
        Verifies:
        - Validation of canonical name
        """
        sub_file = tmp_path / "substitutions.txt"
        sub_file.write_text(
            "=Carl|Carul\n"
            "Karl=Carl\n"
        )
        
        result = load_substitutions(str(sub_file))
        
        assert len(result) == 1
        assert "Karl" in result
    
    def test_load_substitutions_empty_patterns_skipped(self, tmp_path, caplog):
        """
        Test that lines with empty pattern list are skipped.
        
        Verifies:
        - Validation of pattern list
        """
        sub_file = tmp_path / "substitutions.txt"
        sub_file.write_text(
            "Karl=\n"
            "Oracle=Oricle\n"
        )
        
        result = load_substitutions(str(sub_file))
        
        assert len(result) == 1
        assert "Oracle" in result
    
    def test_load_substitutions_file_not_found_raises_error(self):
        """
        Test that missing file raises FileNotFoundError.
        
        Verifies:
        - File existence check
        """
        with pytest.raises(FileNotFoundError):
            load_substitutions("/nonexistent/path/substitutions.txt")
    
    def test_load_substitutions_from_env(self, tmp_path, monkeypatch):
        """
        Test loading from file path in SUBSTITUTIONS_FILE env var.
        
        Verifies:
        - Environment variable used when no argument provided
        """
        sub_file = tmp_path / "substitutions.txt"
        sub_file.write_text("Karl=Carl\n")
        
        monkeypatch.setenv("SUBSTITUTIONS_FILE", str(sub_file))
        
        result = load_substitutions()
        
        assert "Karl" in result


class TestApplySubstitutions:
    """Test suite for applying regex substitutions to text."""
    
    def test_substitution_replaces_right_side_with_left(self):
        """
        Test basic substitution: right side matched, left side replaces.
        
        Verifies:
        - Pattern matching and replacement
        """
        substitutions = {
            "Karl": ["Carl", "Carul"],
            "Oracle": ["Oricle"]
        }
        
        text = "Carl mentioned Oricle database in the meeting."
        
        result = apply_substitutions(text, substitutions)
        
        assert "Karl" in result
        assert "Oracle" in result
        assert "Carl" not in result
        assert "Oricle" not in result
    
    def test_substitution_is_case_insensitive(self):
        """
        Test that substitution matching is case-insensitive.
        
        Verifies:
        - re.IGNORECASE flag applied
        """
        substitutions = {
            "Karl": ["carl", "CARL", "CaRl"]
        }
        
        text = "Carl, CARL, and CaRl all followed up on the action items."
        
        result = apply_substitutions(text, substitutions)
        
        assert result.count("Karl") == 3
        assert "Carl" not in result or result.count("Carl") == 0
    
    def test_substitution_multiple_patterns(self):
        """
        Test applying multiple patterns for same canonical name.
        
        Verifies:
        - All patterns replaced with canonical
        """
        substitutions = {
            "Supabase": ["Supa Base", "Supa-Base", "SupaBase"]
        }
        
        text = "Using Supa Base, Supa-Base, and SupaBase in our architecture."
        
        result = apply_substitutions(text, substitutions)
        
        assert result.count("Supabase") == 3
        assert "Supa Base" not in result
        assert "Supa-Base" not in result
        assert "SupaBase" not in result
    
    def test_substitution_empty_substitutions(self):
        """
        Test applying empty substitutions dict returns original text.
        
        Verifies:
        - No-op case handled
        """
        substitutions = {}
        text = "Some meeting transcript with no substitutions."
        
        result = apply_substitutions(text, substitutions)
        
        assert result == text


class TestApplySubstitutionsToSummary:
    """Test suite for applying substitutions to summary dictionaries."""
    
    def test_substitutions_to_summary_valid_dict(self, tmp_path):
        """
        Test applying substitutions to a valid summary dict.
        
        Verifies:
        - Serialization to JSON
        - Substitution applied
        - JSON parsed back
        """
        sub_file = tmp_path / "substitutions.txt"
        sub_file.write_text("Karl=Carl\n")
        
        summary = {
            "meeting_subject": "Q2 Planning",
            "speakers": ["Carl", "Sarah"],
            "action_items": [{"assigned_to": "Carl", "action": "Follow up"}],
            "discussion_topics": ["Strategy", "Budget"],
            "resourcing": ["Team A"]
        }
        
        result = apply_substitutions_to_summary(summary, str(sub_file))
        
        assert result["speakers"] == ["Karl", "Sarah"]
        assert result["action_items"][0]["assigned_to"] == "Karl"
    
    def test_substitutions_to_summary_preserves_structure(self, tmp_path):
        """
        Test that summary structure is preserved after substitution.
        
        Verifies:
        - All required keys present
        - Schema maintained
        """
        sub_file = tmp_path / "substitutions.txt"
        sub_file.write_text("Oracle=Oricle\n")
        
        summary = {
            "meeting_subject": "Oricle Migration",
            "speakers": ["John"],
            "action_items": [{"assigned_to": "John", "action": "Plan Oricle setup"}],
            "discussion_topics": ["Technical"],
            "resourcing": ["Database team"]
        }
        
        result = apply_substitutions_to_summary(summary, str(sub_file))
        
        assert "meeting_subject" in result
        assert "speakers" in result
        assert "action_items" in result
        assert "discussion_topics" in result
        assert "resourcing" in result
    
    def test_substitutions_to_summary_missing_file_returns_original(self, caplog):
        """
        Test that missing substitutions file returns original dict.
        
        Verifies:
        - Graceful degradation
        """
        summary = {
            "meeting_subject": "Test",
            "speakers": ["Carl"],
            "action_items": [],
            "discussion_topics": [],
            "resourcing": []
        }
        
        result = apply_substitutions_to_summary(summary, "/nonexistent/path/substitutions.txt")
        
        assert result == summary
    
    def test_substitutions_to_summary_invalid_json_returns_original(self, tmp_path):
        """
        Test that invalid JSON after substitution returns original dict.
        
        Verifies:
        - Invalid JSON detection
        - Fallback to original
        """
        sub_file = tmp_path / "substitutions.txt"
        # Create a substitution that will break JSON
        sub_file.write_text('meeting_subject="}corrupted"}\n')
        
        summary = {
            "meeting_subject": "Test Meeting",
            "speakers": ["John"],
            "action_items": [],
            "discussion_topics": [],
            "resourcing": []
        }
        
        result = apply_substitutions_to_summary(summary, str(sub_file))
        
        # Should return original if JSON becomes invalid
        assert result == summary


class TestApplySubstitutionsToJsonString:
    """Test suite for applying substitutions to JSON strings."""
    
    def test_substitutions_to_json_string_valid(self, tmp_path):
        """
        Test applying substitutions to a JSON string.
        
        Verifies:
        - JSON string processed
        - Substitutions applied
        - Valid JSON returned
        """
        sub_file = tmp_path / "substitutions.txt"
        sub_file.write_text("Karl=Carl\n")
        
        json_string = json.dumps({
            "meeting_subject": "Planning",
            "speakers": ["Carl"],
            "action_items": [],
            "discussion_topics": [],
            "resourcing": []
        })
        
        result = apply_substitutions_to_json_string(json_string, str(sub_file))
        
        # Result should be valid JSON with substitutions applied
        parsed = json.loads(result)
        assert parsed["speakers"] == ["Karl"]
    
    def test_substitutions_to_json_string_invalid_returns_original(self, tmp_path):
        """
        Test that invalid JSON after substitution returns original.
        
        Verifies:
        - Fallback to original on JSON error
        """
        sub_file = tmp_path / "substitutions.txt"
        sub_file.write_text('meeting_subject=}\n')
        
        json_string = json.dumps({"meeting_subject": "Test"})
        
        result = apply_substitutions_to_json_string(json_string, str(sub_file))
        
        # Should return original string if JSON becomes invalid
        assert json.loads(result) == json.loads(json_string)
    
    def test_substitutions_to_json_string_missing_file(self):
        """
        Test that missing file returns original JSON string.
        
        Verifies:
        - Graceful degradation
        """
        json_string = json.dumps({"meeting_subject": "Test"})
        
        result = apply_substitutions_to_json_string(
            json_string,
            "/nonexistent/path/substitutions.txt"
        )
        
        assert result == json_string


class TestIntegration:
    """Integration tests for substitution workflow."""
    
    def test_full_substitution_workflow(self, tmp_path):
        """
        Test complete workflow: dict -> JSON -> substitution -> dict.
        
        Verifies:
        - End-to-end process
        - Schema maintained
        - Substitutions applied
        """
        sub_file = tmp_path / "substitutions.txt"
        sub_file.write_text(
            "Karl=Carl|Carul\n"
            "Supabase=Supa Base\n"
        )
        
        original_summary = {
            "meeting_subject": "Supa Base Architecture",
            "speakers": ["Carl", "Sarah"],
            "action_items": [
                {"assigned_to": "Carl", "action": "Setup Supa Base"},
                {"assigned_to": "Carul", "action": "Test Supa Base"}
            ],
            "discussion_topics": ["Supa Base scalability", "Performance"],
            "resourcing": ["Database team"]
        }
        
        result = apply_substitutions_to_summary(original_summary, str(sub_file))
        
        # Verify all substitutions applied
        assert result["meeting_subject"] == "Supabase Architecture"
        assert result["speakers"] == ["Karl", "Sarah"]
        assert result["action_items"][0]["assigned_to"] == "Karl"
        assert result["action_items"][1]["assigned_to"] == "Karl"
        assert result["discussion_topics"][0] == "Supabase scalability"
