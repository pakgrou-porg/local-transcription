import os
import re
import json
import logging
from pathlib import Path


logger = logging.getLogger(__name__)


def load_substitutions(substitutions_file=None):
    """
    Load substitutions from file.
    
    Format: CanonicalName=regex_alt1|regex_alt2|regex_alt3
    - Skip: blank lines, lines starting with #
    - Right side is the pattern (matched case-insensitive)
    - Left side is the replacement (canonical form)
    
    Args:
        substitutions_file (str, Path, optional): Path to substitutions file.
                                                   If None, uses SUBSTITUTIONS_FILE from .env
        
    Returns:
        dict: Dict mapping canonical_name -> list of regex patterns
              e.g., {"Karl": ["Carl", "Carul", "Kharel"], ...}
              
    Raises:
        FileNotFoundError: If file not found
    """
    if substitutions_file is None:
        substitutions_file = os.getenv("SUBSTITUTIONS_FILE", "./substitutions.txt")
    
    substitutions_file = Path(substitutions_file)
    
    if not substitutions_file.exists():
        logger.error(f"Substitutions file not found: {substitutions_file}")
        raise FileNotFoundError(f"Substitutions file not found: {substitutions_file}")
    
    substitutions = {}
    
    try:
        with open(substitutions_file, "r", encoding="utf-8") as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                
                # Skip blank lines and comments
                if not line or line.startswith("#"):
                    continue
                
                # Parse line format: CanonicalName=alt1|alt2|alt3
                if "=" not in line:
                    logger.warning(f"Invalid substitution line {line_num}: missing '=' in '{line}'")
                    continue
                
                canonical, patterns_str = line.split("=", 1)
                canonical = canonical.strip()
                patterns_str = patterns_str.strip()
                
                if not canonical:
                    logger.warning(f"Invalid substitution line {line_num}: empty canonical name")
                    continue
                
                if not patterns_str:
                    logger.warning(f"Invalid substitution line {line_num}: empty pattern list")
                    continue
                
                # Split patterns by |
                patterns = [p.strip() for p in patterns_str.split("|") if p.strip()]
                
                if patterns:
                    substitutions[canonical] = patterns
        
        logger.info(f"Loaded {len(substitutions)} substitution rules from {substitutions_file}")
        return substitutions
    
    except Exception as e:
        logger.exception(f"Error loading substitutions file: {e}")
        raise


def apply_substitutions(text, substitutions):
    """
    Apply substitutions to text using regex.
    
    All patterns matched case-insensitive (re.IGNORECASE).
    
    Args:
        text (str): Text to process
        substitutions (dict): Dict mapping canonical_name -> list of patterns
        
    Returns:
        str: Text with substitutions applied
    """
    result = text
    
    for canonical, patterns in substitutions.items():
        for pattern in patterns:
            # Use re.IGNORECASE for case-insensitive matching
            result = re.sub(
                pattern,
                canonical,
                result,
                flags=re.IGNORECASE
            )
            logger.debug(f"Applied substitution: {pattern} -> {canonical}")
    
    return result


def apply_substitutions_to_summary(summary_dict, substitutions_file=None):
    """
    Apply substitutions to a summary dictionary.
    
    PROCESS:
    1. Serialize summary dict to JSON string
    2. Load substitutions from file
    3. Apply regex substitutions to JSON string
    4. Parse JSON back to dict (validates round-trip)
    5. Return parsed dict, or original dict if JSON invalid
    
    Args:
        summary_dict (dict): Summary dictionary to process
        substitutions_file (str, Path, optional): Path to substitutions file
        
    Returns:
        dict: Summary dict with substitutions applied
    """
    # Serialize to JSON
    try:
        json_string = json.dumps(summary_dict)
    except (TypeError, ValueError) as e:
        logger.error(f"Failed to serialize summary dict: {e}")
        return summary_dict
    
    # Load substitutions
    try:
        substitutions = load_substitutions(substitutions_file)
    except FileNotFoundError:
        logger.warning(f"Substitutions file not found, returning original summary")
        return summary_dict
    
    # Apply substitutions
    modified_json = apply_substitutions(json_string, substitutions)
    
    # Validate round-trip JSON parse
    try:
        parsed_dict = json.loads(modified_json)
        logger.info("Substitutions applied and JSON validated successfully")
        return parsed_dict
    
    except json.JSONDecodeError as e:
        logger.warning(f"JSON validation failed after substitutions: {e}. Returning original summary.")
        return summary_dict


def apply_substitutions_to_json_string(json_string, substitutions_file=None):
    """
    Apply substitutions to a JSON string.
    
    PROCESS:
    1. Load substitutions from file
    2. Apply regex substitutions to JSON string
    3. Round-trip parse and validate JSON
    4. Return original if invalid
    
    Args:
        json_string (str): JSON string to process
        substitutions_file (str, Path, optional): Path to substitutions file
        
    Returns:
        str: JSON string with substitutions applied (validated)
    """
    # Load substitutions
    try:
        substitutions = load_substitutions(substitutions_file)
    except FileNotFoundError:
        logger.warning(f"Substitutions file not found, returning original JSON")
        return json_string
    
    # Apply substitutions
    modified_json = apply_substitutions(json_string, substitutions)
    
    # Validate round-trip JSON parse
    try:
        json.loads(modified_json)
        logger.info("Substitutions applied to JSON string and validated")
        return modified_json
    
    except json.JSONDecodeError as e:
        logger.warning(f"JSON validation failed after substitutions: {e}. Returning original JSON.")
        return json_string
