"""Text substitution — load patterns from file and apply to summary JSON."""

import json
import logging
import os
import re
from typing import Any

logger = logging.getLogger(__name__)


def load_substitutions(file_path: str) -> list[tuple[str, str]]:
    """Load substitution rules from a text file.

    Format per line: CanonicalName=regex_alt1|regex_alt2|regex_alt3
    Blank lines and lines starting with # are skipped.

    Parameters
    ----------
    file_path : str
        Path to the substitutions file.

    Returns
    -------
    list[tuple[str, str]]
        List of (canonical_name, regex_pattern) tuples.
    """
    if not os.path.exists(file_path):
        logger.warning("Substitutions file not found: %s", file_path)
        return []

    substitutions = []
    with open(file_path, "r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, start=1):
            line = line.strip()
            if not line or line.startswith("#"):
                continue

            if "=" not in line:
                logger.warning(
                    "Skipping malformed substitution line %d: %s",
                    line_num, line,
                )
                continue

            canonical, pattern = line.split("=", 1)
            canonical = canonical.strip()
            pattern = pattern.strip()

            if not canonical or not pattern:
                logger.warning(
                    "Skipping empty substitution at line %d: %s",
                    line_num, line,
                )
                continue

            # Validate the regex pattern
            try:
                re.compile(pattern)
            except re.error as e:
                logger.warning(
                    "Skipping invalid regex at line %d: %s (error: %s)",
                    line_num, pattern, e,
                )
                continue

            substitutions.append((canonical, pattern))

    logger.info("Loaded %d substitution rule(s) from %s", len(substitutions), file_path)
    return substitutions


def apply_substitutions(
    summary_dict: dict[str, Any],
    substitutions: list[tuple[str, str]],
) -> dict[str, Any]:
    """Apply text substitutions to a summary dict via JSON round-trip.

    The summary dict is serialized to a JSON string, regex substitutions are
    applied, and the result is re-parsed. If re-parsing fails, the original
    dict is returned unchanged.

    Parameters
    ----------
    summary_dict : dict
        Parsed summary dictionary.
    substitutions : list[tuple[str, str]]
        List of (canonical_name, regex_pattern) from load_substitutions().

    Returns
    -------
    dict
        The substitution-cleaned summary dictionary.
    """
    if not substitutions:
        logger.info("No substitutions to apply")
        return summary_dict

    json_string = json.dumps(summary_dict, ensure_ascii=False)
    original_json = json_string
    replacements_made = 0

    for canonical, pattern in substitutions:
        new_string = re.sub(pattern, canonical, json_string, flags=re.IGNORECASE)
        if new_string != json_string:
            count = len(re.findall(pattern, json_string, flags=re.IGNORECASE))
            replacements_made += count
            logger.info(
                "Substitution: '%s' -> '%s' (%d match(es))",
                pattern, canonical, count,
            )
            json_string = new_string

    if replacements_made == 0:
        logger.info("No substitution matches found")
        return summary_dict

    logger.info("Total substitutions applied: %d", replacements_made)

    # Validate the modified JSON is still parseable
    try:
        result = json.loads(json_string)
        logger.info("Post-substitution JSON validation passed")
        return result
    except json.JSONDecodeError as e:
        logger.warning(
            "Post-substitution JSON is invalid (%s). "
            "Reverting to pre-substitution data.",
            e,
        )
        return summary_dict
