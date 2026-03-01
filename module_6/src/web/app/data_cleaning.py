"""Data cleaning helpers formerly provided by the legacy module_2 package."""

from __future__ import annotations

import json
import re
from typing import Any


def load_data(filename: str = "applicant_data.json") -> list[dict[str, Any]]:
    """Load applicant data from a JSON array file."""
    with open(filename, "r", encoding="utf-8") as file_handle:
        payload = json.load(file_handle)
    return payload if isinstance(payload, list) else []


def clean_data(data: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Normalize raw scraped records into a cleaner, display-safe structure."""
    cleaned: list[dict[str, Any]] = []

    for row in data:
        clean_row: dict[str, Any] = {}
        for key, value in row.items():
            if value is None:
                clean_value: Any = ""
            elif isinstance(value, str):
                clean_value = re.sub(r"<[^>]+>", "", value)
                clean_value = re.sub(r"\s+", " ", clean_value).strip()
                if clean_value.lower() in {"n/a", "na", "none", "-", "â€”"}:
                    clean_value = ""
            else:
                clean_value = value
            clean_row[key] = clean_value
        cleaned.append(clean_row)

    return cleaned


def save_data(data: list[dict[str, Any]], filename: str = "cleaned_applicant_data.json") -> None:
    """Persist cleaned applicant data to JSON."""
    with open(filename, "w", encoding="utf-8") as file_handle:
        json.dump(data, file_handle, indent=2, ensure_ascii=False)
