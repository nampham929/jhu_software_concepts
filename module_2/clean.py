# clean.py
import json
import re

# -------------------------
# Required public functions
# -------------------------

def load_data(filename: str = "applicant_data.json") -> list:
    """Load scraped applicant data from JSON."""
    with open(filename, "r", encoding="utf-8") as f:
        return json.load(f)


def clean_data(data: list) -> list:
    """
    Perform lightweight, deterministic cleaning.
    DO NOT split program + university.
    DO NOT use LLMs here.
    """
    cleaned = []

    for row in data:
        clean_row = {}

        for key, value in row.items():
            if value is None:
                clean_value = ""
            elif isinstance(value, str):
                # Remove HTML remnants
                clean_value = re.sub(r"<[^>]+>", "", value)

                # Normalize whitespace
                clean_value = re.sub(r"\s+", " ", clean_value).strip()

                # Normalize common junk placeholders
                if clean_value.lower() in {"n/a", "na", "none", "-", "—"}:
                    clean_value = ""
            else:
                clean_value = value

            clean_row[key] = clean_value

        cleaned.append(clean_row)

    return cleaned


def save_data(data: list, filename: str = "cleaned_applicant_data.json") -> None:
    """Save cleaned applicant data to JSON."""
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

# -------------------------
# Script entry point
# -------------------------

if __name__ == "__main__":
    raw_data = load_data("applicant_data.json")
    cleaned_data = clean_data(raw_data)
    save_data(cleaned_data, "cleaned_applicant_data.json")
    print(f"Cleaned data saved to cleaned_applicant_data.json ({len(cleaned_data)} records)")
