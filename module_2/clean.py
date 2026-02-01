import json
import re

# Open the JSON file created by scrape.py
def load_data(filename: str = "applicant_data.json") -> list:
    # Return a list of dictionaries
    with open(filename, "r", encoding="utf-8") as f:
        return json.load(f)

# standardize formatting
def clean_data(data: list) -> list:
    cleaned = []
    
    # Go through each dictionary
    for row in data:
        clean_row = {}
        # Go through each key value pair
        for key, value in row.items():
            if value is None:
                clean_value = ""
            elif isinstance(value, str):
                # Remove HTML tags
                clean_value = re.sub(r"<[^>]+>", "", value)

                 # Replace multiple spaces, tabs, or line breaks with a single space
                clean_value = re.sub(r"\s+", " ", clean_value).strip()

                # Replace placeholder junk values with empty string
                if clean_value.lower() in {"n/a", "na", "none", "-", "—"}:
                    clean_value = ""
            else:
                clean_value = value

            # Save cleaned value back under the same key
            clean_row[key] = clean_value

        # Add the cleaned record to output list
        cleaned.append(clean_row)

    return cleaned

# Save cleaned applicant data back to a JSON file.
def save_data(data: list, filename: str = "cleaned_applicant_data.json") -> None:
    """Save cleaned applicant data to JSON."""
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

if __name__ == "__main__":
    raw_data = load_data("applicant_data.json")
    cleaned_data = clean_data(raw_data)
    save_data(cleaned_data, "applicant_data.json")
    print(f"Cleaned data saved to cleaned_applicant_data.json ({len(cleaned_data)} records)")
