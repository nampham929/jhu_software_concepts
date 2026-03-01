"""Legacy scrape-clean orchestration preserved under the module_6 web package."""

from __future__ import annotations

from app import data_cleaning, scrape_support


def main() -> None:
    """Run the raw scrape step followed by the clean step."""
    print("Starting scraping process...")
    print(scrape_support.check_robots_allowed(scrape_support.BASE_URL))

    raw_data = scrape_support.scrape_data(pages=1600)
    scrape_support.save_data(raw_data, "applicant_data.json")
    print(f"Scraping complete: {len(raw_data)} records saved to applicant_data.json")

    print("\nStarting cleaning process...")
    loaded = data_cleaning.load_data("applicant_data.json")
    cleaned = data_cleaning.clean_data(loaded)
    data_cleaning.save_data(cleaned, "cleaned_applicant_data.json")
    print(f"Cleaning complete: {len(cleaned)} records saved to cleaned_applicant_data.json")
