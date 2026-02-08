
import scrape
import clean


def main():
    print("Starting scraping process...")
    
    # Check robots.txt before scraping
    print(scrape._check_robots_allowed(scrape.BASE_URL))

    #Scrape data
    raw_data = scrape.scrape_data(pages=1600)
    scrape.save_data(raw_data, "applicant_data.json")
    print(f"Scraping complete: {len(raw_data)} records saved to applicant_data.json")

    print("\nStarting cleaning process...")

    #Load raw data
    data = clean.load_data("applicant_data.json")

    #Clean data
    cleaned_data = clean.clean_data(data)

    #Save cleaned data
    clean.save_data(cleaned_data, "cleaned_applicant_data.json")
    print(f"Cleaning complete: {len(cleaned_data)} records saved to cleaned_applicant_data.json")


if __name__ == "__main__":
    main()
