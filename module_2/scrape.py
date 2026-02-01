from urllib.request import Request, urlopen
import urllib.robotparser
from bs4 import BeautifulSoup
import json
import re
import time

HEADERS = {"User-Agent": "Mozilla/5.0"}
BASE_URL = "https://www.thegradcafe.com/survey/"


# Check robots.txt
def _check_robots_allowed(base_url: str, user_agent: str = "Mozilla/5.0") -> str:
    """
    Reads robots.txt and checks if scraping the given URL is allowed.
    """
    rp = urllib.robotparser.RobotFileParser()
    rp.set_url("https://www.thegradcafe.com/robots.txt")
    rp.read()

    allowed = rp.can_fetch(user_agent, base_url)

    if allowed:
        return f"Scraping is ALLOWED for {base_url} according to robots.txt."
    else:
        return f"Scraping is DISALLOWED for {base_url} according to robots.txt."
'''
Sends a web request to the given URL and 
returns the HTML page as a string.
'''
def _fetch_html(url: str) -> str:
    req = Request(url, headers=HEADERS)
    with urlopen(req) as resp:
        return resp.read().decode("utf-8")

'''
Takes raw HTML from a GradCafe page and extracts applicant records.
Returns a list of dictionaries (one per applicant).
'''
def _parse_page(html: str) -> list:
    # Parse HTML text
    soup = BeautifulSoup(html, "html.parser")
    # Obtain all data in the table body section
    tbody = soup.find("tbody")
    if not tbody:
        return []

    '''
    Each applicant entry spans multiple rows.
    Store the rows of each entry in a list.
    '''
    rows = tbody.find_all("tr", recursive=False)
    results = []

    i = 0
    # Collect information from the main row of an entry
    while i < len(rows):
        row = rows[i]

        # Skip the empty rows
        if "tw-border-none" in row.get("class", []):
            i += 1
            continue
        
        # Application data from each row is stored in the 'cols' list
        cols = row.find_all("td", recursive=False)
        if len(cols) < 4:
            i += 1
            continue

        # Collect need information from each element in the 'cols' list
        university = cols[0].get_text(strip=True)

        spans = cols[1].find_all("span")
        program_name = spans[0].get_text(strip=True) if len(spans) > 0 else ""
        degree_type = spans[1].get_text(strip=True) if len(spans) > 1 else ""

        program = f"{program_name}, {university}"

        date_added = cols[2].get_text(strip=True)

        status_text = cols[3].get_text(" ", strip=True)
        applicant_status = ""
        decision_date = ""

        if "Accepted" in status_text:
            applicant_status = "Accepted"
            m = re.search(r"Accepted on (.+)", status_text)
            decision_date = m.group(1) if m else ""
        elif "Rejected" in status_text:
            applicant_status = "Rejected"
            m = re.search(r"Rejected on (.+)", status_text)
            decision_date = m.group(1) if m else ""

        link = row.find("a", href=re.compile(r"/result/"))
        entry_url = "https://www.thegradcafe.com" + link["href"] if link else ""

        '''
        Some application data elements are not always availalbe.
        These are set at blank by default
        '''
        comments = ""
        term = ""
        citizenship = ""
        gre_total = ""
        gre_v = ""
        gre_aw = ""
        gpa = ""

        # Collect information from the next row of an entry
        j = i + 1
        while j < len(rows) and "tw-border-none" in rows[j].get("class", []):
            text = rows[j].get_text(" ", strip=True)

            # Collect comments
            p = rows[j].find("p")
            if p:
                comments = p.get_text(strip=True)

            # Collect terms information
            m = re.search(r"(Fall|Spring|Summer)\s+\d{4}", text)
            if m:
                term = m.group(0)

            # Collect Citizenship status
            if "American" in text:
                citizenship = "American"
            elif "International" in text:
                citizenship = "International"

            # Collect GPA information
            m = re.search(r"GPA\s*[:]?[\s]*([\d.]+)", text)
            if m:
                gpa = m.group(1)

            # Collect GRE total information
            m = re.search(r"GRE\s*[:]?[\s]*([\d]{3})", text)
            if m:
                gre_total = m.group(1)

            # Collect GRE Verbal information
            m = re.search(r"V\s*[:]?[\s]*([\d]{2,3})", text)
            if m:
                gre_v = m.group(1)

            # Collect GRE AW information
            m = re.search(r"AW\s*[:]?[\s]*([\d.]+)", text)
            if m:
                gre_aw = m.group(1)

            j += 1

        # -------- Final record --------
        results.append({
            "program": program,
            "comments": comments,
            "date_added": date_added,
            "url": entry_url,
            "status": applicant_status,
            "decision_date": decision_date,
            "term": term,
            "US/International": citizenship,
            "GRE_SCORE": gre_total,
            "GRE_V": gre_v,
            "GRE_AW": gre_aw,
            "GPA": gpa,
            "Degree": degree_type
        })

        i = j

    return results

#Loops through multiple survey pages and collects all applicant data.
def scrape_data(pages: int = 5) -> list:
    data = []
    for page in range(1, pages + 1):
        print(f"Fetching page {page}...")  # LIVE progress
        html = _fetch_html(f"{BASE_URL}?page={page}")
        time.sleep(0.5)
        page_data = _parse_page(html)
        data.extend(page_data)
    return data

#Saves the scraped data into a JSON file.
def save_data(data: list, filename: str = "applicant_data.json") -> None:
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

if __name__ == "__main__":
    print(_check_robots_allowed(BASE_URL))
    scraped = scrape_data(pages=5)
    save_data(scraped)
    print(f"Saved {len(scraped)} entries to applicant_data.json")
