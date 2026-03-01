"""GradCafe scraping helpers used by the worker pull task."""

import json
import re
import urllib.robotparser
from urllib.request import Request, urlopen

from bs4 import BeautifulSoup

HEADERS = {"User-Agent": "Mozilla/5.0"}
BASE_URL = "https://www.thegradcafe.com/survey/"


def _check_robots_allowed(base_url: str, user_agent: str = "Mozilla/5.0") -> str:
    """Read robots.txt and report whether scraping the target URL is allowed."""
    rp = urllib.robotparser.RobotFileParser()
    rp.set_url("https://www.thegradcafe.com/robots.txt")
    rp.read()

    allowed = rp.can_fetch(user_agent, base_url)
    if allowed:
        return f"Scraping is ALLOWED for {base_url} according to robots.txt."
    return f"Scraping is DISALLOWED for {base_url} according to robots.txt."


def _fetch_html(url: str) -> str:
    """Send a request to one GradCafe page and return its HTML."""
    req = Request(url, headers=HEADERS)
    with urlopen(req) as resp:
        return resp.read().decode("utf-8")


def _parse_page(html: str) -> list:
    """Parse one GradCafe results page into applicant dictionaries."""
    soup = BeautifulSoup(html, "html.parser")
    tbody = soup.find("tbody")
    if not tbody:
        return []

    rows = tbody.find_all("tr", recursive=False)
    results = []

    i = 0
    while i < len(rows):
        row = rows[i]

        if "tw-border-none" in row.get("class", []):
            i += 1
            continue

        cols = row.find_all("td", recursive=False)
        if len(cols) < 4:
            i += 1
            continue

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
            match = re.search(r"Accepted on (.+)", status_text)
            decision_date = match.group(1) if match else ""
        elif "Rejected" in status_text:
            applicant_status = "Rejected"
            match = re.search(r"Rejected on (.+)", status_text)
            decision_date = match.group(1) if match else ""

        link = row.find("a", href=re.compile(r"/result/"))
        entry_url = "https://www.thegradcafe.com" + link["href"] if link else ""

        comments = ""
        term = ""
        citizenship = ""
        gre_total = ""
        gre_v = ""
        gre_aw = ""
        gpa = ""

        j = i + 1
        while j < len(rows) and "tw-border-none" in rows[j].get("class", []):
            text = rows[j].get_text(" ", strip=True)

            paragraph = rows[j].find("p")
            if paragraph:
                comments = paragraph.get_text(strip=True)

            match = re.search(r"(Fall|Spring|Summer)\s+\d{4}", text)
            if match:
                term = match.group(0)

            if "American" in text:
                citizenship = "American"
            elif "International" in text:
                citizenship = "International"

            match = re.search(r"GPA\s*[:]?[\s]*([\d.]+)", text)
            if match:
                gpa = match.group(1)

            match = re.search(r"GRE\s*[:]?[\s]*([\d]{3})", text)
            if match:
                gre_total = match.group(1)

            match = re.search(r"V\s*[:]?[\s]*([\d]{2,3})", text)
            if match:
                gre_v = match.group(1)

            match = re.search(r"AW\s*[:]?[\s]*([\d.]+)", text)
            if match:
                gre_aw = match.group(1)

            j += 1

        results.append(
            {
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
                "Degree": degree_type,
            }
        )

        i = j

    return results


def scrape_data(pages: int = 5) -> list:
    """Collect applicant data across multiple survey pages."""
    data = []
    for page in range(1, pages + 1):
        print(f"Fetching page {page}...")
        html = _fetch_html(f"{BASE_URL}?page={page}")
        page_data = _parse_page(html)
        data.extend(page_data)
    return data


def save_data(data: list, filename: str = "applicant_data.json") -> None:
    """Write scraped applicant data to a JSON file."""
    with open(filename, "w", encoding="utf-8") as file_handle:
        json.dump(data, file_handle, indent=2, ensure_ascii=False)


if __name__ == "__main__":  # pragma: no cover
    print(_check_robots_allowed(BASE_URL))
    scraped = scrape_data(pages=5)
    save_data(scraped)
    print(f"Saved {len(scraped)} entries to applicant_data.json")
