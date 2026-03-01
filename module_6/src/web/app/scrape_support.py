"""GradCafe scraping helpers formerly provided by the legacy module_2 package."""

from __future__ import annotations

import json
import re
import urllib.robotparser
from urllib.request import Request, urlopen

from bs4 import BeautifulSoup


HEADERS = {"User-Agent": "Mozilla/5.0"}
BASE_URL = "https://www.thegradcafe.com/survey/"


def _check_robots_allowed(base_url: str, user_agent: str = "Mozilla/5.0") -> str:
    """Return whether the GradCafe robots rules allow scraping the target URL."""
    parser = urllib.robotparser.RobotFileParser()
    parser.set_url("https://www.thegradcafe.com/robots.txt")
    parser.read()
    if parser.can_fetch(user_agent, base_url):
        return f"Scraping is ALLOWED for {base_url} according to robots.txt."
    return f"Scraping is DISALLOWED for {base_url} according to robots.txt."


def check_robots_allowed(base_url: str, user_agent: str = "Mozilla/5.0") -> str:
    """Public wrapper for the robots check used by the compatibility runner."""
    return _check_robots_allowed(base_url, user_agent)


def _fetch_html(url: str) -> str:
    """Fetch one survey page and return the decoded HTML body."""
    request = Request(url, headers=HEADERS)
    with urlopen(request) as response:
        return response.read().decode("utf-8")


def fetch_html(url: str) -> str:
    """Public wrapper for fetching one survey page."""
    return _fetch_html(url)


def _parse_page(html: str) -> list[dict]:
    """Parse one GradCafe survey page into structured applicant rows."""
    soup = BeautifulSoup(html, "html.parser")
    tbody = soup.find("tbody")
    if not tbody:
        return []

    rows = tbody.find_all("tr", recursive=False)
    results: list[dict] = []

    index = 0
    while index < len(rows):
        row = rows[index]

        if "tw-border-none" in row.get("class", []):
            index += 1
            continue

        cols = row.find_all("td", recursive=False)
        if len(cols) < 4:
            index += 1
            continue

        university = cols[0].get_text(strip=True)
        spans = cols[1].find_all("span")
        program_name = spans[0].get_text(strip=True) if spans else ""
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

        detail_index = index + 1
        while detail_index < len(rows) and "tw-border-none" in rows[detail_index].get("class", []):
            text = rows[detail_index].get_text(" ", strip=True)
            paragraph = rows[detail_index].find("p")
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

            detail_index += 1

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
        index = detail_index

    return results


def parse_page(html: str) -> list[dict]:
    """Public wrapper for parsing one survey page."""
    return _parse_page(html)


def scrape_data(pages: int = 5) -> list[dict]:
    """Scrape a fixed number of GradCafe survey pages."""
    data: list[dict] = []
    for page in range(1, pages + 1):
        html = _fetch_html(f"{BASE_URL}?page={page}")
        data.extend(_parse_page(html))
    return data


def save_data(data: list[dict], filename: str = "applicant_data.json") -> None:
    """Persist scraped raw data to JSON."""
    with open(filename, "w", encoding="utf-8") as file_handle:
        json.dump(data, file_handle, indent=2, ensure_ascii=False)
