import json
import re
from urllib.request import urlopen, Request
from urllib.error import HTTPError, URLError
from bs4 import BeautifulSoup

BASE_URL = "https://www.thegradcafe.com/survey/?page={page}"
RAW_OUTPUT_FILE = "raw_applicant_data.json"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; GradCafeScraper/1.0)"
}


def _fetch(url):
    """Fetch a URL using urllib and return decoded HTML or None."""
    req = Request(url, headers=HEADERS)
    try:
        with urlopen(req) as resp:
            return resp.read().decode("utf-8", errors="ignore")
    except (HTTPError, URLError):
        return None


def _extract(pattern, text):
    """Extract a single regex group from text."""
    m = re.search(pattern, text, flags=re.IGNORECASE)
    return m.group(1) if m else None


def _parse_row(tr):
    """Parse a single GradCafe table row."""
    tds = tr.find_all("td")
    if len(tds) < 5:
        return None

    col0 = tds[0]
    program_raw = col0.get_text(strip=True)
    link_tag = col0.find("a")
    url = link_tag["href"] if link_tag and link_tag.has_attr("href") else None

    decision_text = tds[1].get_text(strip=True)
    date_added = tds[2].get_text(strip=True)
    status_text = tds[3].get_text(strip=True)
    comments = tds[4].get_text(" ", strip=True)

    gre = _extract(r"\bGRE[: ]+(\d+)", comments)
    gre_v = _extract(r"\bV[: ]+(\d+)", comments)
    gre_aw = _extract(r"\bAW[: ]+([\d.]+)", comments)
    gpa = _extract(r"\bGPA[: ]+([\d.]+)", comments)

    lower_all = (comments + " " + status_text).lower()
    international_status = None
    if "international" in lower_all:
        international_status = "International"
    elif "american" in lower_all or "domestic" in lower_all:
        international_status = "American"

    degree_type = None
    if re.search(r"\bph\.?d\b|\bphd\b", program_raw, flags=re.IGNORECASE):
        degree_type = "PhD"
    elif re.search(r"\bms\b|\bmsc\b|\bma\b|\bmaster", program_raw, flags=re.IGNORECASE):
        degree_type = "Masters"

    sem_match = re.search(
        r"\b(Fall|Spring|Summer|Winter)\s+(\d{4})",
        comments + " " + status_text,
        flags=re.IGNORECASE,
    )
    semester_start = sem_match.group(1).title() if sem_match else None
    year_start = sem_match.group(2) if sem_match else None

    accepted_date = _extract(
        r"Accepted on ([A-Za-z]{3,9}\s+\d{1,2},\s*\d{4})",
        comments,
    )
    rejected_date = _extract(
        r"Rejected on ([A-Za-z]{3,9}\s+\d{1,2},\s*\d{4})",
        comments,
    )

    if re.search(r"accept", decision_text, flags=re.IGNORECASE):
        applicant_status = "Accepted"
    elif re.search(r"reject", decision_text, flags=re.IGNORECASE):
        applicant_status = "Rejected"
    elif re.search(r"interview", decision_text, flags=re.IGNORECASE):
        applicant_status = "Interview"
    elif re.search(r"waitlist", decision_text, flags=re.IGNORECASE):
        applicant_status = "Waitlisted"
    else:
        applicant_status = decision_text or None

    return {
        "program": program_raw,
        "comments": comments or None,
        "date_added": date_added or None,
        "url": url,
        "applicant_status": applicant_status,
        "accepted_date": accepted_date,
        "rejected_date": rejected_date,
        "semester_start": semester_start,
        "year_start": year_start,
        "international_status": international_status,
        "gre": gre,
        "gre_v": gre_v,
        "gre_aw": gre_aw,
        "gpa": gpa,
        "degree_type": degree_type,
    }


def scrape_data(min_entries=30, max_pages=3):
    """Scrape GradCafe until at least min_entries rows are collected."""
    all_rows = []

    for page in range(1, max_pages + 1):
        html = _fetch(BASE_URL.format(page=page))
        if not html:
            continue

        soup = BeautifulSoup(html, "html.parser")
        table = soup.find("table")
        if not table:
            continue

        trs = table.find_all("tr")
        for tr in trs[1:]:
            parsed = _parse_row(tr)
            if parsed:
                all_rows.append(parsed)

        print("Page", page, ":", len(all_rows), "rows")

        if len(all_rows) >= min_entries:
            break

    return all_rows


def save_data(rows, path=RAW_OUTPUT_FILE):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)
    print("Saved", len(rows), "rows to", path)


if __name__ == "__main__":
    rows = scrape_data()
    save_data(rows)
