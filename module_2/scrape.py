# scrape.py
from urllib.request import Request, urlopen
from bs4 import BeautifulSoup
import json
import re

HEADERS = {"User-Agent": "Mozilla/5.0"}
BASE_URL = "https://www.thegradcafe.com/survey/"

# -------------------------
# Internal helpers
# -------------------------

def _fetch_html(url: str) -> str:
    req = Request(url, headers=HEADERS)
    with urlopen(req) as resp:
        return resp.read().decode("utf-8")

def _parse_page(html: str) -> list:
    soup = BeautifulSoup(html, "html.parser")
    tbody = soup.find("tbody")
    if not tbody:
        return []

    rows = tbody.find_all("tr", recursive=False)
    results = []

    i = 0
    while i < len(rows):
        row = rows[i]

        # Skip metadata-only rows
        if "tw-border-none" in row.get("class", []):
            i += 1
            continue

        cols = row.find_all("td", recursive=False)
        if len(cols) < 4:
            i += 1
            continue

        # -------- Main row --------
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

        # -------- Defaults --------
        comments = ""
        term = ""
        citizenship = ""
        gre_total = ""
        gre_v = ""
        gre_aw = ""
        gpa = ""

        # -------- Look-ahead metadata rows --------
        j = i + 1
        while j < len(rows) and "tw-border-none" in rows[j].get("class", []):
            text = rows[j].get_text(" ", strip=True)

            # Comments
            p = rows[j].find("p")
            if p:
                comments = p.get_text(strip=True)

            # Semester / year
            m = re.search(r"(Fall|Spring|Summer)\s+\d{4}", text)
            if m:
                term = m.group(0)

            # Citizenship
            if "American" in text:
                citizenship = "American"
            elif "International" in text:
                citizenship = "International"

            # GPA
            m = re.search(r"GPA\s*[:]?[\s]*([\d.]+)", text)
            if m:
                gpa = m.group(1)

            # GRE total
            m = re.search(r"GRE\s*[:]?[\s]*([\d]{3})", text)
            if m:
                gre_total = m.group(1)

            # GRE Verbal
            m = re.search(r"V\s*[:]?[\s]*([\d]{2,3})", text)
            if m:
                gre_v = m.group(1)

            # GRE AW
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
            "applicant_status": applicant_status,
            "decision_date": decision_date,
            "semester_start": term,
            "citizenship": citizenship,
            "gre_score": gre_total,
            "gre_v": gre_v,
            "gre_aw": gre_aw,
            "gpa": gpa,
            "degree_type": degree_type
        })

        i = j

    return results

# -------------------------
# Required public functions
# -------------------------

def scrape_data(pages: int = 5) -> list:
    data = []
    for page in range(1, pages + 1):
        html = _fetch_html(f"{BASE_URL}?page={page}")
        page_data = _parse_page(html)
        data.extend(page_data)
    return data

def save_data(data: list, filename: str = "applicant_data.json") -> None:
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

# -------------------------
# Script entry point
# -------------------------

if __name__ == "__main__":
    scraped = scrape_data(pages=5)
    save_data(scraped)
    print(f"Saved {len(scraped)} entries to applicant_data.json")
