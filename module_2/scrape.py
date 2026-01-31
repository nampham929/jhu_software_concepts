# scrape.py
from urllib.request import Request, urlopen
from bs4 import BeautifulSoup
import re
import json

HEADERS = {"User-Agent": "Mozilla/5.0"}

BASE_URL = "https://www.thegradcafe.com/survey/"

def fetch_html(url):
    """Fetch HTML content from a URL using urllib."""
    req = Request(url, headers=HEADERS)
    with urlopen(req) as resp:
        return resp.read().decode("utf-8")


def parse_results(html):
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

        # --- MAIN INFO ---
        university = cols[0].get_text(strip=True)

        program_td = cols[1]
        spans = program_td.find_all("span")
        program_name = spans[0].get_text(strip=True) if len(spans) > 0 else None
        degree = spans[1].get_text(strip=True) if len(spans) > 1 else None

        date_added = cols[2].get_text(strip=True)

        status_text = cols[3].get_text(strip=True)
        status = None
        decision_date = None
        if "Accepted" in status_text:
            status = "Accepted"
            m = re.search(r"Accepted on (.+)", status_text)
            if m:
                decision_date = m.group(1)
        elif "Rejected" in status_text:
            status = "Rejected"
            m = re.search(r"Rejected on (.+)", status_text)
            if m:
                decision_date = m.group(1)

        link = row.find("a", href=re.compile(r"/result/"))
        entry_url = "https://www.thegradcafe.com" + link["href"] if link else None

        # --- LOOK AHEAD FOR METADATA / COMMENTS ---
        term = citizenship = gpa = comments = None
        gre_total = gre_v = gre_aw = None

        j = i + 1
        while j < len(rows) and "tw-border-none" in rows[j].get("class", []):
            text = rows[j].get_text(" ", strip=True)

            # Term (semester/year)
            term_match = re.search(r"(Fall|Spring|Summer)\s+\d{4}", text)
            if term_match:
                term = term_match.group(0)

            # Citizenship
            if "American" in text:
                citizenship = "American"
            elif "International" in text:
                citizenship = "International"

            # GPA
            gpa_match = re.search(r"GPA\s*[:]?[\s]*([\d.]+)", text)
            if gpa_match:
                gpa = gpa_match.group(1)

            # Comments
            comment_p = rows[j].find("p")
            if comment_p:
                comments = comment_p.get_text(strip=True)

            # --- GRE Scores ---
            gre_total_match = re.search(r"GRE[:\s]*([\d]{3})", text)
            if gre_total_match:
                gre_total = gre_total_match.group(1)

            gre_v_match = re.search(r"V[:\s]*([\d]{2,3})", text)
            if gre_v_match:
                gre_v = gre_v_match.group(1)

            gre_aw_match = re.search(r"AW[:\s]*([\d.]+)", text)
            if gre_aw_match:
                gre_aw = gre_aw_match.group(1)

            j += 1

        results.append({
            "Program Name": program_name or "",
            "University": university or "",
            "Comments": comments or "",
            "Date of Information Added to Grad Café": date_added or "",
            "URL link to applicant entry": entry_url or "",
            "Applicant Status": status or "",
            "Accepted / Rejected Date": decision_date or "",
            "Semester and Year of Program Start": term or "",
            "International / American Student": citizenship or "",
            "Masters or PhD": degree or "",
            "GPA": gpa or "",
            "GRE Score": gre_total or "",
            "GRE V Score": gre_v or "",
            "GRE AW": gre_aw or ""
        })

        i = j

    return results


def scrape_data(pages=2):
    """Scrape Grad Café data for multiple pages."""
    data = []
    for page_number in range(1, pages + 1):
        url = f"{BASE_URL}?page={page_number}"
        html = fetch_html(url)
        page_data = parse_results(html)
        data.extend(page_data)
        print(f"Scraped page {page_number}, entries found: {len(page_data)}")
    return data


def save_data(data, filename="applicant_data.json"):
    """Save scraped data to JSON."""
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def load_data(filename="applicant_data.json"):
    """Load scraped data from JSON."""
    with open(filename, "r", encoding="utf-8") as f:
        return json.load(f)


if __name__ == "__main__":
    data = scrape_data(pages=2)  # Increase pages for larger dataset
    print(f"Total entries scraped: {len(data)}")
    save_data(data)
