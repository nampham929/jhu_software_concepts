import os
import sys

from flask import Blueprint, render_template, redirect, request, url_for
import psycopg
from psycopg import OperationalError

from load_data import create_applicants_table, parse_date, parse_float


dashboard_bp = Blueprint("dashboard", __name__)

DB_CONFIG = {
    "db_name": "postgres",
    "db_user": "postgres",
    "db_password": "dataBase!605",
    "db_host": "localhost",
    "db_port": "5432",
}


def create_connection(db_name, db_user, db_password, db_host, db_port):
    """Create a connection to PostgreSQL database."""
    connection = None
    try:
        connection = psycopg.connect(
            dbname=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
        )
    except OperationalError as exc:
        raise RuntimeError(f"Database connection failed: {exc}") from exc
    return connection


def load_query_results():
    queries = [
        {
            "title": "Q1: Entries for Fall 2026",
            "description": "Counts entries where term is exactly 'Fall 2026'.",
            "sql": """
                SELECT COUNT(*)
                FROM applicants
                WHERE term = 'Fall 2026';
            """,
            "params": None,
        },
        {
            "title": "Q2: Percent international (not American/Other)",
            "description": "Percent of entries where nationality is not American or Other.",
            "sql": """
                SELECT ROUND(
                    100.0 * SUM(CASE WHEN LOWER(us_or_international) NOT IN ('american', 'other') THEN 1 ELSE 0 END)
                    / NULLIF(COUNT(*), 0),
                    2
                ) AS percent_international
                FROM applicants
                WHERE us_or_international IS NOT NULL AND us_or_international <> '';
            """,
            "params": None,
        },
        {
            "title": "Q3: Average GPA/GRE metrics",
            "description": "Averages only rows where each metric is present.",
            "sql": """
                SELECT
                    AVG(gpa) AS avg_gpa,
                    AVG(gre) AS avg_gre,
                    AVG(gre_v) AS avg_gre_v,
                    AVG(gre_aw) AS avg_gre_aw
                FROM applicants
                WHERE gpa IS NOT NULL OR gre IS NOT NULL OR gre_v IS NOT NULL OR gre_aw IS NOT NULL;
            """,
            "params": None,
        },
        {
            "title": "Q4: Average GPA of American students in Fall 2026",
            "description": "Averages GPA for American students only, within Fall 2026 entries.",
            "sql": """
                SELECT AVG(gpa) AS avg_gpa_american_fall_2026
                FROM applicants
                WHERE term = 'Fall 2026'
                  AND LOWER(us_or_international) = 'american'
                  AND gpa IS NOT NULL
                  AND gpa < 5.0;
            """,
            "params": None,
        },
        {
            "title": "Q5: Percent of Fall 2025 acceptances",
            "description": "Percent of Fall 2025 entries with status 'Accepted'.",
            "sql": """
                SELECT ROUND(
                    100.0 * SUM(CASE WHEN LOWER(status) = 'accepted' THEN 1 ELSE 0 END)
                    / NULLIF(COUNT(*), 0),
                    2
                ) AS percent_accept_fall_2025
                FROM applicants
                WHERE term = 'Fall 2025';
            """,
            "params": None,
        },
        {
            "title": "Q6: Average GPA of Fall 2026 acceptances",
            "description": "Averages GPA for accepted applicants in Fall 2026.",
            "sql": """
                SELECT ROUND(AVG(gpa)::numeric, 2) AS avg_gpa_fall_2026_accepts
                FROM applicants
                WHERE term = 'Fall 2026'
                  AND LOWER(status) = 'accepted'
                  AND gpa IS NOT NULL
                  AND gpa < 5.0;
            """,
            "params": None,
        },
        {
            "title": "Q7: JHU masters in Computer Science",
            "description": "Counts entries with program including Johns Hopkins and Computer Science, and degree as masters.",
            "sql": """
                SELECT COUNT(*)
                FROM applicants
                WHERE program ILIKE %s
                  AND program ILIKE %s
                  AND degree ILIKE %s;
            """,
            "params": ("%Johns Hopkins%", "%Computer Science%", "Master%"),
        },
        {
            "title": "Q8: 2026 CS PhD acceptances (selected universities)",
            "description": "Counts acceptances in 2026 for Georgetown, MIT, Stanford, or CMU in CS PhD.",
            "sql": """
                SELECT COUNT(*)
                FROM applicants
                WHERE term LIKE %s
                  AND LOWER(status) = 'accepted'
                  AND degree ILIKE %s
                  AND program ILIKE %s
                  AND (
                      program ILIKE %s
                      OR program ILIKE %s
                      OR program ILIKE %s
                      OR program ILIKE %s
                      OR program ILIKE %s
                      OR program ILIKE %s
                      OR program ILIKE %s
                  );
            """,
            "params": (
                "%2026%",
                "PhD%",
                "%Computer Science%",
                "%George Town%",
                "%Georgetown%",
                "%Massachusetts Institute of Technology%",
                "%MIT%",
                "%Stanford University%",
                "%Carnegie Mellon University%",
                "%CMU%",
            ),
        },
        {
            "title": "Q9: 2026 CS PhD acceptances using LLM fields",
            "description": "Counts acceptances in 2026 using llm_generated_university and llm_generated_program.",
            "sql": """
                SELECT COUNT(*)
                FROM applicants
                WHERE term LIKE %s
                  AND LOWER(status) = 'accepted'
                  AND degree ILIKE %s
                  AND llm_generated_program ILIKE %s
                  AND (
                      llm_generated_university ILIKE %s
                      OR llm_generated_university ILIKE %s
                      OR llm_generated_university ILIKE %s
                      OR llm_generated_university ILIKE %s
                      OR llm_generated_university ILIKE %s
                      OR llm_generated_university ILIKE %s
                  );
            """,
            "params": (
                "%2026%",
                "PhD%",
                "%Computer Science%",
                "%George Town University%",
                "%Massachusetts Institute of Technology%",
                "%MIT%",
                "%Stanford University%",
                "%Carnegie Mellon University%",
                "%CMU%",
            ),
        },
        {
            "title": "Q10: Top 5 universities by total submissions",
            "description": "Top 5 universities by count (using LLM generated university).",
            "sql": """
                SELECT llm_generated_university, COUNT(*) AS total_submissions
                FROM applicants
                WHERE llm_generated_university IS NOT NULL AND llm_generated_university <> ''
                GROUP BY llm_generated_university
                ORDER BY total_submissions DESC
                LIMIT 5;
            """,
            "params": None,
        },
        {
            "title": "Q11: Acceptance rate for international students",
            "description": "Acceptance rate for international students (not American/Other).",
            "sql": """
                SELECT ROUND(
                    100.0 * SUM(CASE WHEN LOWER(status) = 'accepted' THEN 1 ELSE 0 END)
                    / NULLIF(COUNT(*), 0),
                    2
                ) AS percent_international_accepts
                FROM applicants
                WHERE LOWER(us_or_international) NOT IN ('american', 'other')
                  AND us_or_international IS NOT NULL
                  AND us_or_international <> '';
            """,
            "params": None,
        },
    ]

    results = []
    connection = create_connection(**DB_CONFIG)
    try:
        for query in queries:
            cursor = connection.execute(query["sql"], query["params"] or ())
            rows = cursor.fetchall()
            columns = [col.name for col in cursor.description] if cursor.description else []
            results.append(
                {
                    "title": query["title"],
                    "description": query["description"],
                    "sql": query["sql"].strip(),
                    "columns": columns,
                    "rows": rows,
                    "error": None,
                }
            )
    except Exception as exc:
        results.append(
            {
                "title": "Query Error",
                "description": "An error occurred while running the query set.",
                "sql": "",
                "columns": [],
                "rows": [],
                "error": str(exc),
            }
        )
    finally:
        connection.close()

    return results


def _ensure_module_2_on_path() -> str:
    module_2_path = os.path.join(os.path.dirname(__file__), "..", "module_2")
    module_2_path = os.path.abspath(module_2_path)
    if module_2_path not in sys.path:
        sys.path.insert(0, module_2_path)
    return module_2_path


def _fetch_existing_urls(connection) -> set:
    cursor = connection.execute(
        "SELECT url FROM applicants WHERE url IS NOT NULL AND url <> '';"
    )
    return {row[0] for row in cursor.fetchall()}


def pull_gradcafe_data(pages: int = 5) -> dict:
    _ensure_module_2_on_path()
    import scrape
    import clean

    raw_data = scrape.scrape_data(pages=pages)
    cleaned_data = clean.clean_data(raw_data)

    connection = create_connection(**DB_CONFIG)
    create_applicants_table(connection)
    existing_urls = _fetch_existing_urls(connection)

    insert_query = """
        INSERT INTO applicants (
            program, comments, date_added, url, status, term,
            us_or_international, gpa, gre, gre_v, gre_aw,
            degree, llm_generated_program, llm_generated_university
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    inserted_count = 0
    duplicate_count = 0
    missing_url_count = 0
    error_count = 0

    try:
        for index, entry in enumerate(cleaned_data, 1):
            url = entry.get("url") or ""
            if not url:
                missing_url_count += 1
                continue
            if url in existing_urls:
                duplicate_count += 1
                continue

            try:
                connection.execute(
                    insert_query,
                    (
                        entry.get("program"),
                        entry.get("comments"),
                        parse_date(entry.get("date_added")),
                        url,
                        entry.get("status"),
                        entry.get("term"),
                        entry.get("US/International"),
                        parse_float(entry.get("GPA")),
                        parse_float(entry.get("GRE_SCORE")),
                        parse_float(entry.get("GRE_V")),
                        parse_float(entry.get("GRE_AW")),
                        entry.get("Degree"),
                        None,
                        None,
                    ),
                )
                inserted_count += 1
                existing_urls.add(url)
            except Exception:
                error_count += 1
                connection.rollback()

            if index % 100 == 0:
                connection.commit()

        connection.commit()
    finally:
        connection.close()

    return {
        "pages": pages,
        "inserted": inserted_count,
        "duplicates": duplicate_count,
        "missing_urls": missing_url_count,
        "errors": error_count,
    }


@dashboard_bp.route("/")
def dashboard():
    pull_status = request.args.get("pull_status")
    pull_message = request.args.get("pull_message")
    results = load_query_results()
    return render_template(
        "dashboard.html",
        results=results,
        pull_status=pull_status,
        pull_message=pull_message,
    )


@dashboard_bp.route("/pull-data", methods=["POST"])
def pull_data():
    try:
        summary = pull_gradcafe_data(pages=5)
        message = (
            "Pulled {pages} pages. Added {inserted} new entries; "
            "skipped {duplicates} duplicates and {missing_urls} entries without URLs."
        ).format(**summary)
        if summary["errors"]:
            message += f" {summary['errors']} inserts failed."
        return redirect(
            url_for("dashboard.dashboard", pull_status="success", pull_message=message)
        )
    except Exception as exc:
        message = f"Pull failed: {exc}"
        return redirect(
            url_for("dashboard.dashboard", pull_status="error", pull_message=message)
        )
