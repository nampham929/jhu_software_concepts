"""Query definitions and helpers for reporting on applicants data."""

import os
from dataclasses import dataclass

import psycopg
from psycopg import OperationalError
from db_connection import (
    build_db_config,
    create_connection_from_env,
    create_connection_with_driver,
)


def get_queries():
    """Return query metadata and SQL used by CLI and dashboard views."""
    return [
        {
            "title": "Q1: Number of entries for Fall 2026:",
            "description": "Counts entries where term is exactly 'Fall 2026'.",
            "sql": """
                SELECT COUNT(*)
                FROM applicants
                WHERE term = 'Fall 2026';
            """,
            "params": None,
            "display_mode": "number",
        },
        {
            "title": "Q2: Percentage of entries of international students:",
            "description": (
                "Calculates the percentage of entries where nationality "
                "is not American or Other."
            ),
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
            "display_mode": "percent",
        },
        {
            "title": "Q3: The average GPA, GRE, GRE V, GRE AW of applicants:",
            "description": "Averages only rows where each metric is present.",
            "sql": """
                SELECT
                    ROUND(AVG(gpa)::numeric, 2) AS avg_gpa,
                    ROUND(AVG(gre)::numeric, 2) AS avg_gre,
                    ROUND(AVG(gre_v)::numeric, 2) AS avg_gre_v,
                    ROUND(AVG(gre_aw)::numeric, 2) AS avg_gre_aw
                FROM applicants
                WHERE gpa IS NOT NULL OR gre IS NOT NULL OR gre_v IS NOT NULL OR gre_aw IS NOT NULL;
            """,
            "params": None,
            "display_mode": "labels",
            "display_labels": ["GPA", "GRE", "GRE V", "GRE AW"],
        },
        {
            "title": "Q4: The average GPA of American students in Fall 2026:",
            "description": "Averages GPA for American students only, within Fall 2026 entries.",
            "sql": """
                SELECT ROUND(AVG(gpa)::numeric, 2) AS avg_gpa_american_fall_2026
                FROM applicants
                WHERE term = 'Fall 2026'
                  AND LOWER(us_or_international) = 'american'
                  AND gpa IS NOT NULL
                  AND gpa < 5.0;
            """,
            "params": None,
            "display_mode": "number",
        },
        {
            "title": "Q5: Percentage of entries of Fall 2025 acceptances",
            "description": "Calculates percentage of Fall 2025 entries with status 'Accepted'.",
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
            "display_mode": "percent",
        },
        {
            "title": "Q6: Average GPA of Fall 2026 acceptances:",
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
            "display_mode": "number",
        },
        {
            "title": (
                "Q7: Number of applicants who applied to JHU for a masters "
                "degree in Computer Science:"
            ),
            "description": (
                "Counts entries with program including Johns Hopkins and "
                "Computer Science, and degree as masters."
            ),
            "sql": """
                SELECT COUNT(*)
                FROM applicants
                WHERE program ILIKE %s
                  AND program ILIKE %s
                  AND degree ILIKE %s;
            """,
            "params": ("%Johns Hopkins%", "%Computer Science%", "Master%"),
            "display_mode": "number",
        },
        {
            "title": (
                "Q8: Number of 2026 Computer Science PhD acceptances "
                "(Georgetown, MIT, Stanford, or CMU):"
            ),
            "description": (
                "Counts acceptances in 2026 for Georgetown, MIT, Stanford, "
                "or CMU in CS PhD."
            ),
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
            "display_mode": "number",
        },
        {
            "title": "Q9: 2026 Computer Science PhD acceptances using LLM fields:",
            "description": (
                "Counts acceptances in 2026 using llm_generated_university "
                "and llm_generated_program."
            ),
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
            "display_mode": "number",
        },
        {
            "title": "Q10: Top 3 universities by total submissions:",
            "description": "Top 3 universities by count (using LLM generated university).",
            "sql": """
                SELECT llm_generated_university, COUNT(*) AS total_submissions
                FROM applicants
                WHERE llm_generated_university IS NOT NULL AND llm_generated_university <> ''
                GROUP BY llm_generated_university
                ORDER BY total_submissions DESC
                LIMIT 3;
            """,
            "params": None,
            "display_mode": "pairs",
        },
        {
            "title": "Q11: Acceptance rate for international students:",
            "description": (
                "Calculates acceptance rate for international students "
                "(not American/Other)."
            ),
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
            "display_mode": "number",
        },
    ]


# Create a connection to PostgreSQL database.
def create_connection(db_name, db_user, db_password, db_host, db_port):
    """Create and return a PostgreSQL connection."""
    return create_connection_with_driver(
        psycopg.connect,
        OperationalError,
        build_db_config(db_name, db_user, db_password, db_host, db_port),
    )


# Execute a SQL query and return rows and column names.
def execute_query(connection, sql_query, params=None):
    """Execute a SQL query and return rows plus column names."""
    cursor = connection.execute(sql_query, params or ())
    rows = cursor.fetchall()
    columns = [col.name for col in cursor.description] if cursor.description else []
    return rows, columns


# Format query results for simple display modes.
def format_display(rows, display_mode, display_labels=None):
    """Format query rows for supported dashboard display modes."""
    result = None
    if rows:
        if display_mode == "number" and len(rows) == 1 and len(rows[0]) == 1:
            result = str(rows[0][0])
        elif display_mode == "percent" and len(rows) == 1 and len(rows[0]) == 1:
            value = rows[0][0]
            result = "0.00%" if value is None else f"{float(value):.2f}%"
        elif display_mode == "labels" and len(rows) == 1:
            labels = display_labels or []
            if labels and len(labels) == len(rows[0]):
                result = ", ".join(
                    f"{label}: {value}" for label, value in zip(labels, rows[0])
                )
        elif display_mode == "pairs":
            result = ", ".join(
                f"{row[0]}: {row[1]}" for row in rows if len(row) >= 2
            )
    return result


@dataclass
class RunQueryOptions:
    """Display options for run_query formatting."""

    number_only: bool = False
    percent_only: bool = False
    number_labels: list[str] | None = None
    pair_only: bool = False


# Further format the result for display, and print it.
def run_query(
    connection,
    title,
    sql_query,
    params=None,
    options: RunQueryOptions | None = None,
):
    """Run one query and print a formatted answer for CLI usage."""
    print(f"\n{title}")
    display_options = options or RunQueryOptions()
    try:
        rows, _ = execute_query(connection, sql_query, params)
        display_mode = None
        if display_options.number_only and display_options.number_labels:
            display_mode = "labels"
        elif display_options.number_only:
            display_mode = "number"
        elif display_options.percent_only:
            display_mode = "percent"
        elif display_options.pair_only:
            display_mode = "pairs"

        display = format_display(rows, display_mode, display_options.number_labels)
        if display is not None:
            print(f"Answer: {display}")
        else:
            print(f"Answer: {rows}")

    except (RuntimeError, psycopg.Error, ValueError, TypeError) as e:
        print(f"Error: {e}")
        rows = []

    return rows


# Database configuration
def main():
    """Connect to the database and execute all configured queries."""
    try:
        conn = create_connection_from_env(psycopg.connect, create_connection, os.getenv)

        queries = get_queries()
        for query in queries:
            display_mode = query.get("display_mode")
            number_only = display_mode in {"number", "labels"}
            percent_only = display_mode == "percent"
            pair_only = display_mode == "pairs"
            run_query(
                conn,
                query["title"],
                query["sql"],
                query.get("params"),
                RunQueryOptions(
                    number_only=number_only,
                    percent_only=percent_only,
                    number_labels=query.get("display_labels"),
                    pair_only=pair_only,
                ),
            )

        if hasattr(conn, "close"):
            conn.close()
        print("\nConnection closed.")

    except (RuntimeError, psycopg.Error, OSError, ValueError, TypeError) as e:
        print(f"Failed to complete query run: {e}")


if __name__ == "__main__":  # pragma: no cover
    main()
