import psycopg
from psycopg import OperationalError


def get_queries():
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
            "description": "Calculates the percentage of entries where nationality is not American or Other.",
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
            "title": "Q7: Number of applicants who applied to JHU for a masters degree in Computer Science:",
            "description": "Counts entries with program including Johns Hopkins and Computer Science, and degree as masters.",
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
            "title": "Q8: Number of 2026 Computer Science PhD acceptances (Georgetown, MIT, Stanford, or CMU):",
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
            "display_mode": "number",
        },
        {
            "title": "Q9: 2026 Computer Science PhD acceptances using LLM fields:",
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
            "description": "Calculates acceptance rate for international students (not American/Other).",
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
    connection = None
    try:
        connection = psycopg.connect(
            dbname=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
        )
        print("Connection to PostgreSQL DB successful")
    except OperationalError as e:
        print(f"The error '{e}' occurred")
        raise
    return connection

def execute_query(connection, sql_query, params=None):
    """Execute a SQL query and return rows plus column names."""
    cursor = connection.execute(sql_query, params or ())
    rows = cursor.fetchall()
    columns = [col.name for col in cursor.description] if cursor.description else []
    return rows, columns


def format_display(rows, display_mode, display_labels=None):
    """Format query results for simple display modes."""
    if not rows:
        return None
    if display_mode == "number" and len(rows) == 1 and len(rows[0]) == 1:
        return str(rows[0][0])
    if display_mode == "percent" and len(rows) == 1 and len(rows[0]) == 1:
        return f"{rows[0][0]}%"
    if display_mode == "labels" and len(rows) == 1:
        labels = display_labels or []
        if labels and len(labels) == len(rows[0]):
            return ", ".join(
                f"{label}: {value}" for label, value in zip(labels, rows[0])
            )
    if display_mode == "pairs":
        return ", ".join(f"{row[0]}: {row[1]}" for row in rows if len(row) >= 2)
    return None


"""
Run queries loaded from queries.py one query at a time
and print results in a readable format.
"""
def run_query(
    connection,
    title,
    sql_query,
    params=None,
    number_only=False,
    percent_only=False,
    number_labels=None,
    pair_only=False,
):
    print(f"\n{title}")
    try:
        rows, _ = execute_query(connection, sql_query, params)
        display_mode = None
        if number_only and number_labels:
            display_mode = "labels"
        elif number_only:
            display_mode = "number"
        elif percent_only:
            display_mode = "percent"
        elif pair_only:
            display_mode = "pairs"

        display = format_display(rows, display_mode, number_labels)
        if display is not None:
            print(f"Answer: {display}")
        else:
            print(f"Answer: {rows}")

    except Exception as e:
        print(f"Error: {e}")
        rows = []

    return rows


def main():
    # Database configuration
    db_name = "postgres"
    db_user = "postgres"
    db_password = "dataBase!605"
    db_host = "localhost"
    db_port = "5432"

    try:
        conn = create_connection(db_name, db_user, db_password, db_host, db_port)

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
                number_only=number_only,
                percent_only=percent_only,
                number_labels=query.get("display_labels"),
                pair_only=pair_only,
            )

        conn.close()
        print("\nConnection closed.")

    except Exception as e:
        print(f"Failed to complete query run: {e}")


if __name__ == "__main__":
    main()
