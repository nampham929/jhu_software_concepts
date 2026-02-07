import psycopg
from psycopg import OperationalError


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
        print("Connection to PostgreSQL DB successful")
    except OperationalError as e:
        print(f"The error '{e}' occurred")
        raise
    return connection


def run_query(connection, title, description, sql_query, params=None):
    """Run a query and print the result with context."""
    print("\n" + "=" * 80)
    print(title)
    print(description)
    print("SQL:")
    print(sql_query.strip())
    try:
        cursor = connection.execute(sql_query, params or ())
        rows = cursor.fetchall()
        print("Result:")
        for row in rows:
            print(row)
    except Exception as e:
        print(f"Query failed: {e}")


def main():
    # Database configuration
    db_name = "postgres"
    db_user = "postgres"
    db_password = "dataBase!605"
    db_host = "localhost"
    db_port = "5432"

    try:
        conn = create_connection(db_name, db_user, db_password, db_host, db_port)

        # 1. How many entries for Fall 2026?
        run_query(
            conn,
            "Q1: Entries for Fall 2026",
            "Counts entries where term is exactly 'Fall 2026'.",
            """
            SELECT COUNT(*)
            FROM applicants
            WHERE term = 'Fall 2026';
            """,
        )

        # 2. Percentage of international students (not American or Other)
        run_query(
            conn,
            "Q2: Percent international (not American/Other)",
            "Calculates the percentage of entries where nationality is not American or Other.",
            """
            SELECT ROUND(
                100.0 * SUM(CASE WHEN LOWER(us_or_international) NOT IN ('american', 'other') THEN 1 ELSE 0 END)
                / NULLIF(COUNT(*), 0),
                2
            ) AS percent_international
            FROM applicants
            WHERE us_or_international IS NOT NULL AND us_or_international <> '';
            """,
        )

        # 3. Average GPA, GRE, GRE V, GRE AW of applicants who provide these metrics
        run_query(
            conn,
            "Q3: Average GPA/GRE metrics",
            "Averages only rows where each metric is present.",
            """
            SELECT
                AVG(gpa) AS avg_gpa,
                AVG(gre) AS avg_gre,
                AVG(gre_v) AS avg_gre_v,
                AVG(gre_aw) AS avg_gre_aw
            FROM applicants
            WHERE gpa IS NOT NULL OR gre IS NOT NULL OR gre_v IS NOT NULL OR gre_aw IS NOT NULL;
            """,
        )

        # 4. Average GPA of American students in Fall 2026
        run_query(
            conn,
            "Q4: Average GPA of American students in Fall 2026",
            "Averages GPA for American students only, within Fall 2026 entries.",
            """
            SELECT AVG(gpa) AS avg_gpa_american_fall_2026
            FROM applicants
            WHERE term = 'Fall 2026'
              AND LOWER(us_or_international) = 'american'
              AND gpa IS NOT NULL;
            """,
        )

        # 5. Percent of Fall 2025 entries that are Acceptances
        run_query(
            conn,
            "Q5: Percent of Fall 2025 acceptances",
            "Calculates percentage of Fall 2025 entries with status 'Accepted'.",
            """
            SELECT ROUND(
                100.0 * SUM(CASE WHEN LOWER(status) = 'accepted' THEN 1 ELSE 0 END)
                / NULLIF(COUNT(*), 0),
                2
            ) AS percent_accept_fall_2025
            FROM applicants
            WHERE term = 'Fall 2025';
            """,
        )

        # 6. Average GPA of Fall 2026 acceptances
        run_query(
            conn,
            "Q6: Average GPA of Fall 2026 acceptances",
            "Averages GPA for accepted applicants in Fall 2026.",
            """
            SELECT AVG(gpa) AS avg_gpa_fall_2026_accepts
            FROM applicants
            WHERE term = 'Fall 2026'
              AND LOWER(status) = 'accepted'
              AND gpa IS NOT NULL;
            """,
        )

        # 7. JHU masters in Computer Science
        run_query(
            conn,
            "Q7: JHU masters in Computer Science",
            "Counts entries with program including Johns Hopkins and Computer Science, and degree as masters.",
            """
            SELECT COUNT(*)
            FROM applicants
            WHERE program ILIKE '%Johns Hopkins%'
              AND program ILIKE '%Computer Science%'
              AND degree ILIKE 'Master%';
            """,
        )

        # 8. 2026 acceptances for selected universities, PhD in CS
        run_query(
            conn,
            "Q8: 2026 CS PhD acceptances (selected universities)",
            "Counts acceptances in 2026 for Georgetown, MIT, Stanford, or CMU in CS PhD.",
            """
            SELECT COUNT(*)
            FROM applicants
            WHERE term LIKE '%2026%'
              AND LOWER(status) = 'accepted'
              AND degree ILIKE 'PhD%'
              AND program ILIKE '%Computer Science%'
              AND (
                  program ILIKE '%Georgetown University%'
                  OR program ILIKE '%Massachusetts Institute of Technology%'
                  OR program ILIKE '%MIT%'
                  OR program ILIKE '%Stanford University%'
                  OR program ILIKE '%Carnegie Mellon University%'
                  OR program ILIKE '%CMU%'
              );
            """,
        )

        # 9. Same as Q8, using LLM generated fields
        run_query(
            conn,
            "Q9: 2026 CS PhD acceptances using LLM fields",
            "Counts acceptances in 2026 using llm_generated_university and llm_generated_program.",
            """
            SELECT COUNT(*)
            FROM applicants
            WHERE term LIKE '%2026%'
              AND LOWER(status) = 'accepted'
              AND degree ILIKE 'PhD%'
              AND llm_generated_program ILIKE '%Computer Science%'
              AND llm_generated_university IN (
                  'Georgetown University',
                  'Massachusetts Institute of Technology',
                  'Stanford University',
                  'Carnegie Mellon University'
              );
            """,
        )

        # Additional question A
        run_query(
            conn,
            "Q10: Top 5 universities by total submissions",
            "Shows the top 5 universities by count (using LLM generated university).",
            """
            SELECT llm_generated_university, COUNT(*) AS total_submissions
            FROM applicants
            WHERE llm_generated_university IS NOT NULL AND llm_generated_university <> ''
            GROUP BY llm_generated_university
            ORDER BY total_submissions DESC
            LIMIT 5;
            """,
        )

        # Additional question B
        run_query(
            conn,
            "Q11: Acceptance rate for international students",
            "Calculates acceptance rate for international students (not American/Other).",
            """
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
        )

        conn.close()
        print("\nConnection closed.")

    except Exception as e:
        print(f"Failed to complete query run: {e}")


if __name__ == "__main__":
    main()
