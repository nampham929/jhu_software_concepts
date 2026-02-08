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


def run_query(
    connection,
    title,
    description,
    sql_query,
    params=None,
    number_only=False,
    percent_only=False,
    number_labels=None,
    pair_only=False,
):
    """Run a query and print the result with context."""
    print(f"\n{title}")
    try:
        cursor = connection.execute(sql_query, params or ())
        rows = cursor.fetchall()
        if number_only and rows and len(rows) == 1:
            row = rows[0]
            if len(row) == 1:
                print(f"Answer: {row[0]}")
            elif number_labels and len(number_labels) == len(row):
                labeled_values = ", ".join(
                    f"{label}: {value}" for label, value in zip(number_labels, row)
                )
                print(f"Answer: {labeled_values}")
            else:
                print(f"Answer: {', '.join(str(value) for value in row)}")
        elif pair_only and rows:
            formatted_rows = ", ".join(
                f"{row[0]}: {row[1]}" for row in rows if len(row) >= 2
            )
            print(f"Answer: {formatted_rows}")
        elif percent_only and rows and len(rows) == 1 and len(rows[0]) == 1:
            print(f"Answer: {rows[0][0]}%")
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

        

        # 1. How many entries for Fall 2026?
        run_query( 
            conn,
            "Q1: Number of entries for Fall 2026:",
            "Counts entries where term is exactly 'Fall 2026'.",
            """
            SELECT COUNT(*)
            FROM applicants
            WHERE term = 'Fall 2026';
            """,
            number_only=True,
        )

        # 2. Percentage of international students (not American or Other)
        run_query(
            conn,
            "Q2: Percentage of entries of international students:",
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
            percent_only=True,
        )

        # 3. Average GPA, GRE, GRE V, GRE AW of applicants who provide these metrics
        run_query(
            conn,
            "Q3: The average GPA, GRE, GRE V, GRE AW of applicants:",
            "Averages only rows where each metric is present.",
            """
            SELECT
                ROUND(AVG(gpa)::numeric, 2) AS avg_gpa,
                ROUND(AVG(gre)::numeric, 2) AS avg_gre,
                ROUND(AVG(gre_v)::numeric, 2) AS avg_gre_v,
                ROUND(AVG(gre_aw)::numeric, 2) AS avg_gre_aw
            FROM applicants
            WHERE gpa IS NOT NULL OR gre IS NOT NULL OR gre_v IS NOT NULL OR gre_aw IS NOT NULL;
            """,
            number_only=True,
            number_labels=["GPA", "GRE", "GRE V", "GRE AW"],
        )

        # 4. Average GPA of American students in Fall 2026
        run_query(
            conn,
            "Q4: The average GPA of American students in Fall 2026:",
            "Averages GPA for American students only, within Fall 2026 entries.",
            """
                        SELECT ROUND(AVG(gpa)::numeric, 2) AS avg_gpa_american_fall_2026
            FROM applicants
            WHERE term = 'Fall 2026'
              AND LOWER(us_or_international) = 'american'
              AND gpa IS NOT NULL
              AND gpa < 5.0 -- Exclude unrealistic GPAs to avoid skewing the average;
            """,
                        number_only=True,
        )

        # 5. Percent of Fall 2025 entries that are Acceptances
        run_query(
            conn,
            "Q5: Percentage of entries of Fall 2025 acceptances",
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
            percent_only=True,
        )

        # 6. Average GPA of Fall 2026 acceptances
        run_query(
            conn,
            "Q6: Average GPA of Fall 2026 acceptances:",
            "Averages GPA for accepted applicants in Fall 2026.",
            """
            SELECT ROUND(AVG(gpa)::numeric, 2) AS avg_gpa_fall_2026_accepts
            FROM applicants
            WHERE term = 'Fall 2026'
              AND LOWER(status) = 'accepted'
              AND gpa IS NOT NULL
              AND gpa < 5.0 -- Exclude unrealistic GPAs to avoid skewing the average;
            """,
                        number_only=True,
        )

        # 7. JHU masters in Computer Science
        run_query(
            conn,
            "Q7: Number of applicants who applied to JHU for a masters degree in Computer Science:",
            "Counts entries with program including Johns Hopkins and Computer Science, and degree as masters.",
            """
            SELECT COUNT(*)
            FROM applicants
                        WHERE program ILIKE %s
                            AND program ILIKE %s
                            AND degree ILIKE %s;
            """,
                        ("%Johns Hopkins%", "%Computer Science%", "Master%"),
            number_only=True,
        )

        # 8. 2026 acceptances for selected universities, PhD in CS
        run_query(
            conn,
            "Q8: Number of 2026 Computer Science PhD acceptances (Georgetown, MIT, Stanford, or CMU):",
            "Counts acceptances in 2026 for Georgetown, MIT, Stanford, or CMU in CS PhD.",
            """
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
            (
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
            number_only=True,
        )

        # 9. Same as Q8, using LLM generated fields
        run_query(
            conn,
            "Q9: 2026 Computer Science PhD acceptances using LLM fields:",
            "Counts acceptances in 2026 using llm_generated_university and llm_generated_program.",
            """
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
            (
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
            number_only=True,
        )

        # Additional question A
        run_query(
            conn,
            "Q10: Top 3 universities by total submissions:",
            "Shows the top 3 universities by count (using LLM generated university).",
            """
            SELECT llm_generated_university, COUNT(*) AS total_submissions
            FROM applicants
            WHERE llm_generated_university IS NOT NULL AND llm_generated_university <> ''
            GROUP BY llm_generated_university
            ORDER BY total_submissions DESC
            LIMIT 3;
            """,
            pair_only=True,
        )

        # Additional question B
        run_query(
            conn,
            "Q11: Acceptance rate for international students:",
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
            number_only=True,
        )

        conn.close()
        print("\nConnection closed.")

    except Exception as e:
        print(f"Failed to complete query run: {e}")


if __name__ == "__main__":
    main()
