from __future__ import annotations

from bs4 import BeautifulSoup
import pytest

import blueprints.dashboard as dashboard
import query_data
from flask_app import create_app
from load_data import create_applicants_table


def _build_pull_runner(db_url: str, records: list[tuple]):

    def fake_pull_runner(progress_callback=None):
        conn = dashboard.create_connection(database_url=db_url)
        inserted = 0
        duplicates = 0
        try:
            create_applicants_table(conn)
            existing = {
                row[0]
                for row in conn.execute(
                    "SELECT url FROM applicants WHERE url IS NOT NULL AND url <> ''"
                ).fetchall()
            }
            for row in records:
                if row[3] in existing:
                    duplicates += 1
                    continue
                conn.execute(
                    """
                    INSERT INTO applicants (
                        program, comments, date_added, url, status, term,
                        us_or_international, gpa, gre, gre_v, gre_aw,
                        degree, llm_generated_program, llm_generated_university
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    row,
                )
                inserted += 1
                existing.add(row[3])
            conn.commit()
        finally:
            conn.close()

        if progress_callback:
            progress_callback(progress={"processed": len(records), "inserted": inserted})

        return {
            "start_page": 1,
            "end_page": 1,
            "pages_scraped": 1,
            "processed": len(records),
            "inserted": inserted,
            "duplicates": duplicates,
            "missing_urls": 0,
            "errors": 0,
        }

    return fake_pull_runner


def _analysis_results_from_db(db_url: str):
    conn = dashboard.create_connection(database_url=db_url)
    try:
        total = conn.execute("SELECT COUNT(*) FROM applicants;").fetchone()[0]
        accepted = conn.execute(
            "SELECT COUNT(*) FROM applicants WHERE LOWER(status) = 'accepted';"
        ).fetchone()[0]
    finally:
        conn.close()

    percent = (accepted / total * 100.0) if total else 0.0
    return [
        {
            "title": "Total Rows",
            "description": "Count",
            "sql": "SELECT COUNT(*)",
            "columns": ["count"],
            "rows": [[total]],
            "display": query_data.format_display([[total]], "number"),
            "error": None,
        },
        {
            "title": "Acceptance Rate",
            "description": "Percent",
            "sql": "SELECT ...",
            "columns": ["percent"],
            "rows": [[percent]],
            "display": query_data.format_display([[percent]], "percent"),
            "error": None,
        },
    ]


def test_end_to_end_pull_update_render(
    mock_create_connection,
    monkeypatch,
    mock_db_url,
    mock_reset_applicants_table,
    fake_pull_records,
):
    mock_reset_applicants_table()
    app = create_app(
        {
            "TESTING": True,
            "DATABASE_URL": mock_db_url,
            "RUN_PULL_IN_BACKGROUND": False,
            "PULL_RUNNER": _build_pull_runner(mock_db_url, fake_pull_records),
        }
    )

    monkeypatch.setattr(dashboard, "load_query_results", lambda: _analysis_results_from_db(mock_db_url))

    with app.test_client() as client:
        pull_resp = client.post("/pull-data")
        update_resp = client.post("/update-analysis")
        page_resp = client.get("/analysis")

    assert pull_resp.status_code == 200
    assert pull_resp.get_json()["ok"] is True
    assert update_resp.status_code == 200
    assert update_resp.get_json()["ok"] is True
    assert page_resp.status_code == 200

    html = page_resp.get_data(as_text=True)
    soup = BeautifulSoup(html, "html.parser")
    assert soup.find(string="Answer:") is not None
    assert "50.00%" in html


def test_multiple_pulls_with_overlapping_data_remain_consistent(
    mock_create_connection,
    mock_db_url,
    mock_reset_applicants_table,
    fake_pull_records,
):
    mock_reset_applicants_table()
    app = create_app(
        {
            "TESTING": True,
            "DATABASE_URL": mock_db_url,
            "RUN_PULL_IN_BACKGROUND": False,
            "PULL_RUNNER": _build_pull_runner(mock_db_url, fake_pull_records),
        }
    )

    with app.test_client() as client:
        first = client.post("/pull-data")
        second = client.post("/pull-data")

    assert first.status_code == 200
    assert second.status_code == 200

    conn = dashboard.create_connection(database_url=mock_db_url)
    try:
        count = conn.execute("SELECT COUNT(*) FROM applicants;").fetchone()[0]
    finally:
        conn.close()

    assert count == 2

