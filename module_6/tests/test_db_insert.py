from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

import app.blueprints.dashboard as dashboard
from app.flask_app import create_app
import load_data
from load_data import create_applicants_table


REQUIRED_FIELDS = [
    "program",
    "comments",
    "date_added",
    "url",
    "status",
    "term",
    "us_or_international",
    "gpa",
    "gre",
    "gre_v",
    "gre_aw",
    "degree",
    "llm_generated_program",
    "llm_generated_university",
]


class _LoadConn:
    # Lightweight connection double for load_data unit tests.
    def __init__(self, fail_execute=False):
        self.fail_execute = fail_execute
        self.executed = []
        self.commit_count = 0
        self.rollback_count = 0

    def execute(self, sql, params=None):
        if self.fail_execute:
            raise RuntimeError("execute failed")
        self.executed.append((sql, params))

    def commit(self):
        self.commit_count += 1

    def rollback(self):
        self.rollback_count += 1


@pytest.mark.db
def test_insert_on_pull_writes_required_schema_rows(
    mock_create_connection,
    mock_db_connection,
    mock_db_url,
    mock_reset_applicants_table,
    fake_applicant_row,
    insert_row_tuple,
    monkeypatch,
):
    # Ensure a pull inserts at least one row and populates all required schema columns.
    mock_reset_applicants_table()

    # Simulate the worker side by handling the queued pull immediately in test.
    def fake_publish_task(kind, payload=None):
        assert kind == "scrape_new_data"
        conn = dashboard.create_connection(database_url=mock_db_url)
        try:
            create_applicants_table(conn)
            conn.execute(
                """
                INSERT INTO applicants (
                    program, comments, date_added, url, status, term,
                    us_or_international, gpa, gre, gre_v, gre_aw,
                    degree, llm_generated_program, llm_generated_university
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                insert_row_tuple(fake_applicant_row),
            )
            conn.commit()
        finally:
            conn.close()

    app = create_app(
        {
            "TESTING": True,
            "DATABASE_URL": mock_db_url,
        }
    )
    monkeypatch.setattr(dashboard, "publish_task", fake_publish_task)
    monkeypatch.setattr(dashboard, "_set_job_status", lambda *args, **kwargs: None)

    conn = dashboard.create_connection(database_url=mock_db_url)
    try:
        # Baseline before triggering the route handler.
        before_count = conn.execute("SELECT COUNT(*) FROM applicants;").fetchone()[0]
    finally:
        conn.close()

    with app.test_client() as client:
        response = client.post("/pull-data")

    assert response.status_code == 202
    assert response.get_json()["ok"] is True

    conn = dashboard.create_connection(database_url=mock_db_url)
    try:
        # Re-read row count and one sample row after route execution.
        after_count = conn.execute("SELECT COUNT(*) FROM applicants;").fetchone()[0]
        row = conn.execute(
            """
            SELECT
                program, comments, date_added, url, status, term,
                us_or_international, gpa, gre, gre_v, gre_aw,
                degree, llm_generated_program, llm_generated_university
            FROM applicants
            LIMIT 1;
            """
        ).fetchone()
    finally:
        conn.close()

    assert before_count == 0
    assert after_count >= 1
    assert row is not None
    for value in row:
        assert value is not None


@pytest.mark.db
def test_idempotency_duplicate_pull_does_not_duplicate_rows(
    mock_create_connection,
    mock_db_connection,
    mock_db_url,
    mock_reset_applicants_table,
    fake_applicant_row,
):
    # Ensure repeated pulls of the same source data do not create duplicate inserts.
    mock_reset_applicants_table()
    entry = dict(fake_applicant_row)
    entry["comments"] = "idempotency test"
    entry["url"] = "https://example.test/unique-row"
    entry["GPA"] = "3.95"

    fake_scraper = SimpleNamespace(
        # Return one page of data, then stop to emulate finite scrape pagination.
        BASE_URL="https://fake.local/survey",
        _fetch_html=lambda url: url,
        _parse_page=lambda html: [entry] if dashboard._extract_page_number(html) == 1 else [],
    )
    fake_clean = SimpleNamespace(clean_data=lambda rows: rows)
    connection_factory = lambda: dashboard.create_connection(database_url=mock_db_url)

    summary_1 = dashboard.pull_gradcafe_data(
        scraper_module=fake_scraper,
        clean_module=fake_clean,
        connection_factory=connection_factory,
    )
    summary_2 = dashboard.pull_gradcafe_data(
        scraper_module=fake_scraper,
        clean_module=fake_clean,
        connection_factory=connection_factory,
    )

    conn = dashboard.create_connection(database_url=mock_db_url)
    try:
        total_rows = conn.execute("SELECT COUNT(*) FROM applicants;").fetchone()[0]
    finally:
        conn.close()

    assert summary_1["inserted"] == 1
    assert summary_2["inserted"] == 0
    assert total_rows == 1


@pytest.mark.db
def test_simple_query_function_returns_expected_schema_keys(
    mock_create_connection,
    mock_db_url,
    mock_reset_applicants_table,
    fake_applicant_row,
    insert_row_tuple,
):
    # Ensure fetch-by-URL returns a dictionary with the expected applicant schema keys.
    mock_reset_applicants_table()
    entry = dict(fake_applicant_row)
    entry["comments"] = "query test"
    entry["url"] = "https://example.test/query-row"
    entry["GPA"] = "3.8"
    entry["GRE_SCORE"] = "328"
    entry["GRE_V"] = "162"
    entry["GRE_AW"] = "4.0"

    conn = dashboard.create_connection(database_url=mock_db_url)
    try:
        # Seed one row directly, then read it through the dashboard query helper.
        conn.execute(
            """
            INSERT INTO applicants (
                program, comments, date_added, url, status, term,
                us_or_international, gpa, gre, gre_v, gre_aw,
                degree, llm_generated_program, llm_generated_university
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            insert_row_tuple(entry),
        )
        conn.commit()

        row_dict = dashboard.fetch_applicant_row_by_url(
            conn,
            "https://example.test/query-row",
        )
    finally:
        conn.close()

    assert row_dict is not None
    assert set(REQUIRED_FIELDS).issubset(set(row_dict.keys()))


@pytest.mark.db
def test_load_data_create_connection_success_and_failure(monkeypatch):
    # Ensure load_data.create_connection succeeds on good config and raises on connect errors.
    fake_conn = object()
    monkeypatch.setattr(load_data.psycopg, "connect", lambda **kwargs: fake_conn)
    assert load_data.create_connection("d", "u", "p", "h", "5432") is fake_conn

    def _raise(**kwargs):
        raise load_data.OperationalError("bad")

    monkeypatch.setattr(load_data.psycopg, "connect", _raise)
    with pytest.raises(load_data.OperationalError):
        load_data.create_connection("d", "u", "p", "h", "5432")


@pytest.mark.db
def test_create_applicants_table_success_and_error():
    # Ensure table creation commits on success and rolls back when execution fails.
    good = _LoadConn()
    load_data.create_applicants_table(good)
    assert good.commit_count == 1

    bad = _LoadConn(fail_execute=True)
    with pytest.raises(RuntimeError):
        load_data.create_applicants_table(bad)
    assert bad.rollback_count == 1


@pytest.mark.db
def test_create_ingestion_watermarks_table_success_and_error():
    # Ensure watermark table creation commits on success and rolls back on failure.
    good = _LoadConn()
    load_data.create_ingestion_watermarks_table(good)
    assert good.commit_count == 1

    bad = _LoadConn(fail_execute=True)
    with pytest.raises(RuntimeError):
        load_data.create_ingestion_watermarks_table(bad)
    assert bad.rollback_count == 1


@pytest.mark.db
def test_parse_helpers_and_detect_encoding(tmp_path):
    # Validate parsing helpers and BOM/encoding detection across common file encodings.
    assert load_data.parse_date("January 15, 2026") == "2026-01-15"
    assert load_data.parse_date("not-a-date") is None
    assert load_data.parse_date("") is None

    assert load_data.parse_float("3.5") == 3.5
    assert load_data.parse_float("") is None
    assert load_data.parse_float("x") is None

    utf16_file = tmp_path / "utf16.jsonl"
    utf16_file.write_bytes(b"\xff\xfea\x00")
    assert load_data.detect_file_encoding(str(utf16_file)) == "utf-16"

    utf8sig_file = tmp_path / "utf8sig.jsonl"
    utf8sig_file.write_bytes(b"\xef\xbb\xbf{")
    assert load_data.detect_file_encoding(str(utf8sig_file)) == "utf-8-sig"

    utf8_file = tmp_path / "utf8.jsonl"
    utf8_file.write_text("{}", encoding="utf-8")
    assert load_data.detect_file_encoding(str(utf8_file)) == "utf-8"


@pytest.mark.db
def test_load_data_from_jsonl_success_and_error_paths(tmp_path):
    # Cover JSONL happy path plus malformed JSON, insert failure, and missing-file branches.
    jsonl_path = tmp_path / "rows.jsonl"
    row = {
        "program": "Computer Science, JHU",
        "comments": "c",
        "date_added": "January 15, 2026",
        "url": "https://example.test/a",
        "status": "Accepted",
        "term": "Fall 2026",
        "US/International": "American",
        "GPA": "3.8",
        "GRE_SCORE": "320",
        "GRE_V": "160",
        "GRE_AW": "4.0",
        "Degree": "Masters",
        "llm-generated-program": "Computer Science",
        "llm-generated-university": "Johns Hopkins University",
    }
    jsonl_path.write_text(json.dumps(row) + "\n", encoding="utf-8")

    conn = _LoadConn()
    # Happy path: one valid JSON line should yield one insert.
    load_data.load_data_from_jsonl(conn, str(jsonl_path))
    assert conn.commit_count >= 1
    assert len(conn.executed) == 1

    bad_jsonl_path = tmp_path / "bad_rows.jsonl"
    bad_jsonl_path.write_text("{not-json}\n", encoding="utf-8")
    conn_bad_json = _LoadConn()
    # Malformed JSON should be reported but not crash the loader.
    load_data.load_data_from_jsonl(conn_bad_json, str(bad_jsonl_path))

    conn_insert_fail = _LoadConn(fail_execute=True)
    # Insert exceptions should trigger rollback handling inside insert loop.
    load_data.load_data_from_jsonl(conn_insert_fail, str(jsonl_path))
    assert conn_insert_fail.rollback_count >= 1

    with pytest.raises(FileNotFoundError):
        load_data.load_data_from_jsonl(conn, str(tmp_path / "missing.jsonl"))


@pytest.mark.db
def test_load_data_main_success_and_failure(monkeypatch):
    # Ensure load_data.main handles both successful setup and top-level failure path.
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("DB_NAME", "d")
    monkeypatch.setenv("DB_USER", "u")
    monkeypatch.setenv("DB_PASSWORD", "p")
    monkeypatch.setenv("DB_HOST", "h")
    monkeypatch.setenv("DB_PORT", "9")
    class _MainConn:
        def __init__(self):
            self.closed = False

        def close(self):
            self.closed = True

    conn = _MainConn()
    # Isolate main() control flow by stubbing table creation and load operations.
    monkeypatch.setattr(load_data, "create_connection", lambda *args, **kwargs: conn)
    monkeypatch.setattr(load_data, "create_applicants_table", lambda c: None)
    monkeypatch.setattr(load_data, "create_ingestion_watermarks_table", lambda c: None)
    monkeypatch.setattr(load_data, "load_data_from_jsonl", lambda c, p: None)
    load_data.main()
    assert conn.closed is True

    def _raise(*args, **kwargs):
        raise RuntimeError("bad")

    monkeypatch.setattr(load_data, "create_connection", _raise)
    load_data.main()

@pytest.mark.db
def test_load_data_empty_line_commit_100_and_outer_exception(tmp_path, monkeypatch):
    # Ensure batch commits occur and outer exceptions from encoding detection are propagated.
    jsonl_path = tmp_path / "bulk.jsonl"
    row = {
        "program": "P",
        "comments": "C",
        "date_added": "January 15, 2026",
        "url": "https://example.test/a",
        "status": "Accepted",
        "term": "Fall 2026",
        "US/International": "American",
        "GPA": "3.8",
        "GRE_SCORE": "320",
        "GRE_V": "160",
        "GRE_AW": "4.0",
        "Degree": "Masters",
        "llm-generated-program": "Computer Science",
        "llm-generated-university": "JHU",
    }
    lines = [json.dumps(row) for _ in range(100)]
    # Explicit blank line exercises skip-empty-line branch in JSONL iterator.
    lines.append("")
    jsonl_path.write_text("\n".join(lines), encoding="utf-8")

    conn = _LoadConn()
    load_data.load_data_from_jsonl(conn, str(jsonl_path))
    # Expect at least one periodic commit plus final commit.
    assert conn.commit_count >= 2

    # Force detect_file_encoding failure to cover outer exception propagation path.
    monkeypatch.setattr(load_data, "detect_file_encoding", lambda path: (_ for _ in ()).throw(RuntimeError("enc fail")))
    with pytest.raises(RuntimeError):
        load_data.load_data_from_jsonl(conn, str(jsonl_path))

@pytest.mark.db
def test_pull_gradcafe_data_stop_url_branch_and_default_module_import(monkeypatch):
    # Cover stop-URL short-circuit logic and default built-in helper wiring.
    import types

    class _Cursor:
        def __init__(self, rows=None, row=None):
            self._rows = rows or []
            self._row = row

        def fetchall(self):
            return self._rows

        def fetchone(self):
            return self._row

    class _ConnRead:
        def execute(self, sql, params=None):
            if "ORDER BY date_added" in sql:
                return _Cursor(row=("https://example.test/stop",))
            return _Cursor(rows=[])

        def close(self):
            return None

    class _ConnWrite:
        def __init__(self):
            self.rows = []

        def execute(self, sql, params=None):
            self.rows.append((sql, params))
            return _Cursor()

        def commit(self):
            return None

        def rollback(self):
            return None

        def close(self):
            return None

    read_conn = _ConnRead()
    write_conn = _ConnWrite()
    # pull_gradcafe_data opens two short-lived connections: read context then write inserts.
    pool = [read_conn, write_conn]

    def connection_factory():
        return pool.pop(0)

    fake_scrape = types.SimpleNamespace(
        BASE_URL="https://fake.local/survey",
        _fetch_html=lambda url: url,
        _parse_page=lambda html: [
            {"url": "https://example.test/new-1", "date_added": "January 15, 2026"},
            {"url": "https://example.test/stop", "date_added": "January 15, 2026"},
        ],
    )
    fake_clean = types.SimpleNamespace(clean_data=lambda rows: rows)
    monkeypatch.setattr(dashboard, "scrape_support", fake_scrape)
    monkeypatch.setattr(dashboard, "data_cleaning", fake_clean)
    monkeypatch.setattr(dashboard, "create_applicants_table", lambda conn: None)

    callbacks = []
    summary = dashboard.pull_gradcafe_data(
        progress_callback=lambda **kwargs: callbacks.append(kwargs),
        scraper_module=None,
        clean_module=None,
        connection_factory=connection_factory,
    )

    assert summary["pages_scraped"] == 1
    # stop_url should truncate one page payload to only unseen leading rows.
    assert summary["processed"] == 1
    assert callbacks
@pytest.mark.db
def test_pull_gradcafe_data_insert_branches_progress_and_rollbacks(monkeypatch):
    # Cover insert, duplicate, missing-url, error, progress, commit, and rollback branches.
    class _Cursor:
        def __init__(self, rows=None, row=None):
            self._rows = rows or []
            self._row = row

        def fetchall(self):
            return self._rows

        def fetchone(self):
            return self._row

    class _ConnRead:
        def execute(self, sql, params=None):
            if "ORDER BY date_added" in sql:
                return _Cursor(row=None)
            return _Cursor(rows=[("https://example.test/dup",)])

        def close(self):
            return None

    class _ConnWrite:
        def __init__(self):
            self.commit_count = 0
            self.rollback_count = 0
            self.inserted = []

        def execute(self, sql, params=None):
            # Raise once for a sentinel URL to drive rollback/error accounting branch.
            if "INSERT INTO applicants" in sql:
                url = params[3]
                if url == "https://example.test/error":
                    raise RuntimeError("insert fail")
                self.inserted.append(url)
            return _Cursor()

        def commit(self):
            self.commit_count += 1

        def rollback(self):
            self.rollback_count += 1

        def close(self):
            return None

    read_conn = _ConnRead()
    write_conn = _ConnWrite()
    pool = [read_conn, write_conn]

    def connection_factory():
        return pool.pop(0)

    rows = []
    # 101 rows trigger periodic commit logic at index 100.
    for i in range(101):
        rows.append(
            {
                "program": "P",
                "comments": "C",
                "date_added": "January 15, 2026",
                "url": f"https://example.test/{i}",
                "status": "Accepted",
                "term": "Fall 2026",
                "US/International": "American",
                "GPA": "3.8",
                "GRE_SCORE": "320",
                "GRE_V": "160",
                "GRE_AW": "4.0",
                "Degree": "Masters",
            }
        )
    rows.extend(
        [
            # Explicit edge rows for missing URL, duplicate URL, and failing insert.
            {"url": ""},
            {"url": "https://example.test/dup"},
            {
                "program": "P",
                "comments": "C",
                "date_added": "January 15, 2026",
                "url": "https://example.test/error",
                "status": "Accepted",
                "term": "Fall 2026",
                "US/International": "American",
                "GPA": "3.8",
                "GRE_SCORE": "320",
                "GRE_V": "160",
                "GRE_AW": "4.0",
                "Degree": "Masters",
            },
        ]
    )

    fake_scrape = SimpleNamespace(
        BASE_URL="https://fake.local/survey",
        _fetch_html=lambda url: url,
        _parse_page=lambda html: rows if dashboard._extract_page_number(html) == 1 else [],
    )
    fake_clean = SimpleNamespace(clean_data=lambda data: data)

    monkeypatch.setattr(dashboard, "create_applicants_table", lambda conn: None)

    progress_calls = []
    summary = dashboard.pull_gradcafe_data(
        progress_callback=lambda **kwargs: progress_calls.append(kwargs),
        scraper_module=fake_scrape,
        clean_module=fake_clean,
        connection_factory=connection_factory,
    )

    assert summary["missing_urls"] >= 1
    assert summary["duplicates"] >= 1
    assert summary["errors"] >= 1
    assert write_conn.rollback_count >= 1
    # At least one batch commit and one final commit are expected.
    assert write_conn.commit_count >= 2
    assert progress_calls



@pytest.mark.db
def test_load_data_skips_blank_line_explicitly(tmp_path):
    # Ensure blank lines in JSONL input are skipped without affecting valid inserts.
    jsonl_path = tmp_path / "blank_line.jsonl"
    row = {
        "program": "P",
        "comments": "C",
        "date_added": "January 15, 2026",
        "url": "https://example.test/b",
        "status": "Accepted",
        "term": "Fall 2026",
        "US/International": "American",
        "GPA": "3.8",
        "GRE_SCORE": "320",
        "GRE_V": "160",
        "GRE_AW": "4.0",
        "Degree": "Masters",
        "llm-generated-program": "Computer Science",
        "llm-generated-university": "JHU",
    }
    jsonl_path.write_text(json.dumps(row) + "\n\n" + json.dumps(row) + "\n", encoding="utf-8")

    conn = _LoadConn()
    load_data.load_data_from_jsonl(conn, str(jsonl_path))
    assert len(conn.executed) == 2


@pytest.mark.db
def test_load_data_main_uses_database_url(monkeypatch):
    # Ensure load_data.main prefers DATABASE_URL when that environment variable is set.
    class _Conn:
        def __init__(self):
            self.closed = False

        def close(self):
            self.closed = True

    conn = _Conn()
    monkeypatch.setenv("DATABASE_URL", "postgresql://db.example.invalid:5432/dbname")
    monkeypatch.setattr(load_data.psycopg, "connect", lambda url: conn)
    monkeypatch.setattr(load_data, "create_applicants_table", lambda c: None)
    monkeypatch.setattr(load_data, "create_ingestion_watermarks_table", lambda c: None)
    monkeypatch.setattr(load_data, "load_data_from_jsonl", lambda c, p: None)
    load_data.main()
    assert conn.closed is True


@pytest.mark.db
def test_load_data_main_missing_env_hits_runtime_branch(monkeypatch, capsys):
    # Ensure missing DB env vars hit the runtime failure branch with expected output text.
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("DB_NAME", raising=False)
    monkeypatch.delenv("DB_USER", raising=False)
    monkeypatch.delenv("DB_PASSWORD", raising=False)
    monkeypatch.delenv("DB_HOST", raising=False)
    monkeypatch.delenv("DB_PORT", raising=False)

    load_data.main()
    out = capsys.readouterr().out
    assert "Failed to complete data loading: Database configuration missing." in out

