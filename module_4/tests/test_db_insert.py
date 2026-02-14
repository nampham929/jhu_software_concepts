from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

import blueprints.dashboard as dashboard
from flask_app import create_app
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


def test_insert_on_pull_writes_required_schema_rows(
    mock_create_connection,
    mock_db_connection,
    mock_db_url,
    mock_reset_applicants_table,
    fake_applicant_row,
    insert_row_tuple,
):
    mock_reset_applicants_table()

    def fake_pull_runner(progress_callback=None):
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

        return {
            "start_page": 1,
            "end_page": 1,
            "pages_scraped": 1,
            "processed": 1,
            "inserted": 1,
            "duplicates": 0,
            "missing_urls": 0,
            "errors": 0,
        }

    app = create_app(
        {
            "TESTING": True,
            "DATABASE_URL": mock_db_url,
            "RUN_PULL_IN_BACKGROUND": False,
            "PULL_RUNNER": fake_pull_runner,
        }
    )

    conn = dashboard.create_connection(database_url=mock_db_url)
    try:
        before_count = conn.execute("SELECT COUNT(*) FROM applicants;").fetchone()[0]
    finally:
        conn.close()

    with app.test_client() as client:
        response = client.post("/pull-data")

    assert response.status_code == 200
    assert response.get_json()["ok"] is True

    conn = dashboard.create_connection(database_url=mock_db_url)
    try:
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


def test_idempotency_duplicate_pull_does_not_duplicate_rows(
    mock_create_connection,
    mock_db_connection,
    mock_db_url,
    mock_reset_applicants_table,
    fake_applicant_row,
):
    mock_reset_applicants_table()
    entry = dict(fake_applicant_row)
    entry["comments"] = "idempotency test"
    entry["url"] = "https://example.test/unique-row"
    entry["GPA"] = "3.95"

    fake_scraper = SimpleNamespace(
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


def test_simple_query_function_returns_expected_schema_keys(
    mock_create_connection,
    mock_db_url,
    mock_reset_applicants_table,
    fake_applicant_row,
    insert_row_tuple,
):
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
    good = _LoadConn()
    load_data.create_applicants_table(good)
    assert good.commit_count == 1

    bad = _LoadConn(fail_execute=True)
    with pytest.raises(RuntimeError):
        load_data.create_applicants_table(bad)
    assert bad.rollback_count == 1


@pytest.mark.db
def test_parse_helpers_and_detect_encoding(tmp_path):
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
    load_data.load_data_from_jsonl(conn, str(jsonl_path))
    assert conn.commit_count >= 1
    assert len(conn.executed) == 1

    bad_jsonl_path = tmp_path / "bad_rows.jsonl"
    bad_jsonl_path.write_text("{not-json}\n", encoding="utf-8")
    conn_bad_json = _LoadConn()
    load_data.load_data_from_jsonl(conn_bad_json, str(bad_jsonl_path))

    conn_insert_fail = _LoadConn(fail_execute=True)
    load_data.load_data_from_jsonl(conn_insert_fail, str(jsonl_path))
    assert conn_insert_fail.rollback_count >= 1

    with pytest.raises(FileNotFoundError):
        load_data.load_data_from_jsonl(conn, str(tmp_path / "missing.jsonl"))


@pytest.mark.db
def test_load_data_main_success_and_failure(monkeypatch):
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
    monkeypatch.setattr(load_data, "create_connection", lambda *args, **kwargs: conn)
    monkeypatch.setattr(load_data, "create_applicants_table", lambda c: None)
    monkeypatch.setattr(load_data, "load_data_from_jsonl", lambda c, p: None)
    load_data.main()
    assert conn.closed is True

    def _raise(*args, **kwargs):
        raise RuntimeError("bad")

    monkeypatch.setattr(load_data, "create_connection", _raise)
    load_data.main()

@pytest.mark.db
def test_load_data_empty_line_commit_100_and_outer_exception(tmp_path, monkeypatch):
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
    lines.append("")
    jsonl_path.write_text("\n".join(lines), encoding="utf-8")

    conn = _LoadConn()
    load_data.load_data_from_jsonl(conn, str(jsonl_path))
    assert conn.commit_count >= 2

    monkeypatch.setattr(load_data, "detect_file_encoding", lambda path: (_ for _ in ()).throw(RuntimeError("enc fail")))
    with pytest.raises(RuntimeError):
        load_data.load_data_from_jsonl(conn, str(jsonl_path))

@pytest.mark.db
def test_pull_gradcafe_data_stop_url_branch_and_default_module_import(monkeypatch):
    import types
    import sys

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
    fake_module2 = types.ModuleType("module_2")
    fake_module2.scrape = fake_scrape
    fake_module2.clean = fake_clean

    monkeypatch.setitem(sys.modules, "module_2", fake_module2)
    monkeypatch.setattr(dashboard, "create_applicants_table", lambda conn: None)

    callbacks = []
    summary = dashboard.pull_gradcafe_data(
        progress_callback=lambda **kwargs: callbacks.append(kwargs),
        scraper_module=None,
        clean_module=None,
        connection_factory=connection_factory,
    )

    assert summary["pages_scraped"] == 1
    assert summary["processed"] == 1
    assert callbacks


@pytest.mark.db
def test_pull_gradcafe_data_insert_branches_progress_and_rollbacks(monkeypatch):
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
    assert write_conn.commit_count >= 2
    assert progress_calls


@pytest.mark.db
def test_ensure_module_2_on_path_adds_path_once():
    module_root = dashboard._ensure_module_2_on_path()
    assert module_root in dashboard.sys.path
    module_root_2 = dashboard._ensure_module_2_on_path()
    assert module_root_2 == module_root

@pytest.mark.db
def test_ensure_module_2_on_path_insertion_branch(monkeypatch):
    module_root = dashboard.os.path.abspath(
        dashboard.os.path.join(dashboard.os.path.dirname(dashboard.__file__), "..")
    )
    monkeypatch.setattr(dashboard.sys, "path", [p for p in dashboard.sys.path if p != module_root])
    added = dashboard._ensure_module_2_on_path()
    assert added == module_root
    assert dashboard.sys.path[0] == module_root


@pytest.mark.db
def test_load_data_skips_blank_line_explicitly(tmp_path):
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
    class _Conn:
        def __init__(self):
            self.closed = False

        def close(self):
            self.closed = True

    conn = _Conn()
    monkeypatch.setenv("DATABASE_URL", "postgresql://db.example.invalid:5432/dbname")
    monkeypatch.setattr(load_data.psycopg, "connect", lambda url: conn)
    monkeypatch.setattr(load_data, "create_applicants_table", lambda c: None)
    monkeypatch.setattr(load_data, "load_data_from_jsonl", lambda c, p: None)
    load_data.main()
    assert conn.closed is True


@pytest.mark.db
def test_load_data_main_missing_env_hits_runtime_branch(monkeypatch, capsys):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("DB_NAME", raising=False)
    monkeypatch.delenv("DB_USER", raising=False)
    monkeypatch.delenv("DB_PASSWORD", raising=False)
    monkeypatch.delenv("DB_HOST", raising=False)
    monkeypatch.delenv("DB_PORT", raising=False)

    load_data.main()
    out = capsys.readouterr().out
    assert "Failed to complete data loading: Database configuration missing." in out
