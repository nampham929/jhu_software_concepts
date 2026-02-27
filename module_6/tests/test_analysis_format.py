from __future__ import annotations

import re
from types import SimpleNamespace

from bs4 import BeautifulSoup
import pytest

import blueprints.dashboard as dashboard
from flask_app import create_app
import query_data


class _FakeCursor:
    # Minimal cursor stand-in for tests that only need fetchall/description.
    def __init__(self, rows, columns):
        self._rows = rows
        self.description = [SimpleNamespace(name=name) for name in columns] if columns else None

    def fetchall(self):
        return self._rows


class _FakeConnection:
    # Minimal connection stand-in that tracks SQL calls made by query_data.
    def __init__(self, rows, columns):
        self.rows = rows
        self.columns = columns
        self.calls = []

    def execute(self, sql, params):
        self.calls.append((sql, params))
        return _FakeCursor(self.rows, self.columns)


@pytest.mark.analysis
def test_answer_labels_rendered_in_analysis_page(monkeypatch):
    # Ensure the analysis page renders an explicit "Answer:" label for query results.
    # Patch the dashboard loader so template rendering is deterministic.
    monkeypatch.setattr(
        dashboard,
        "load_query_results",
        lambda: [
            {
                "title": "Q2",
                "description": "desc",
                "sql": "SELECT 39.28",
                "columns": ["percent"],
                "rows": [[39.28]],
                "display": query_data.format_display([[39.28]], "percent"),
                "error": None,
            }
        ],
    )

    app = create_app({"TESTING": True})
    with app.test_client() as client:
        response = client.get("/analysis")

    assert response.status_code == 200
    html = response.get_data(as_text=True)
    soup = BeautifulSoup(html, "html.parser")
    assert soup.find(string=re.compile(r"Answer:")) is not None


@pytest.mark.analysis
def test_percentage_always_two_decimals():
    # Verify percent formatting normalizes to two decimal places, including None input.
    formatted = query_data.format_display([[39.2]], "percent")
    assert formatted == "39.20%"

    formatted_none = query_data.format_display([[None]], "percent")
    assert formatted_none == "0.00%"

    # Keep a regex assertion to guard against accidental format drift.
    assert re.search(r"\d+\.\d{2}%$", formatted)


@pytest.mark.analysis
def test_query_data_create_connection_success(monkeypatch):
    # Confirm create_connection returns the psycopg connection object on success.
    fake_conn = object()
    monkeypatch.setattr(query_data.psycopg, "connect", lambda **kwargs: fake_conn)
    conn = query_data.create_connection("d", "u", "p", "h", "5432")
    assert conn is fake_conn


@pytest.mark.analysis
def test_query_data_create_connection_failure(monkeypatch):
    # Confirm OperationalError is propagated when psycopg.connect fails.
    def _raise(**kwargs):
        raise query_data.OperationalError("boom")

    monkeypatch.setattr(query_data.psycopg, "connect", _raise)
    with pytest.raises(query_data.OperationalError):
        query_data.create_connection("d", "u", "p", "h", "5432")


@pytest.mark.analysis
def test_execute_query_returns_rows_and_columns():
    # Validate execute_query returns both row data and column names from cursor metadata.
    conn = _FakeConnection(rows=[(1, 2)], columns=["a", "b"])
    rows, columns = query_data.execute_query(conn, "SELECT 1", ())
    assert rows == [(1, 2)]
    assert columns == ["a", "b"]


@pytest.mark.analysis
def test_format_display_all_modes():
    # Cover each supported display mode and the unknown-mode fallback.
    assert query_data.format_display([], "number") is None
    assert query_data.format_display([[42]], "number") == "42"
    assert query_data.format_display([[39.2]], "percent") == "39.20%"
    assert query_data.format_display([[None]], "percent") == "0.00%"
    assert query_data.format_display([[3.8, 320]], "labels", ["GPA", "GRE"]) == "GPA: 3.8, GRE: 320"
    assert query_data.format_display([["JHU", 10], ["MIT", 8]], "pairs") == "JHU: 10, MIT: 8"
    assert query_data.format_display([[1, 2]], "unknown") is None


@pytest.mark.analysis
def test_run_query_success_and_error_paths(monkeypatch, capsys):
    # Validate both normal query execution and error-path behavior with log output.
    conn = _FakeConnection(rows=[(50.0,)], columns=["pct"])
    result = query_data.run_query(
        conn,
        "T",
        "SELECT",
        params=(),
        options=query_data.RunQueryOptions(percent_only=True),
    )
    assert result == [(50.0,)]

    # Replace execute_query with a failing stub to exercise exception handling.
    def _raise(*args, **kwargs):
        raise RuntimeError("bad")

    monkeypatch.setattr(query_data, "execute_query", _raise)
    result_error = query_data.run_query(conn, "T2", "SELECT", params=())
    assert result_error == []
    out = capsys.readouterr().out
    assert "Error:" in out


@pytest.mark.analysis
def test_query_data_main_success_and_failure(monkeypatch):
    # Exercise main() when connection succeeds and when setup raises an exception.
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("DB_NAME", "d")
    monkeypatch.setenv("DB_USER", "u")
    monkeypatch.setenv("DB_PASSWORD", "p")
    monkeypatch.setenv("DB_HOST", "h")
    monkeypatch.setenv("DB_PORT", "9")
    class _Conn:
        def __init__(self):
            self.closed = False

        def close(self):
            self.closed = True

    conn = _Conn()
    calls = {"run": 0}

    # Patch DB/query functions so this test focuses only on main() flow.
    monkeypatch.setattr(query_data, "create_connection", lambda *args, **kwargs: conn)
    monkeypatch.setattr(query_data, "get_queries", lambda: [{"title": "t", "sql": "s", "params": None, "display_mode": "number"}])
    monkeypatch.setattr(query_data, "run_query", lambda *args, **kwargs: calls.__setitem__("run", calls["run"] + 1))
    query_data.main()
    assert calls["run"] == 1
    assert conn.closed is True

    # Re-run main with a failing connection factory to cover fail-safe branch.
    def _raise(*args, **kwargs):
        raise RuntimeError("bad")

    monkeypatch.setattr(query_data, "create_connection", _raise)
    query_data.main()


@pytest.mark.analysis
def test_query_data_main_missing_env_prints_failure(monkeypatch, capsys):
    # Ensure missing DB environment variables trigger the expected failure message.
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("DB_NAME", raising=False)
    monkeypatch.delenv("DB_USER", raising=False)
    monkeypatch.delenv("DB_PASSWORD", raising=False)
    monkeypatch.delenv("DB_HOST", raising=False)
    monkeypatch.delenv("DB_PORT", raising=False)
    query_data.main()
    assert "Database configuration missing" in capsys.readouterr().out

@pytest.mark.analysis
def test_query_data_get_queries_and_run_query_modes():
    # Validate query catalog availability and run_query mode flags for number/pair paths.
    queries = query_data.get_queries()
    assert len(queries) >= 1

    conn = _FakeConnection(rows=[(1, 2)], columns=["a", "b"])
    result_number = query_data.run_query(
        conn, "Tn", "SELECT", options=query_data.RunQueryOptions(number_only=True)
    )
    assert result_number == [(1, 2)]

    result_pairs = query_data.run_query(
        conn, "Tp", "SELECT", options=query_data.RunQueryOptions(pair_only=True)
    )
    assert result_pairs == [(1, 2)]


@pytest.mark.analysis
def test_module2_clean_load_clean_save(tmp_path):
    # Verify module_2 clean pipeline loads, transforms, and saves sanitized JSON data.
    import module_2.clean as clean_mod

    input_path = tmp_path / "in.json"
    output_path = tmp_path / "out.json"
    input_path.write_text('[{"a": "<b>x</b>   y", "b": null, "c": "N/A", "d": 7}]', encoding="utf-8")

    loaded = clean_mod.load_data(str(input_path))
    # Pipeline under test: load -> clean -> save.
    cleaned = clean_mod.clean_data(loaded)
    clean_mod.save_data(cleaned, str(output_path))

    assert cleaned[0]["a"] == "x y"
    assert cleaned[0]["b"] == ""
    assert cleaned[0]["c"] == ""
    assert cleaned[0]["d"] == 7
    assert output_path.exists()


@pytest.mark.analysis
def test_module2_scrape_helpers_without_network(monkeypatch, tmp_path):
    # Exercise scrape helpers with mocked network/robots to keep test fully offline.
    import module_2.scrape as scrape_mod

    class FakeRP:
        def set_url(self, url):
            self.url = url

        def read(self):
            return None

        def can_fetch(self, ua, base):
            return True

    monkeypatch.setattr(scrape_mod.urllib.robotparser, "RobotFileParser", FakeRP)
    allowed = scrape_mod._check_robots_allowed("https://x")
    assert "ALLOWED" in allowed

    class FakeResp:
        # Context-manager shape mirrors urllib response objects used by the scraper.
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self):
            return b"<html></html>"

    monkeypatch.setattr(scrape_mod, "urlopen", lambda req: FakeResp())
    assert scrape_mod._fetch_html("https://x") == "<html></html>"

    # Simulate the paired-row structure seen in GradCafe tables.
    html = """
    <table><tbody>
      <tr>
        <td>Johns Hopkins University</td>
        <td><span>Computer Science</span><span>Masters</span></td>
        <td>February 10, 2026</td>
        <td>Accepted on February 1</td>
        <td><a href="/result/123">r</a></td>
      </tr>
      <tr class="tw-border-none"><td><p>great</p> Fall 2026 American GPA: 3.9 GRE: 330 V: 165 AW: 4.5</td></tr>
    </tbody></table>
    """
    parsed = scrape_mod._parse_page(html)
    assert len(parsed) == 1
    assert parsed[0]["status"] == "Accepted"
    assert parsed[0]["url"].endswith("/result/123")

    # Empty markup should parse to an empty result set.
    assert scrape_mod._parse_page("<html></html>") == []

    # Stub network/parser pieces to test scrape_data pagination behavior only.
    monkeypatch.setattr(scrape_mod, "_fetch_html", lambda url: "<html></html>")
    monkeypatch.setattr(scrape_mod, "_parse_page", lambda html: [{"x": 1}] if html else [])
    all_rows = scrape_mod.scrape_data(pages=2)
    assert len(all_rows) == 2

    out = tmp_path / "scrape.json"
    scrape_mod.save_data(all_rows, str(out))
    assert out.exists()


@pytest.mark.analysis
def test_query_data_run_query_labels_branch():
    # Cover run_query path that returns numeric tuples with provided labels.
    conn = _FakeConnection(rows=[(3.9, 330)], columns=["gpa", "gre"])
    result = query_data.run_query(
        conn,
        "Tl",
        "SELECT",
        params=(),
        options=query_data.RunQueryOptions(
            number_only=True,
            number_labels=["GPA", "GRE"],
        ),
    )
    assert result == [(3.9, 330)]


@pytest.mark.analysis
def test_module2_scrape_disallowed_and_rejected_branches(monkeypatch):
    # Cover robots disallow handling and rejected-row parsing branch.
    import module_2.scrape as scrape_mod

    class FakeRP:
        def set_url(self, url):
            self.url = url

        def read(self):
            return None

        def can_fetch(self, ua, base):
            return False

    monkeypatch.setattr(scrape_mod.urllib.robotparser, "RobotFileParser", FakeRP)
    disallowed = scrape_mod._check_robots_allowed("https://x")
    assert "DISALLOWED" in disallowed

    # Include rows that should be skipped before a valid rejected-row payload.
    html = """
    <table><tbody>
      <tr class="tw-border-none"><td>skip row</td></tr>
      <tr><td>Only one col</td></tr>
      <tr>
        <td>MIT</td>
        <td><span>Computer Science</span></td>
        <td>February 10, 2026</td>
        <td>Rejected on February 9</td>
        <td><a href="/result/222">r</a></td>
      </tr>
      <tr class="tw-border-none"><td><p>note</p> Fall 2026 International GPA: 3.7 GRE: 325 V: 160 AW: 4.0</td></tr>
    </tbody></table>
    """
    parsed = scrape_mod._parse_page(html)
    assert len(parsed) == 1
    assert parsed[0]["status"] == "Rejected"
    assert parsed[0]["US/International"] == "International"


@pytest.mark.analysis
def test_module2_run_main_imported_as_package(monkeypatch):
    # Validate module_2.run.main works when imported as a package with patched dependencies.
    import importlib
    import module_2.clean as clean_mod
    import module_2.scrape as scrape_mod
    import sys

    monkeypatch.setitem(sys.modules, "clean", clean_mod)
    monkeypatch.setitem(sys.modules, "scrape", scrape_mod)

    # Import after sys.modules patch so module_2.run resolves these names.
    run_mod = importlib.import_module("module_2.run")

    monkeypatch.setattr(run_mod.scrape, "_check_robots_allowed", lambda base: "ok")
    # Patch I/O-heavy routines so main() runs as a pure unit test.
    monkeypatch.setattr(run_mod.scrape, "scrape_data", lambda pages: [{"a": 1}])
    monkeypatch.setattr(run_mod.scrape, "save_data", lambda data, filename: None)
    monkeypatch.setattr(run_mod.clean, "load_data", lambda filename: [{"a": 1}])
    monkeypatch.setattr(run_mod.clean, "clean_data", lambda data: data)
    monkeypatch.setattr(run_mod.clean, "save_data", lambda data, filename: None)

    run_mod.main()



@pytest.mark.analysis
def test_query_data_main_uses_database_url(monkeypatch):
    # Ensure main() prefers DATABASE_URL direct connection path when provided.
    class _Conn:
        def __init__(self):
            self.closed = False

        def close(self):
            self.closed = True

    conn = _Conn()
    monkeypatch.setenv("DATABASE_URL", "postgresql://db.example.invalid:5432/dbname")
    # Stub direct-url connect path used by create_connection_from_env.
    monkeypatch.setattr(query_data.psycopg, "connect", lambda url: conn)
    monkeypatch.setattr(query_data, "get_queries", lambda: [])
    query_data.main()
    assert conn.closed is True


@pytest.mark.analysis
def test_query_limit_clamping_and_param_fallbacks():
    # Ensure LIMIT values are clamped and params fallback works for legacy/non-limit query maps.
    assert query_data.clamp_query_limit(0) == 1
    assert query_data.clamp_query_limit(999) == 100
    assert query_data.clamp_query_limit("abc", default=7) == 7

    assert query_data.get_query_params({"params": ("x",), "limit": 500}) == ("x", 100)
    assert query_data.get_query_params({"params": ("x",)}) == ("x",)
