from __future__ import annotations

import pytest

import app.blueprints.dashboard as dashboard
from app.flask_app import create_app


@pytest.fixture
def app(monkeypatch):
    # Provide a stable one-query payload so template rendering is predictable in tests.
    monkeypatch.setattr(
        dashboard,
        "load_query_results",
        lambda: [
            {
                "title": "Q",
                "description": "d",
                "sql": "SELECT 1",
                "columns": ["v"],
                "rows": [[1]],
                "display": "1",
                "error": None,
            }
        ],
    )

    dashboard._set_pull_in_progress(False)
    dashboard._update_pull_status(
        message="Idle",
        progress={
            "processed": 0,
            "inserted": 0,
            "duplicates": 0,
            "missing_urls": 0,
            "errors": 0,
            "pages_scraped": 0,
            "current_page": None,
        },
    )
    return create_app({"TESTING": True})


@pytest.mark.buttons
def test_post_pull_data_returns_202_and_publishes_task(app, monkeypatch):
    # Ensure /pull-data queues work immediately instead of running the pull in-process.
    calls = {"count": 0, "kind": None}

    def fake_publish_task(kind, payload=None):
        calls["count"] += 1
        calls["kind"] = kind

    monkeypatch.setattr(dashboard, "publish_task", fake_publish_task)

    with app.test_client() as client:
        response = client.post("/pull-data")

    assert response.status_code == 202
    payload = response.get_json()
    assert payload["ok"] is True
    assert payload["message"] == "Pull request queued."
    assert calls["count"] == 1
    assert calls["kind"] == "scrape_new_data"


@pytest.mark.buttons
def test_post_update_analysis_returns_202_when_not_busy(app):
    # Ensure /update-analysis queues analytics refresh when no pull job is active.
    dashboard._set_pull_in_progress(False)

    with app.test_client() as client:
        response = client.post("/update-analysis")

    assert response.status_code == 202
    payload = response.get_json()
    assert payload["ok"] is True
    assert payload["busy"] is False
    assert payload["updated"] is True
    assert payload["message"] == "Analysis update queued."


@pytest.mark.buttons
def test_busy_gating_update_analysis_returns_409_and_no_update(app):
    # Ensure /update-analysis is blocked with 409 while a pull is in progress.
    dashboard._set_pull_in_progress(True)

    with app.test_client() as client:
        response = client.post("/update-analysis")

    assert response.status_code == 409
    payload = response.get_json()
    assert payload["busy"] is True
    assert payload["ok"] is False


@pytest.mark.buttons
def test_pull_data_still_queues_when_local_fallback_state_is_busy(app):
    # The queue endpoint accepts work even if the in-memory fallback state is busy.
    dashboard._set_pull_in_progress(True)

    with app.test_client() as client:
        response = client.post("/pull-data")

    assert response.status_code == 202
    payload = response.get_json()
    assert payload["busy"] is False
    assert payload["ok"] is True


@pytest.mark.buttons
def test_pull_status_endpoint_returns_snapshot(app):
    # Ensure /pull-status returns the latest status message and progress snapshot.
    dashboard._update_pull_status(message="Working", progress={"processed": 2})
    with app.test_client() as client:
        response = client.get("/pull-status")
    assert response.status_code == 200
    payload = response.get_json()
    assert payload["message"] == "Working"
    assert payload["progress"]["processed"] == 2


@pytest.mark.buttons
def test_pull_data_publish_failure_returns_503(monkeypatch):
    # Ensure publish failures surface as HTTP 503 to the browser.
    app = create_app({"TESTING": True})

    monkeypatch.setattr(dashboard, "_set_job_status", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        dashboard,
        "publish_task",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("publish failed")),
    )

    with app.test_client() as client:
        response = client.post("/pull-data")

    assert response.status_code == 503
    assert response.get_json()["ok"] is False


@pytest.mark.buttons
def test_run_pull_job_branches_and_failure():
    # Cover _run_pull_job success-without-pages and exception failure branches.
    dashboard._set_pull_in_progress(True)

    def one_page_runner(progress_callback=None):
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

    dashboard._run_pull_job(one_page_runner)
    snapshot_one_page = dashboard._get_pull_status_snapshot()
    assert "Pulled pages 1-1." in snapshot_one_page["message"]

    dashboard._set_pull_in_progress(True)

    def no_pages_runner(progress_callback=None):
        return {
            "start_page": 1,
            "end_page": 0,
            "pages_scraped": 0,
            "processed": 0,
            "inserted": 0,
            "duplicates": 0,
            "missing_urls": 1,
            "errors": 1,
        }

    dashboard._run_pull_job(no_pages_runner)
    snapshot = dashboard._get_pull_status_snapshot()
    assert "No new pages found" in snapshot["message"]
    assert "1 inserts failed." in snapshot["message"]
    assert dashboard._get_pull_in_progress() is False

    dashboard._set_pull_in_progress(True)

    def failing_runner(progress_callback=None):
        raise RuntimeError("boom")

    dashboard._run_pull_job(failing_runner)
    snapshot_fail = dashboard._get_pull_status_snapshot()
    assert "Pull failed:" in snapshot_fail["message"]
    assert dashboard._get_pull_in_progress() is False


@pytest.mark.db
def test_pull_state_and_shared_status_helpers(monkeypatch):
    # Cover queue-status helper functions that back the polling UI.
    dashboard._set_pull_in_progress(False)
    assert dashboard._try_start_pull() is True
    assert dashboard._try_start_pull() is False
    dashboard._set_pull_in_progress(False)

    class Cursor:
        def __init__(self, row):
            self._row = row

        def fetchone(self):
            return self._row

    class Conn:
        def __init__(self, row=None):
            self.row = row
            self.commit_count = 0
            self.closed = False

        def execute(self, sql, params=None):
            if "SELECT state, message, progress_json" in sql:
                return Cursor(self.row)
            return Cursor(None)

        def commit(self):
            self.commit_count += 1

        def close(self):
            self.closed = True

    write_conn = Conn()
    monkeypatch.setattr(dashboard, "create_connection", lambda *args, **kwargs: write_conn)
    dashboard._set_job_status("job", "queued", "message", {"processed": 1})
    assert write_conn.commit_count == 2
    assert write_conn.closed is True

    none_conn = Conn(row=None)
    monkeypatch.setattr(dashboard, "create_connection", lambda *args, **kwargs: none_conn)
    assert dashboard._get_shared_pull_status() is None

    bad_json_conn = Conn(row=("running", "msg", "{not-json}"))
    monkeypatch.setattr(dashboard, "create_connection", lambda *args, **kwargs: bad_json_conn)
    bad_status = dashboard._get_shared_pull_status()
    assert bad_status["running"] is True
    assert bad_status["progress"]["processed"] == 0

    good_json_conn = Conn(row=("queued", "msg", "{\"processed\": 3}"))
    monkeypatch.setattr(dashboard, "create_connection", lambda *args, **kwargs: good_json_conn)
    good_status = dashboard._get_shared_pull_status()
    assert good_status["progress"]["processed"] == 3

    monkeypatch.setattr(dashboard, "_get_shared_pull_status", lambda: {"running": True, "message": "db", "progress": {}})
    assert dashboard._get_pull_status_snapshot()["message"] == "db"

    sentinel_runner = object()
    dashboard.APP_SETTINGS["PULL_RUNNER"] = sentinel_runner
    assert dashboard._get_pull_runner() is sentinel_runner
    dashboard.APP_SETTINGS["PULL_RUNNER"] = None


@pytest.mark.buttons
def test_dashboard_view_calls_render_template(monkeypatch):
    # Cover dashboard() directly so the render path is exercised without DB I/O.
    captured = {}

    monkeypatch.setattr(
        dashboard,
        "load_query_results",
        lambda: [{"title": "T", "description": "D", "sql": "SELECT 1", "columns": [], "rows": [], "display": None, "error": None}],
    )
    monkeypatch.setattr(
        dashboard,
        "_get_pull_status_snapshot",
        lambda: {"running": False, "message": "Idle", "progress": {}},
    )
    monkeypatch.setattr(
        dashboard,
        "render_template",
        lambda template, **kwargs: captured.update({"template": template, **kwargs}) or "rendered",
    )

    app = create_app({"TESTING": True})
    with app.test_request_context("/analysis?pull_status=ok&pull_message=done"):
        result = dashboard.dashboard()

    assert result == "rendered"
    assert captured["template"] == "dashboard.html"
    assert captured["pull_status"] == "ok"
    assert captured["pull_message"] == "done"


@pytest.mark.buttons
def test_update_analysis_publish_failure_returns_503(app, monkeypatch):
    # Ensure analytics publish failures are returned to the client as 503.
    monkeypatch.setattr(
        dashboard,
        "publish_task",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("publish failed")),
    )

    with app.test_client() as client:
        response = client.post("/update-analysis")

    assert response.status_code == 503
    assert response.get_json()["ok"] is False


@pytest.mark.db
def test_create_connection_uses_db_config_and_error(monkeypatch):
    # Verify create_connection uses discrete DB settings and wraps connection failures.
    monkeypatch.delenv("DATABASE_URL", raising=False)
    dashboard.APP_SETTINGS["DATABASE_URL"] = None

    class DummyOpErr(Exception):
        pass

    monkeypatch.setattr(dashboard, "OperationalError", DummyOpErr)

    calls = {}

    def fake_connect(*args, **kwargs):
        calls.update(kwargs)
        return object()

    monkeypatch.setattr(dashboard.psycopg, "connect", fake_connect)
    conn = dashboard.create_connection(
        db_name="d",
        db_user="u",
        db_password="p",
        db_host="h",
        db_port="9",
        database_url=None,
    )
    assert conn is not None
    assert calls["dbname"] == "d"
    assert calls["user"] == "u"

    def raise_connect(*args, **kwargs):
        raise DummyOpErr("bad")

    monkeypatch.setattr(dashboard.psycopg, "connect", raise_connect)
    with pytest.raises(RuntimeError):
        dashboard.create_connection(database_url=None)


@pytest.mark.buttons
def test_load_query_results_success_and_error(monkeypatch):
    # Ensure load_query_results returns formatted data and surfaces query errors.
    class Conn:
        def close(self):
            pass

    monkeypatch.setattr(dashboard, "create_connection", lambda *args, **kwargs: Conn())
    monkeypatch.setattr(
        dashboard.query_data,
        "get_queries",
        lambda: [
            {
                "title": "T",
                "description": "D",
                "sql": " SELECT 1 ",
                "params": None,
                "display_mode": "number",
            }
        ],
    )
    monkeypatch.setattr(
        dashboard.query_data, "execute_query", lambda conn, sql, params: ([(1,)], ["c"])
    )
    monkeypatch.setattr(dashboard.query_data, "format_display", lambda rows, mode, labels=None: "1")

    ok = dashboard.load_query_results()
    assert ok[0]["title"] == "T"
    assert ok[0]["display"] == "1"

    def raise_query(*args, **kwargs):
        raise RuntimeError("bad query")

    monkeypatch.setattr(dashboard.query_data, "execute_query", raise_query)
    err = dashboard.load_query_results()
    assert err[0]["title"] == "Query Error"
    assert err[0]["error"] == "Unable to load query results."


@pytest.mark.buttons
def test_fetch_applicant_row_by_url_none_branch():
    # Ensure fetch_applicant_row_by_url returns None when no row is found.
    class Cursor:
        def fetchone(self):
            return None

    class Conn:
        def execute(self, *args, **kwargs):
            return Cursor()

    assert dashboard.fetch_applicant_row_by_url(Conn(), "x") is None


@pytest.mark.buttons
def test_fetch_applicant_row_by_url_success_branch():
    # Ensure fetch_applicant_row_by_url maps tuple values to expected response keys.
    row = (
        "Computer Science, Johns Hopkins University",
        "c",
        "2026-02-10",
        "https://example.test/ok",
        "Accepted",
        "Fall 2026",
        "American",
        3.9,
        330.0,
        165.0,
        4.5,
        "Masters",
        "Computer Science",
        "Johns Hopkins University",
    )

    class Cursor:
        def fetchone(self):
            return row

    class Conn:
        def execute(self, *args, **kwargs):
            return Cursor()

    result = dashboard.fetch_applicant_row_by_url(Conn(), "https://example.test/ok")
    assert result is not None
    assert result["url"] == "https://example.test/ok"
    assert result["status"] == "Accepted"
    assert result["llm_generated_university"] == "Johns Hopkins University"


@pytest.mark.db
def test_create_connection_missing_config_raises_runtime(monkeypatch):
    # Ensure missing DB configuration raises a RuntimeError.
    monkeypatch.delenv("DATABASE_URL", raising=False)
    dashboard.APP_SETTINGS["DATABASE_URL"] = None
    with pytest.raises(RuntimeError):
        dashboard.create_connection(
            db_name=None,
            db_user=None,
            db_password=None,
            db_host=None,
            db_port=None,
            database_url=None,
        )


@pytest.mark.db
def test_create_connection_conninfo_and_operational_error(monkeypatch):
    # Ensure DATABASE_URL conninfo path works and operational errors are wrapped.
    monkeypatch.setattr(dashboard.psycopg, "connect", lambda conninfo: object())
    conn = dashboard.create_connection(
        database_url="postgresql://test-user:test-pass@localhost:5432/testdb"
    )
    assert conn is not None

    class DummyOpErr(Exception):
        pass

    monkeypatch.setattr(dashboard, "OperationalError", DummyOpErr)

    def _raise(conninfo):
        raise DummyOpErr("cannot connect")

    monkeypatch.setattr(dashboard.psycopg, "connect", _raise)
    with pytest.raises(RuntimeError, match="Database connection failed"):
        dashboard.create_connection(
            database_url="postgresql://test-user:test-pass@localhost:5432/testdb"
        )


@pytest.mark.db
def test_create_connection_uses_db_config_argument(monkeypatch):
    # Ensure create_connection supports the consolidated db_config argument path.
    monkeypatch.delenv("DATABASE_URL", raising=False)
    dashboard.APP_SETTINGS["DATABASE_URL"] = None
    calls = {}

    def fake_connect(*args, **kwargs):
        calls.update(kwargs)
        return object()

    monkeypatch.setattr(dashboard.psycopg, "connect", fake_connect)
    conn = dashboard.create_connection(
        db_config={
            "db_name": "d2",
            "db_user": "u2",
            "db_password": "p2",
            "db_host": "h2",
            "db_port": "5433",
        },
        database_url=None,
    )
    assert conn is not None
    assert calls["dbname"] == "d2"
    assert calls["user"] == "u2"
