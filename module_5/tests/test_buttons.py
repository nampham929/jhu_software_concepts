from __future__ import annotations

import pytest

import blueprints.dashboard as dashboard
from flask_app import create_app


@pytest.fixture
def app(monkeypatch):
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

    return create_app({"TESTING": True, "RUN_PULL_IN_BACKGROUND": False})


@pytest.mark.buttons
def test_post_pull_data_returns_ok_and_triggers_loader(app):
    # Ensure /pull-data succeeds and invokes the configured pull runner once.
    calls = {"count": 0}

    def fake_pull_runner(progress_callback=None):
        calls["count"] += 1
        if progress_callback:
            progress_callback(progress={"processed": 1, "inserted": 1})
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

    app.config["PULL_RUNNER"] = fake_pull_runner
    dashboard.configure_dashboard(app.config)

    with app.test_client() as client:
        response = client.post("/pull-data")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["ok"] is True
    assert calls["count"] == 1


@pytest.mark.buttons
def test_post_update_analysis_returns_200_when_not_busy(app):
    # Ensure /update-analysis returns success when no pull job is active.
    dashboard._set_pull_in_progress(False)

    with app.test_client() as client:
        response = client.post("/update-analysis")

    assert response.status_code == 200
    assert response.get_json() == {"ok": True, "busy": False, "updated": True}


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
def test_busy_gating_pull_data_returns_409(app):
    # Ensure /pull-data is blocked with 409 while a pull is in progress.
    dashboard._set_pull_in_progress(True)

    with app.test_client() as client:
        response = client.post("/pull-data")

    assert response.status_code == 409
    payload = response.get_json()
    assert payload["busy"] is True
    assert payload["ok"] is False

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
def test_pull_data_background_mode_returns_202(monkeypatch):
    # Ensure background pull mode returns 202 and starts a worker thread.
    app = create_app({"TESTING": True, "RUN_PULL_IN_BACKGROUND": True})

    class FakeThread:
        started = False

        def __init__(self, target=None, args=None, daemon=None):
            self.target = target
            self.args = args or ()

        def start(self):
            FakeThread.started = True

    monkeypatch.setattr(dashboard.threading, "Thread", FakeThread)
    app.config["PULL_RUNNER"] = lambda progress_callback=None: {
        "start_page": 1,
        "end_page": 1,
        "pages_scraped": 1,
        "processed": 0,
        "inserted": 0,
        "duplicates": 0,
        "missing_urls": 0,
        "errors": 0,
    }
    dashboard.configure_dashboard(app.config)

    with app.test_client() as client:
        response = client.post("/pull-data")

    assert response.status_code == 202
    assert response.get_json()["ok"] is True
    assert FakeThread.started is True


@pytest.mark.buttons
def test_run_pull_job_branches_and_failure(monkeypatch):
    # Cover _run_pull_job success-without-pages and exception failure branches.
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
        lambda: [{"title": "T", "description": "D", "sql": " SELECT 1 ", "params": None, "display_mode": "number"}],
    )
    monkeypatch.setattr(dashboard.query_data, "execute_query", lambda conn, sql, params: ([(1,)], ["c"]))
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
    conn = dashboard.create_connection(database_url="postgresql://test-user:test-pass@localhost:5432/testdb")
    assert conn is not None

    class DummyOpErr(Exception):
        pass

    monkeypatch.setattr(dashboard, "OperationalError", DummyOpErr)

    def _raise(conninfo):
        raise DummyOpErr("cannot connect")

    monkeypatch.setattr(dashboard.psycopg, "connect", _raise)
    with pytest.raises(RuntimeError, match="Database connection failed"):
        dashboard.create_connection(database_url="postgresql://test-user:test-pass@localhost:5432/testdb")


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
