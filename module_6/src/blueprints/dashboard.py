"""Flask dashboard blueprint for GradCafe data and query reporting."""
from __future__ import annotations

import json
import importlib
import os
import re
import sys
import threading
from typing import Any, Callable

from flask import Blueprint, jsonify, render_template, request
import psycopg
from psycopg import OperationalError

from applicant_insert import InsertEntriesOptions, build_insert_values, insert_entries
from load_data import create_applicants_table as _create_applicants_table, parse_date, parse_float
import query_data


dashboard_bp = Blueprint("dashboard", __name__)


def create_applicants_table(connection):  # pragma: no cover
    """Backward-compatible proxy to load_data table setup helper."""
    return _create_applicants_table(connection)

# Shared runtime configuration; populated by flask_app.create_app(...).
APP_SETTINGS: dict[str, Any] = {
    "DATABASE_URL": os.getenv("DATABASE_URL"),
    "RUN_PULL_IN_BACKGROUND": True,
    "PULL_RUNNER": None,
}

# Shared state for the long-running pull job, guarded by a lock.
pull_state_lock = threading.Lock()
pull_status_state = {
    "running": False,
    "message": "Idle",
    "progress": {
        "processed": 0,
        "inserted": 0,
        "duplicates": 0,
        "missing_urls": 0,
        "errors": 0,
        "pages_scraped": 0,
        "current_page": None,
    },
}

# Default database settings used by the dashboard and pull job.
DB_CONFIG = {
    "db_name": os.getenv("DB_NAME"),
    "db_user": os.getenv("DB_USER"),
    "db_password": os.getenv("DB_PASSWORD"),
    "db_host": os.getenv("DB_HOST"),
    "db_port": os.getenv("DB_PORT"),
}


# Configure runtime settings from Flask app config.
def configure_dashboard(settings: dict[str, Any]) -> None:
    """Apply runtime dashboard settings from Flask app config."""
    for key in ("DATABASE_URL", "RUN_PULL_IN_BACKGROUND", "PULL_RUNNER"):
        if key in settings:
            APP_SETTINGS[key] = settings[key]


# Create a connection to PostgreSQL database.
def create_connection(
    db_config: dict[str, str | None] | None = None,
    database_url: str | None = None,
    **legacy_config: str | None,
):
    """Create a PostgreSQL connection from URL or discrete settings."""
    conninfo = database_url or APP_SETTINGS.get("DATABASE_URL") or os.getenv("DATABASE_URL")
    try:
        if conninfo:
            return psycopg.connect(conninfo)

        combined_config = {
            "db_name": None,
            "db_user": None,
            "db_password": None,
            "db_host": None,
            "db_port": None,
        }
        if db_config:
            combined_config.update(db_config)
        for key in combined_config:
            if key in legacy_config:
                combined_config[key] = legacy_config[key]

        resolved = {
            "dbname": combined_config["db_name"] or DB_CONFIG["db_name"],
            "user": combined_config["db_user"] or DB_CONFIG["db_user"],
            "password": combined_config["db_password"] or DB_CONFIG["db_password"],
            "host": combined_config["db_host"] or DB_CONFIG["db_host"],
            "port": combined_config["db_port"] or DB_CONFIG["db_port"],
        }
        key_to_env = {
            "dbname": "DB_NAME",
            "user": "DB_USER",
            "password": "DB_PASSWORD",
            "host": "DB_HOST",
            "port": "DB_PORT",
        }
        missing = [key_to_env[key] for key, value in resolved.items() if not value]
        if missing:
            missing_text = ", ".join(missing)
            raise RuntimeError(
                "Database configuration is missing. Set DATABASE_URL or "
                "DB_NAME/DB_USER/DB_PASSWORD/DB_HOST/DB_PORT. "
                f"Missing: {missing_text}"
            )

        return psycopg.connect(**resolved)
    except OperationalError as exc:
        raise RuntimeError(f"Database connection failed: {exc}") from exc


# Load all queries from query_data.py and prepare results for rendering.
def load_query_results() -> list[dict[str, Any]]:
    """Execute all configured queries and return render-ready results."""
    queries = query_data.get_queries()

    results = []
    connection = create_connection()
    try:
        for query in queries:
            stmt = query_data.get_query_stmt(query)
            params = query_data.get_query_params(query)
            rows, columns = query_data.execute_query(
                connection, stmt, params
            )
            display = query_data.format_display(
                rows, query.get("display_mode"), query.get("display_labels")
            )
            sql_text = (
                stmt.as_string(connection).strip()
                if hasattr(stmt, "as_string")
                else str(stmt).strip()
            )
            results.append(
                {
                    "title": query["title"],
                    "description": query["description"],
                    "sql": sql_text,
                    "columns": columns,
                    "rows": rows,
                    "display": display,
                    "error": None,
                }
            )
    except (RuntimeError, psycopg.Error, KeyError, TypeError, ValueError):
        # Provide a single error result so the UI can render gracefully.
        results.append(
            {
                "title": "Query Error",
                "description": "An error occurred while running the query set.",
                "sql": "",
                "columns": [],
                "rows": [],
                "error": "Unable to load query results.",
            }
        )
    finally:
        if hasattr(connection, "close"):
            connection.close()

    return results


# Return one applicant row as a dict with required schema keys.
def fetch_applicant_row_by_url(connection, url: str) -> dict[str, Any] | None:
    """Fetch one applicant row by URL using the dashboard output schema."""
    cursor = connection.execute(
        """
        SELECT
            program, comments, date_added, url, status, term,
            us_or_international, gpa, gre, gre_v, gre_aw,
            degree, llm_generated_program, llm_generated_university
        FROM applicants
        WHERE url = %s
        LIMIT 1;
        """,
        (url,),
    )
    row = cursor.fetchone()
    if not row:
        return None

    keys = [
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
    return dict(zip(keys, row))


# Make module_2 importable when running the dashboard.
def _ensure_module_2_on_path() -> str:
    module_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    if module_root not in sys.path:
        sys.path.insert(0, module_root)
    return module_root


# Get a set of URLs already in the DB so the program can skip duplicates.
def _fetch_existing_urls(connection) -> set[str]:
    batch_size = query_data.clamp_query_limit(
        query_data.MAX_QUERY_LIMIT, default=query_data.MAX_QUERY_LIMIT
    )
    offset = 0
    urls: set[str] = set()

    while True:
        cursor = connection.execute(
            """
            SELECT url
            FROM applicants
            WHERE url IS NOT NULL AND url <> ''
            ORDER BY url
            LIMIT %s OFFSET %s;
            """,
            (batch_size, offset),
        )
        rows = cursor.fetchall()
        if not rows:
            break
        urls.update(row[0] for row in rows)
        has_more = len(rows) == batch_size
        offset += batch_size if has_more else 0
        if not has_more:
            break

    return urls


# Return the newest applicant URL in the database.
def _fetch_latest_url(connection) -> str | None:
    cursor = connection.execute(
        "SELECT url FROM applicants "
        "WHERE url IS NOT NULL AND url <> '' "
        "ORDER BY date_added DESC LIMIT 1;"
    )
    row = cursor.fetchone()
    return row[0] if row else None


# Parse page number from a scrape URL.
def _extract_page_number(url: str) -> int:
    match = re.search(r"page=(\d+)", url)
    return int(match.group(1)) if match else 1


def _resolve_scrape_modules(scraper_module, clean_module):
    """Resolve scrape/clean modules from args, module_2 shim, or package imports."""
    if scraper_module is not None and clean_module is not None:
        return scraper_module, clean_module

    # Support both real package imports and tests that monkeypatch
    # sys.modules["module_2"] with scrape/clean attributes.
    module2 = sys.modules.get("module_2")
    scrape = getattr(module2, "scrape", None) if module2 else None
    clean = getattr(module2, "clean", None) if module2 else None
    if scrape is None:
        scrape = importlib.import_module("module_2.scrape")
    if clean is None:
        clean = importlib.import_module("module_2.clean")
    return scraper_module or scrape, clean_module or clean


def _load_existing_context(connection_factory):
    """Load stop URL and existing URLs from database using a short-lived connection."""
    connection = connection_factory()
    try:
        # Ensure first-run pulls work even when the applicants table is not created yet.
        create_applicants_table(connection)
        stop_url = _fetch_latest_url(connection)
        existing_urls = _fetch_existing_urls(connection)
    finally:
        connection.close()
    return stop_url, existing_urls


def _notify_page_progress(progress_callback, pages_scraped, page):
    if progress_callback:
        progress_callback(
            progress={
                "pages_scraped": pages_scraped,
                "current_page": page,
            }
        )


def _resolve_scraper_callables(scraper_module):
    """Resolve scraper callables with support for public and legacy private names."""
    return (
        getattr(scraper_module, "fetch_html", None) or getattr(scraper_module, "_fetch_html"),
        getattr(scraper_module, "parse_page", None) or getattr(scraper_module, "_parse_page"),
    )


def _scrape_new_rows(scraper_module, stop_url, progress_callback, start_page):
    """Scrape pages until no data or stop_url is encountered."""
    raw_data = []
    last_page = 0
    pages_scraped = 0
    page = start_page
    fetch_html, parse_page = _resolve_scraper_callables(scraper_module)

    # Scrape until a page returns no data.
    while True:
        html = fetch_html(f"{scraper_module.BASE_URL}?page={page}")
        page_data = parse_page(html)
        if not page_data:
            break

        if stop_url:
            stop_index = next(
                (
                    index
                    for index, entry in enumerate(page_data)
                    if entry.get("url") == stop_url
                ),
                None,
            )
            if stop_index is not None:
                raw_data.extend(page_data[:stop_index])
                last_page = page
                pages_scraped += 1
                _notify_page_progress(progress_callback, pages_scraped, page)
                break

        raw_data.extend(page_data)
        last_page = page
        pages_scraped += 1
        _notify_page_progress(progress_callback, pages_scraped, page)
        page += 1

    return raw_data, last_page, pages_scraped


def _insert_cleaned_rows(connection_factory, cleaned_data, existing_urls, progress_callback):
    """Insert cleaned rows and return insert counters plus collected new entries."""
    connection = connection_factory()

    stats = {
        "inserted": 0,
        "errors": 0,
        "duplicates": 0,
        "missing_urls": 0,
    }
    new_entries = []

    def should_skip(entry):
        url = entry.get("url") or ""
        if not url:
            stats["missing_urls"] += 1
            return True
        if url in existing_urls:
            stats["duplicates"] += 1
            return True
        return False

    def on_inserted(entry, _index, inserted):
        stats["inserted"] = inserted
        url = entry.get("url") or ""
        existing_urls.add(url)
        new_entries.append(entry)

    def on_insert_error(_entry, _index, _error, errors):
        stats["errors"] = errors

    def on_progress(index, inserted, errors):
        if progress_callback:
            progress_callback(
                progress={
                    "processed": index,
                    "inserted": inserted,
                    "duplicates": stats["duplicates"],
                    "missing_urls": stats["missing_urls"],
                    "errors": errors,
                }
            )

    try:
        inserted_count, error_count = insert_entries(
            connection,
            cleaned_data,
            lambda entry: build_insert_values(entry, parse_date, parse_float),
            InsertEntriesOptions(
                should_skip=should_skip,
                on_inserted=on_inserted,
                on_insert_error=on_insert_error,
                on_progress=on_progress,
                should_commit=lambda index, _inserted, _errors: index % 100 == 0,
            ),
        )
    finally:
        connection.close()

    stats["inserted"] = inserted_count
    stats["errors"] = error_count
    return stats, new_entries


# Scrape new GradCafe pages, clean them, and insert new rows.
def pull_gradcafe_data(
    progress_callback: Callable[..., None] | None = None,
    scraper_module=None,
    clean_module=None,
    connection_factory: Callable[..., Any] | None = None,
) -> dict[str, int]:
    """Scrape new GradCafe data, insert unseen rows, and return stats."""
    _ensure_module_2_on_path()
    scraper_module, clean_module = _resolve_scrape_modules(scraper_module, clean_module)
    connection_factory = connection_factory or create_connection
    start_page = 1
    stop_url, existing_urls = _load_existing_context(connection_factory)
    raw_data, last_page, pages_scraped = _scrape_new_rows(
        scraper_module, stop_url, progress_callback, start_page
    )

    # Normalize the scraped data before inserting.
    cleaned_data = clean_module.clean_data(raw_data)
    insert_stats, new_entries = _insert_cleaned_rows(
        connection_factory, cleaned_data, existing_urls, progress_callback
    )

    # Save the new entries for inspection/debugging.
    new_data_path = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "new_data.json")
    )
    with open(new_data_path, "w", encoding="utf-8") as file_handle:
        json.dump(new_entries, file_handle, indent=2, ensure_ascii=False)

    return {
        "start_page": start_page,
        "end_page": last_page,
        "pages_scraped": pages_scraped,
        "processed": len(cleaned_data),
        "inserted": insert_stats["inserted"],
        "duplicates": insert_stats["duplicates"],
        "missing_urls": insert_stats["missing_urls"],
        "errors": insert_stats["errors"],
    }


# Check if a pull job is currently running.
def _get_pull_in_progress() -> bool:
    with pull_state_lock:
        return pull_status_state["running"]


# Set the pull job running flag.
def _set_pull_in_progress(value: bool) -> None:
    with pull_state_lock:
        pull_status_state["running"] = value


# Attempt to mark a pull job as running; return False if one is active.
def _try_start_pull() -> bool:
    with pull_state_lock:
        if pull_status_state["running"]:
            return False
        pull_status_state["running"] = True
        return True


# Update status text and progress counters for the UI.
def _update_pull_status(message: str | None = None, progress: dict[str, Any] | None = None) -> None:
    """Update pull-state message/progress under lock."""
    with pull_state_lock:
        if message is not None:
            pull_status_state["message"] = message
        if progress:
            pull_status_state["progress"].update(progress)


# Return a safe snapshot of current pull status for the UI.
def _get_pull_status_snapshot() -> dict[str, Any]:
    with pull_state_lock:
        return {
            "running": pull_status_state["running"],
            "message": pull_status_state["message"],
            "progress": dict(pull_status_state["progress"]),
        }


def _get_pull_runner() -> Callable[..., dict[str, int]]:
    return APP_SETTINGS.get("PULL_RUNNER") or pull_gradcafe_data


# Render the main dashboard page with live query results.
@dashboard_bp.route("/")
@dashboard_bp.route("/analysis")
def dashboard():
    """Render the analysis dashboard view."""
    pull_status_value = request.args.get("pull_status")
    pull_message = request.args.get("pull_message")
    is_pull_running = _get_pull_in_progress()
    pull_state = _get_pull_status_snapshot()
    results = load_query_results()
    return render_template(
        "dashboard.html",
        results=results,
        pull_status=pull_status_value,
        pull_message=pull_message,
        pull_in_progress=is_pull_running,
        pull_state=pull_state,
    )


# Start a pull job.
@dashboard_bp.route("/pull-data", methods=["POST"])
def pull_data():
    """Start a pull job, either backgrounded or inline."""
    if not _try_start_pull():
        return jsonify({"ok": False, "busy": True, "message": "Pull Data is already running."}), 409

    _update_pull_status(
        message="Pull started. Scraping pages and inserting new rows.",
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

    pull_runner = _get_pull_runner()
    run_in_background = bool(APP_SETTINGS.get("RUN_PULL_IN_BACKGROUND", True))
    if run_in_background:
        thread = threading.Thread(target=_run_pull_job, args=(pull_runner,), daemon=True)
        thread.start()
        return jsonify({"ok": True, "busy": False, "message": "Pull started."}), 202

    _run_pull_job(pull_runner)
    snapshot = _get_pull_status_snapshot()
    ok = not snapshot["message"].lower().startswith("pull failed")
    status_code = 200 if ok else 500
    return (
        jsonify(
            {
                "ok": ok,
                "busy": False,
                "message": snapshot["message"],
                "pull_state": snapshot,
            }
        ),
        status_code,
    )


# The actual pull job with status updates.
def _run_pull_job(pull_runner: Callable[..., dict[str, int]] | None = None) -> None:
    runner = pull_runner or _get_pull_runner()
    try:
        summary = runner(progress_callback=_update_pull_status)
        if summary["pages_scraped"]:
            message = (
                f"Pulled pages {summary['start_page']}-{summary['end_page']}. "
                f"Added {summary['inserted']} new entries; skipped "
                f"{summary['duplicates']} duplicates and "
                f"{summary['missing_urls']} entries without URLs."
            )
        else:
            message = (
                f"No new pages found starting at page {summary['start_page']}. "
                f"Added {summary['inserted']} new entries; skipped "
                f"{summary['duplicates']} duplicates and "
                f"{summary['missing_urls']} entries without URLs."
            )
        if summary["errors"]:
            message += f" {summary['errors']} inserts failed."
        _update_pull_status(message=message, progress=summary)
    except (RuntimeError, psycopg.Error, OSError, ValueError, TypeError) as exc:
        _update_pull_status(message=f"Pull failed: {exc}")
    finally:
        _set_pull_in_progress(False)


# Refresh the dashboard after data changes (no server-side work).
@dashboard_bp.route("/update-analysis", methods=["POST"])
def update_analysis():
    """Allow the UI to refresh analysis when no pull is running."""
    if _get_pull_in_progress():
        return (
            jsonify(
                {
                    "ok": False,
                    "busy": True,
                    "message": "Update Analysis is disabled while Pull Data is running.",
                }
            ),
            409,
        )
    return jsonify({"ok": True, "busy": False, "updated": True}), 200


# Provide pull status updates for the polling UI.
@dashboard_bp.route("/pull-status", methods=["GET"])
def pull_status():
    """Return current pull status for polling clients."""
    return jsonify(_get_pull_status_snapshot())
