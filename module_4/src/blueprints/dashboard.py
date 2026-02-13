"""Flask dashboard blueprint for GradCafe data and query reporting."""
from __future__ import annotations

import json
import os
import re
import sys
import threading
from typing import Any, Callable

from flask import Blueprint, jsonify, render_template, request
import psycopg
from psycopg import OperationalError

from load_data import create_applicants_table, parse_date, parse_float
import query_data


dashboard_bp = Blueprint("dashboard", __name__)

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
    "db_name": os.getenv("DB_NAME", "postgres"),
    "db_user": os.getenv("DB_USER", "postgres"),
    "db_password": os.getenv("DB_PASSWORD", "postgres"),
    "db_host": os.getenv("DB_HOST", "localhost"),
    "db_port": os.getenv("DB_PORT", "5432"),
}


# Configure runtime settings from Flask app config.
def configure_dashboard(settings: dict[str, Any]) -> None:
    for key in ("DATABASE_URL", "RUN_PULL_IN_BACKGROUND", "PULL_RUNNER"):
        if key in settings:
            APP_SETTINGS[key] = settings[key]


# Create a connection to PostgreSQL database.
def create_connection(
    db_name: str | None = None,
    db_user: str | None = None,
    db_password: str | None = None,
    db_host: str | None = None,
    db_port: str | None = None,
    database_url: str | None = None,
):
    conninfo = database_url or APP_SETTINGS.get("DATABASE_URL") or os.getenv("DATABASE_URL")
    try:
        if conninfo:
            return psycopg.connect(conninfo)

        return psycopg.connect(
            dbname=db_name or DB_CONFIG["db_name"],
            user=db_user or DB_CONFIG["db_user"],
            password=db_password or DB_CONFIG["db_password"],
            host=db_host or DB_CONFIG["db_host"],
            port=db_port or DB_CONFIG["db_port"],
        )
    except OperationalError as exc:
        raise RuntimeError(f"Database connection failed: {exc}") from exc


# Load all queries from query_data.py and prepare results for rendering.
def load_query_results() -> list[dict[str, Any]]:
    queries = query_data.get_queries()

    results = []
    connection = create_connection()
    try:
        for query in queries:
            rows, columns = query_data.execute_query(
                connection, query["sql"], query["params"] or ()
            )
            display = query_data.format_display(
                rows, query.get("display_mode"), query.get("display_labels")
            )
            results.append(
                {
                    "title": query["title"],
                    "description": query["description"],
                    "sql": query["sql"].strip(),
                    "columns": columns,
                    "rows": rows,
                    "display": display,
                    "error": None,
                }
            )
    except Exception as exc:
        # Provide a single error result so the UI can render gracefully.
        results.append(
            {
                "title": "Query Error",
                "description": "An error occurred while running the query set.",
                "sql": "",
                "columns": [],
                "rows": [],
                "error": str(exc),
            }
        )
    finally:
        connection.close()

    return results


# Return one applicant row as a dict with required schema keys.
def fetch_applicant_row_by_url(connection, url: str) -> dict[str, Any] | None:
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
    module_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    if module_root not in sys.path:
        sys.path.insert(0, module_root)
    return module_root


# Get a set of URLs already in the DB so the program can skip duplicates.
def _fetch_existing_urls(connection) -> set[str]:
    cursor = connection.execute(
        "SELECT url FROM applicants WHERE url IS NOT NULL AND url <> '';"
    )
    return {row[0] for row in cursor.fetchall()}


# Return the newest applicant URL in the database.
def _fetch_latest_url(connection) -> str | None:
    cursor = connection.execute(
        "SELECT url FROM applicants WHERE url IS NOT NULL AND url <> '' ORDER BY date_added DESC LIMIT 1;"
    )
    row = cursor.fetchone()
    return row[0] if row else None


# Parse page number from a scrape URL.
def _extract_page_number(url: str) -> int:
    match = re.search(r"page=(\d+)", url)
    return int(match.group(1)) if match else 1


# Scrape new GradCafe pages, clean them, and insert new rows.
def pull_gradcafe_data(
    progress_callback: Callable[..., None] | None = None,
    scraper_module=None,
    clean_module=None,
    connection_factory: Callable[..., Any] | None = None,
) -> dict[str, int]:
    _ensure_module_2_on_path()
    if scraper_module is None or clean_module is None:
        from module_2 import scrape, clean

        scraper_module = scraper_module or scrape
        clean_module = clean_module or clean

    connection_factory = connection_factory or create_connection

    start_page = 1
    raw_data = []
    last_page = 0
    pages_scraped = 0

    connection = connection_factory()
    try:
        stop_url = _fetch_latest_url(connection)
        existing_urls = _fetch_existing_urls(connection)
    finally:
        connection.close()

    page = start_page
    # Scrape until a page returns no data.
    while True:
        html = scraper_module._fetch_html(f"{scraper_module.BASE_URL}?page={page}")
        page_data = scraper_module._parse_page(html)
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
                if progress_callback:
                    progress_callback(
                        progress={
                            "pages_scraped": pages_scraped,
                            "current_page": page,
                        }
                    )
                break

        raw_data.extend(page_data)
        last_page = page
        pages_scraped += 1
        if progress_callback:
            progress_callback(
                progress={
                    "pages_scraped": pages_scraped,
                    "current_page": page,
                }
            )
        page += 1

    # Normalize the scraped data before inserting.
    cleaned_data = clean_module.clean_data(raw_data)

    connection = connection_factory()
    create_applicants_table(connection)

    insert_query = """
        INSERT INTO applicants (
            program, comments, date_added, url, status, term,
            us_or_international, gpa, gre, gre_v, gre_aw,
            degree, llm_generated_program, llm_generated_university
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    inserted_count = 0
    duplicate_count = 0
    missing_url_count = 0
    error_count = 0
    new_entries = []

    try:
        for index, entry in enumerate(cleaned_data, 1):
            url = entry.get("url") or ""
            if not url:
                missing_url_count += 1
                continue
            if url in existing_urls:
                duplicate_count += 1
                continue

            try:
                connection.execute(
                    insert_query,
                    (
                        entry.get("program"),
                        entry.get("comments"),
                        parse_date(entry.get("date_added")),
                        url,
                        entry.get("status"),
                        entry.get("term"),
                        entry.get("US/International"),
                        parse_float(entry.get("GPA")),
                        parse_float(entry.get("GRE_SCORE")),
                        parse_float(entry.get("GRE_V")),
                        parse_float(entry.get("GRE_AW")),
                        entry.get("Degree"),
                        entry.get("llm-generated-program"),
                        entry.get("llm-generated-university"),
                    ),
                )
                inserted_count += 1
                existing_urls.add(url)
                new_entries.append(entry)
            except Exception:
                error_count += 1
                connection.rollback()

            if index % 100 == 0:
                connection.commit()
                if progress_callback:
                    progress_callback(
                        progress={
                            "processed": index,
                            "inserted": inserted_count,
                            "duplicates": duplicate_count,
                            "missing_urls": missing_url_count,
                            "errors": error_count,
                        }
                    )

        connection.commit()
    finally:
        connection.close()

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
        "inserted": inserted_count,
        "duplicates": duplicate_count,
        "missing_urls": missing_url_count,
        "errors": error_count,
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
    pull_status = request.args.get("pull_status")
    pull_message = request.args.get("pull_message")
    is_pull_running = _get_pull_in_progress()
    pull_state = _get_pull_status_snapshot()
    results = load_query_results()
    return render_template(
        "dashboard.html",
        results=results,
        pull_status=pull_status,
        pull_message=pull_message,
        pull_in_progress=is_pull_running,
        pull_state=pull_state,
    )


# Start a pull job.
@dashboard_bp.route("/pull-data", methods=["POST"])
def pull_data():
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
    return jsonify({"ok": ok, "busy": False, "message": snapshot["message"], "pull_state": snapshot}), status_code


# The actual pull job with status updates.
def _run_pull_job(pull_runner: Callable[..., dict[str, int]] | None = None) -> None:
    runner = pull_runner or _get_pull_runner()
    try:
        summary = runner(progress_callback=_update_pull_status)
        if summary["pages_scraped"]:
            message = (
                "Pulled pages {start_page}-{end_page}. Added {inserted} new entries; "
                "skipped {duplicates} duplicates and {missing_urls} entries without URLs."
            ).format(**summary)
        else:
            message = (
                "No new pages found starting at page {start_page}. "
                "Added {inserted} new entries; skipped {duplicates} duplicates and "
                "{missing_urls} entries without URLs."
            ).format(**summary)
        if summary["errors"]:
            message += f" {summary['errors']} inserts failed."
        _update_pull_status(message=message, progress=summary)
    except Exception as exc:
        _update_pull_status(message=f"Pull failed: {exc}")
    finally:
        _set_pull_in_progress(False)


# Refresh the dashboard after data changes (no server-side work).
@dashboard_bp.route("/update-analysis", methods=["POST"])
def update_analysis():
    if _get_pull_in_progress():
        return jsonify({"ok": False, "busy": True, "message": "Update Analysis is disabled while Pull Data is running."}), 409
    return jsonify({"ok": True, "busy": False, "updated": True}), 200


# Provide pull status updates for the polling UI.
@dashboard_bp.route("/pull-status", methods=["GET"])
def pull_status():
    return jsonify(_get_pull_status_snapshot())
