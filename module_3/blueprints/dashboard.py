"""Flask dashboard blueprint for GradCafe data and query reporting."""
import json
import os
import sys
import threading

from flask import Blueprint, jsonify, render_template, redirect, request, url_for
import psycopg
from psycopg import OperationalError

from load_data import create_applicants_table, parse_date, parse_float
import query_data


dashboard_bp = Blueprint("dashboard", __name__)

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
    "db_name": "postgres",
    "db_user": "postgres",
    "db_password": "dataBase!605",
    "db_host": "localhost",
    "db_port": "5432",
}


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
    except OperationalError as exc:
        raise RuntimeError(f"Database connection failed: {exc}") from exc
    return connection


def load_query_results():
    """Execute all dashboard queries and prepare results for rendering."""
    queries = query_data.get_queries()

    results = []
    connection = create_connection(**DB_CONFIG)
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


def _ensure_module_2_on_path() -> str:
    """Make module_2 importable when running the dashboard."""
    module_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    if module_root not in sys.path:
        sys.path.insert(0, module_root)
    return module_root


def _fetch_existing_urls(connection) -> set:
    """Return the set of applicant URLs already stored in the database."""
    cursor = connection.execute(
        "SELECT url FROM applicants WHERE url IS NOT NULL AND url <> '';"
    )
    return {row[0] for row in cursor.fetchall()}


def _fetch_latest_url(connection) -> str | None:
    """Return the most recent applicant URL based on highest primary key."""
    cursor = connection.execute(
        "SELECT url FROM applicants WHERE url IS NOT NULL AND url <> '' ORDER BY date_added DESC LIMIT 1;"
    )
    row = cursor.fetchone()
    return row[0] if row else None


def _save_pull_state(last_page: int) -> None:
    """Persist the last scraped page index to disk."""
    state_path = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "last_pull.json")
    )
    with open(state_path, "w", encoding="utf-8") as file_handle:
        json.dump({"last_page": last_page}, file_handle, indent=2)


def pull_gradcafe_data(progress_callback=None) -> dict:
    """Scrape new GradCafe pages, clean them, and insert new rows."""
    _ensure_module_2_on_path()
    from module_2 import scrape, clean

    start_page = 1
    raw_data = []
    last_page = 0
    pages_scraped = 0

    connection = create_connection(**DB_CONFIG)
    try:
        stop_url = _fetch_latest_url(connection)
        existing_urls = _fetch_existing_urls(connection)
    finally:
        connection.close()

    page = start_page
    # Scrape until a page returns no data.
    while True:
        html = scrape._fetch_html(f"{scrape.BASE_URL}?page={page}")
        page_data = scrape._parse_page(html)
        if not page_data:
            break
        if stop_url:
            stop_index = next(
                (index for index, entry in enumerate(page_data) if entry.get("url") == stop_url),
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

    if pages_scraped:
        _save_pull_state(last_page)

    # Normalize the scraped data before inserting.
    cleaned_data = clean.clean_data(raw_data)

    connection = create_connection(**DB_CONFIG)
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
                        None,
                        None,
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


def _get_pull_in_progress() -> bool:
    """Return True if a pull job is currently running."""
    with pull_state_lock:
        return pull_status_state["running"]


def _set_pull_in_progress(value: bool) -> None:
    """Set the pull job running flag."""
    with pull_state_lock:
        pull_status_state["running"] = value


def _try_start_pull() -> bool:
    """Attempt to mark a pull job as running; return False if one is active."""
    with pull_state_lock:
        if pull_status_state["running"]:
            return False
        pull_status_state["running"] = True
        return True


def _update_pull_status(message=None, progress=None) -> None:
    """Update status text and progress counters for the UI."""
    with pull_state_lock:
        if message is not None:
            pull_status_state["message"] = message
        if progress:
            pull_status_state["progress"].update(progress)


def _get_pull_status_snapshot() -> dict:
    """Return a safe snapshot of current pull status for the UI."""
    with pull_state_lock:
        return {
            "running": pull_status_state["running"],
            "message": pull_status_state["message"],
            "progress": dict(pull_status_state["progress"]),
        }


@dashboard_bp.route("/")
def dashboard():
    """Render the main dashboard page with live query results."""
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


@dashboard_bp.route("/pull-data", methods=["POST"])
def pull_data():
    """Start a background pull job and redirect back to the dashboard."""
    if not _try_start_pull():
        return redirect(
            url_for(
                "dashboard.dashboard",
                pull_status="warning",
                pull_message="Pull Data is already running. Please wait for it to finish.",
            )
        )
    try:
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
        # Run the pull in a background thread so the request returns quickly.
        thread = threading.Thread(target=_run_pull_job, daemon=True)
        thread.start()
        return redirect(
            url_for(
                "dashboard.dashboard",
                pull_status="success",
                pull_message="Pull started. Progress will update while it runs.",
            )
        )
    except Exception as exc:
        message = f"Pull failed: {exc}"
        return redirect(
            url_for("dashboard.dashboard", pull_status="error", pull_message=message)
        )


def _run_pull_job() -> None:
    """Execute the pull job and update the shared status on completion."""
    try:
        summary = pull_gradcafe_data(progress_callback=_update_pull_status)
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


@dashboard_bp.route("/update-analysis", methods=["POST"])
def update_analysis():
    """Refresh the dashboard after data changes (no server-side work)."""
    if _get_pull_in_progress():
        return redirect(
            url_for(
                "dashboard.dashboard",
                pull_status="warning",
                pull_message="Update Analysis is disabled while Pull Data is running.",
            )
        )
    return redirect(
        url_for(
            "dashboard.dashboard",
            pull_status="success",
            pull_message="Analysis refreshed with the latest results.",
        )
    )


@dashboard_bp.route("/pull-status", methods=["GET"])
def pull_status():
    """Provide pull status updates for the polling UI."""
    return jsonify(_get_pull_status_snapshot())
