"""RabbitMQ consumer that processes scrape and analytics tasks."""

from __future__ import annotations

import json
import os
import time
from datetime import datetime

import pika
import psycopg
from etl.scrape import BASE_URL, _fetch_html, _parse_page
from pika.exceptions import AMQPConnectionError

EXCHANGE = "tasks"
QUEUE = "tasks_q"
ROUTING_KEY = "tasks"
WATERMARK_SOURCE = "gradcafe_survey"
RABBITMQ_CONNECT_RETRIES = 30
RABBITMQ_CONNECT_DELAY_SECONDS = 2
PULL_TASK_NAME = "scrape_new_data"
ANALYTICS_TASK_NAME = "recompute_analytics"

INSERT_APPLICANT_SQL = """
    INSERT INTO applicants (
        program, comments, date_added, url, status, term,
        us_or_international, gpa, gre, gre_v, gre_aw,
        degree, llm_generated_program, llm_generated_university
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT DO NOTHING;
"""


def _parse_date(date_string: str | None) -> str | None:
    if not date_string:
        return None
    try:
        return datetime.strptime(date_string, "%B %d, %Y").strftime("%Y-%m-%d")
    except ValueError:
        return None


def _parse_float(value: str | None) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except ValueError:
        return None


def _ensure_schema(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS applicants (
            p_id SERIAL PRIMARY KEY,
            program TEXT,
            comments TEXT,
            date_added DATE,
            url TEXT,
            status TEXT,
            term TEXT,
            us_or_international TEXT,
            gpa FLOAT,
            gre FLOAT,
            gre_v FLOAT,
            gre_aw FLOAT,
            degree TEXT,
            llm_generated_program TEXT,
            llm_generated_university TEXT
        );
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS applicants_url_unique_idx
        ON applicants (url);
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ingestion_watermarks (
            source TEXT PRIMARY KEY,
            last_seen TEXT,
            updated_at TIMESTAMPTZ DEFAULT now()
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS job_status (
            job_name TEXT PRIMARY KEY,
            state TEXT NOT NULL,
            message TEXT NOT NULL,
            progress_json TEXT NOT NULL,
            updated_at TIMESTAMPTZ DEFAULT now()
        );
        """
    )


def _build_row(entry: dict) -> tuple:
    return (
        entry.get("program"),
        entry.get("comments"),
        _parse_date(entry.get("date_added")),
        entry.get("url"),
        entry.get("status"),
        entry.get("term"),
        entry.get("US/International"),
        _parse_float(entry.get("GPA")),
        _parse_float(entry.get("GRE_SCORE")),
        _parse_float(entry.get("GRE_V")),
        _parse_float(entry.get("GRE_AW")),
        entry.get("Degree"),
        entry.get("llm-generated-program"),
        entry.get("llm-generated-university"),
    )


def _get_last_seen(conn) -> str | None:
    row = conn.execute(
        """
        SELECT last_seen
        FROM ingestion_watermarks
        WHERE source = %s;
        """,
        (WATERMARK_SOURCE,),
    ).fetchone()
    return row[0] if row else None


def _set_last_seen(conn, last_seen: str) -> None:
    conn.execute(
        """
        INSERT INTO ingestion_watermarks (source, last_seen, updated_at)
        VALUES (%s, %s, now())
        ON CONFLICT (source)
        DO UPDATE SET last_seen = EXCLUDED.last_seen, updated_at = now();
        """,
        (WATERMARK_SOURCE, last_seen),
    )


def _load_seed_rows(seed_path: str) -> list[dict]:
    try:
        with open(seed_path, "r", encoding="utf-8-sig") as file_handle:
            payload = json.load(file_handle)
    except UnicodeDecodeError:
        with open(seed_path, "r", encoding="utf-16") as file_handle:
            payload = json.load(file_handle)
    if isinstance(payload, list):
        return payload
    return []


def _default_progress() -> dict:
    return {
        "processed": 0,
        "inserted": 0,
        "duplicates": 0,
        "missing_urls": 0,
        "errors": 0,
        "pages_scraped": 0,
        "current_page": None,
    }


def _set_job_status(conn, job_name: str, state: str, message: str, progress: dict | None = None) -> None:
    conn.execute(
        """
        INSERT INTO job_status (job_name, state, message, progress_json, updated_at)
        VALUES (%s, %s, %s, %s, now())
        ON CONFLICT (job_name)
        DO UPDATE SET
            state = EXCLUDED.state,
            message = EXCLUDED.message,
            progress_json = EXCLUDED.progress_json,
            updated_at = now();
        """,
        (job_name, state, message, json.dumps(progress or _default_progress())),
    )


def _seed_if_empty(conn) -> None:
    _ensure_schema(conn)
    row = conn.execute("SELECT COUNT(*) FROM applicants;").fetchone()
    existing = row[0] if row else 0
    if existing:
        return

    seed_path = os.getenv("SEED_JSON")
    if not seed_path or not os.path.exists(seed_path):
        return

    rows = _load_seed_rows(seed_path)
    newest_url = None
    for entry in rows:
        url = entry.get("url")
        if not url:
            continue
        if newest_url is None:
            newest_url = url
        conn.execute(INSERT_APPLICANT_SQL, _build_row(entry))

    if newest_url:
        _set_last_seen(conn, newest_url)
    conn.commit()


def _scrape_until(last_seen: str | None, max_pages: int = 10) -> list[dict]:
    page = 1
    rows: list[dict] = []

    while page <= max_pages:
        html = _fetch_html(f"{BASE_URL}?page={page}")
        parsed = _parse_page(html)
        if not parsed:
            break

        if last_seen:
            stop_index = next(
                (idx for idx, entry in enumerate(parsed) if entry.get("url") == last_seen),
                None,
            )
            if stop_index is not None:
                rows.extend(parsed[:stop_index])
                break

        rows.extend(parsed)
        page += 1

    return rows


def handle_scrape_new_data(conn, payload):
    """Fetch incremental rows and insert idempotently using URL watermark."""
    _ensure_schema(conn)
    _set_job_status(
        conn,
        PULL_TASK_NAME,
        "running",
        "Pull Data is running.",
        _default_progress(),
    )
    since_url = payload.get("since")
    last_seen = since_url or _get_last_seen(conn)

    batch = _scrape_until(last_seen)
    newest_url = None
    inserted = 0

    for entry in batch:
        url = entry.get("url")
        if not url:
            continue
        if newest_url is None:
            newest_url = url
        cursor = conn.execute(INSERT_APPLICANT_SQL, _build_row(entry))
        inserted += max(cursor.rowcount, 0)

    if newest_url:
        _set_last_seen(conn, newest_url)
    _set_job_status(
        conn,
        PULL_TASK_NAME,
        "completed",
        "Pull Data completed.",
        {
            "processed": len(batch),
            "inserted": inserted,
            "duplicates": max(len(batch) - inserted, 0),
            "missing_urls": 0,
            "errors": 0,
            "pages_scraped": 0,
            "current_page": None,
        },
    )


def handle_recompute_analytics(conn, payload):
    """Recompute analytics artifacts used by the web UI."""
    _ = payload
    _ensure_schema(conn)
    _set_job_status(
        conn,
        ANALYTICS_TASK_NAME,
        "running",
        "Analysis refresh is running.",
        _default_progress(),
    )
    conn.execute(
        """
        CREATE MATERIALIZED VIEW IF NOT EXISTS applicant_status_summary AS
        SELECT term, status, COUNT(*) AS total
        FROM applicants
        GROUP BY term, status;
        """
    )
    conn.execute("REFRESH MATERIALIZED VIEW applicant_status_summary;")
    _set_job_status(
        conn,
        ANALYTICS_TASK_NAME,
        "completed",
        "Analysis refresh completed.",
        _default_progress(),
    )


def _on_message(ch, method, _properties, body):
    msg = json.loads(body.decode("utf-8"))
    kind = msg.get("kind")
    payload = msg.get("payload") or {}

    handlers = {
        "scrape_new_data": handle_scrape_new_data,
        "recompute_analytics": handle_recompute_analytics,
    }
    handler = handlers.get(kind)
    if handler is None:
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        return

    with psycopg.connect(os.environ["DATABASE_URL"]) as conn:
        try:
            handler(conn, payload)
            conn.commit()
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception:
            conn.rollback()
            if kind in {PULL_TASK_NAME, ANALYTICS_TASK_NAME}:
                _ensure_schema(conn)
                _set_job_status(
                    conn,
                    kind,
                    "failed",
                    f"{kind} failed.",
                    {"errors": 1},
                )
                conn.commit()
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def main():
    with psycopg.connect(os.environ["DATABASE_URL"]) as db_conn:
        _seed_if_empty(db_conn)

    params = pika.URLParameters(os.environ["RABBITMQ_URL"])
    connection = None
    for attempt in range(1, RABBITMQ_CONNECT_RETRIES + 1):
        try:
            connection = pika.BlockingConnection(params)
            break
        except AMQPConnectionError:
            if attempt == RABBITMQ_CONNECT_RETRIES:
                raise
            time.sleep(RABBITMQ_CONNECT_DELAY_SECONDS)

    if connection is None:
        raise RuntimeError("RabbitMQ connection could not be established.")

    channel = connection.channel()

    channel.exchange_declare(exchange=EXCHANGE, exchange_type="direct", durable=True)
    channel.queue_declare(queue=QUEUE, durable=True)
    channel.queue_bind(exchange=EXCHANGE, queue=QUEUE, routing_key=ROUTING_KEY)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=QUEUE, on_message_callback=_on_message, auto_ack=False)
    channel.start_consuming()


if __name__ == "__main__":
    main()
