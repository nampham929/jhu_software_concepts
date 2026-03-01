"""Utilities for loading applicant data into PostgreSQL."""

import json
import os
from datetime import datetime

import psycopg
from psycopg import OperationalError
from applicant_insert import InsertEntriesOptions, build_insert_values, insert_entries
from db_connection import (
    build_db_config,
    create_connection_from_env,
    create_connection_with_driver,
)

def create_connection(db_name, db_user, db_password, db_host, db_port):
    """Create and return a PostgreSQL connection."""
    # Delegate shared validation and connect-call wiring to db_connection helpers.
    return create_connection_with_driver(
        psycopg.connect,
        OperationalError,
        build_db_config(db_name, db_user, db_password, db_host, db_port),
    )

def create_applicants_table(connection):
    """Create the applicants table if it does not already exist."""
    create_table_query = """
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
    try:
        # DDL is committed explicitly so downstream inserts always see the table.
        connection.execute(create_table_query)
        connection.commit()
        print("Applicants table created successfully")
    except Exception as e:
        print(f"Error creating table: {e}")
        connection.rollback()
        raise

def parse_date(date_string):
    """Parse a date like ``Month DD, YYYY`` into ``YYYY-MM-DD``."""
    if not date_string:
        return None
    try:
        dt = datetime.strptime(date_string, "%B %d, %Y")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        print(f"Warning: Could not parse date '{date_string}'")
        return None

def parse_float(value):
    """Parse a float value, returning ``None`` when empty or invalid."""
    if value is None or value == "":
        return None
    try:
        return float(value)
    except ValueError:
        return None

def detect_file_encoding(file_path):
    """Detect the likely encoding for an input JSONL file."""
    # Inspect BOM bytes only; this avoids reading the full input into memory.
    with open(file_path, "rb") as f:
        first_bytes = f.read(4)
    if first_bytes.startswith(b"\xff\xfe") or first_bytes.startswith(b"\xfe\xff"):
        return "utf-16"
    if first_bytes.startswith(b"\xef\xbb\xbf"):
        return "utf-8-sig"
    return "utf-8"

def _iter_json_entries(file_handle, error_state):
    """Yield parsed entries while tracking and reporting JSON decode errors."""
    for line_num, line in enumerate(file_handle, 1):
        if not line.strip():
            continue
        try:
            yield {"line_num": line_num, "data": json.loads(line.strip())}
        except json.JSONDecodeError as error:
            # Keep loading after malformed lines and retain first-error context for summary.
            error_state["error_count"] += 1
            if error_state["first_error_line"] is None:
                error_state["first_error_line"] = line_num
                error_state["first_error_message"] = f"JSON decode error: {error}"
            if error_state["error_count"] <= 5:
                print(f"Warning: Could not parse JSON at line {line_num}: {error}")
                print(f"Line content: {line[:100]}...")


def _handle_insert_error(entry, _index, error, total_errors, error_state):
    """Track and report insert errors from insert_entries callback."""
    error_state["error_count"] += 1
    if error_state["first_error_line"] is None:
        error_state["first_error_line"] = entry["line_num"]
        error_state["first_error_message"] = f"Insert error: {error}"
    if total_errors <= 5:
        print(f"Error inserting record at line {entry['line_num']}: {error}")


def load_data_from_jsonl(connection, jsonl_file):
    """Load applicant rows from a JSONL file into the database."""
    try:
        # Shared error-state drives both inline warnings and final summary output.
        error_state = {
            "error_count": 0,
            "first_error_line": None,
            "first_error_message": None,
        }

        encoding = detect_file_encoding(jsonl_file)
        print(f"Detected file encoding: {encoding}")

        with open(jsonl_file, 'r', encoding=encoding) as file_handle:
            entries = _iter_json_entries(file_handle, error_state)
            # insert_entries centralizes batch commit and rollback behavior.
            inserted_count, _insert_error_count = insert_entries(
                connection,
                entries,
                lambda item: build_insert_values(item["data"], parse_date, parse_float),
                InsertEntriesOptions(
                    on_insert_error=lambda item, index, error, count: _handle_insert_error(
                        item, index, error, count, error_state
                    ),
                    on_progress=lambda _index, inserted, _errors: print(
                        f"Inserted {inserted} records..."
                    ),
                ),
            )

        total_error_count = error_state["error_count"]
        print(f"Data loading completed. Total records inserted: {inserted_count}")
        if total_error_count > 0:
            print(f"Total errors encountered: {total_error_count}")
            if error_state["first_error_line"] is not None:
                print(
                    "First error at line "
                    f"{error_state['first_error_line']} "
                    f"({error_state['first_error_message']})"
                )

    except FileNotFoundError:
        print(f"Error: File '{jsonl_file}' not found")
        raise
    except Exception as e:
        # Roll back any partial transaction so callers can safely retry.
        print(f"Error during data loading: {e}")
        connection.rollback()
        raise

def main():
    """Main function to orchestrate the data loading process."""
    # Path to the JSONL file
    jsonl_file = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "data", "llm_extend_applicant_data.jsonl")
    )

    try:
        # Prefer DATABASE_URL when present; otherwise use discrete DB_* env vars.
        conn = create_connection_from_env(psycopg.connect, create_connection, os.getenv)

        # Create table
        create_applicants_table(conn)

        # Load data
        load_data_from_jsonl(conn, jsonl_file)

        if hasattr(conn, "close"):
            conn.close()

        print("Connection closed.")

    except (FileNotFoundError, RuntimeError, OperationalError, OSError) as e:
        print(f"Failed to complete data loading: {e}")

if __name__ == "__main__":  # pragma: no cover
    main()
