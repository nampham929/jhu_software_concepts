"""Shared applicant insert helpers used by multiple modules."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable


# Single canonical insert statement so all loaders write rows consistently.
INSERT_APPLICANTS_QUERY = """
    INSERT INTO applicants (
        program, comments, date_added, url, status, term,
        us_or_international, gpa, gre, gre_v, gre_aw,
        degree, llm_generated_program, llm_generated_university
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""


def build_insert_values(
    entry: dict,
    parse_date_func: Callable[[str | None], str | None],
    parse_float_func: Callable[[str | None], float | None],
) -> tuple:
    """Build insert values in applicants-column order."""
    # Keep this order exactly aligned with INSERT_APPLICANTS_QUERY.
    # Source keys reflect upstream scraped/cleaned field names.
    return (
        entry.get("program"),
        entry.get("comments"),
        parse_date_func(entry.get("date_added")),
        entry.get("url"),
        entry.get("status"),
        entry.get("term"),
        entry.get("US/International"),
        parse_float_func(entry.get("GPA")),
        parse_float_func(entry.get("GRE_SCORE")),
        parse_float_func(entry.get("GRE_V")),
        parse_float_func(entry.get("GRE_AW")),
        entry.get("Degree"),
        entry.get("llm-generated-program"),
        entry.get("llm-generated-university"),
    )


@dataclass
class InsertEntriesOptions:
    """Optional callbacks and policies used while inserting entries."""

    # Return True to skip an entry before attempting INSERT.
    should_skip: Callable | None = None
    # Called after a successful INSERT.
    on_inserted: Callable | None = None
    # Called when an INSERT fails (after rollback).
    on_insert_error: Callable | None = None
    # Called on periodic progress checkpoints.
    on_progress: Callable | None = None
    # Custom policy that decides when to commit.
    should_commit: Callable | None = None


def insert_entries(
    connection,
    entries,
    build_values: Callable,
    options: InsertEntriesOptions | None = None,
) -> tuple[int, int]:
    """Insert entries with commit/rollback handling and optional callbacks."""
    inserted_count = 0
    error_count = 0
    # Normalize optional callback config once for the full insert run.
    callbacks = options or InsertEntriesOptions()

    def default_should_commit(index, inserted, errors):
        _ = index, errors
        # Default batching: commit every 100 successful inserts.
        return inserted > 0 and inserted % 100 == 0

    commit_check = callbacks.should_commit or default_should_commit

    for index, entry in enumerate(entries, 1):
        # Skip policy lets callers short-circuit known duplicates/invalid rows.
        if callbacks.should_skip and callbacks.should_skip(entry):
            continue
        try:
            # Build values outside SQL text to keep INSERT parameterized.
            connection.execute(INSERT_APPLICANTS_QUERY, build_values(entry))
            inserted_count += 1
            if callbacks.on_inserted:
                callbacks.on_inserted(entry, index, inserted_count)
        except Exception as error:  # pylint: disable=broad-exception-caught
            error_count += 1
            # Keep the connection usable after statement-level failures.
            connection.rollback()
            if callbacks.on_insert_error:
                callbacks.on_insert_error(entry, index, error, error_count)

        # Commit policy is caller-overridable for batch size/perf tuning.
        if commit_check(index, inserted_count, error_count):
            connection.commit()
            if callbacks.on_progress:
                callbacks.on_progress(index, inserted_count, error_count)

    # Final commit flushes any trailing successful inserts.
    connection.commit()
    return inserted_count, error_count
