from __future__ import annotations

import os
import sys
from pathlib import Path

import psycopg
import pytest


SRC_PATH = Path(__file__).resolve().parents[1] / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))


@pytest.fixture
def db_url() -> str:
    return (
        os.getenv("TEST_DATABASE_URL")
        or os.getenv("DATABASE_URL")
        or "postgresql://postgres:dataBase!605@localhost:5432/postgres"
    )


@pytest.fixture
def db_ready(db_url: str) -> None:
    try:
        conn = psycopg.connect(db_url)
        conn.close()
    except Exception as exc:
        pytest.skip(f"PostgreSQL not available for db/integration tests: {exc}")


@pytest.fixture(autouse=True)
def reset_pull_state():
    import blueprints.dashboard as dashboard

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
