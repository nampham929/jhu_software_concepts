from __future__ import annotations

import os
import sys
from pathlib import Path

import psycopg
import pytest
from dotenv import load_dotenv
from load_data import create_applicants_table


ROOT_PATH = Path(__file__).resolve().parents[1]
SRC_PATH = ROOT_PATH / "src"
WEB_PATH = SRC_PATH / "web"
WEB_APP_PATH = WEB_PATH / "app"
DB_PATH = SRC_PATH / "db"


def _prioritize_test_imports() -> None:
    """Ensure tests import module_6 runtime modules before legacy copies."""
    desired_order = [DB_PATH, WEB_APP_PATH, WEB_PATH, SRC_PATH, ROOT_PATH]
    current_paths = [path for path in sys.path if path not in {str(item) for item in desired_order}]
    for path in reversed(desired_order):
        current_paths.insert(0, str(path))
    sys.path[:] = current_paths


_prioritize_test_imports()

# Load optional test-only env file from repo root.
load_dotenv(Path(__file__).resolve().parents[1] / ".env.test")


# Mock database implementation for tests that don't require a real database
class MockConnection:
    """Mock database connection for testing without a real database."""
    
    def __init__(self):
        self.tables = {}
        self.executed = []
        self.commit_count = 0
        self.rollback_count = 0
        self._initialize_schema()
    
    def _initialize_schema(self):
        """Initialize the applicants table schema."""
        self.tables["applicants"] = []
    
    def execute(self, sql: str, params=None):
        """Mock execute method that simulates SQL execution."""
        self.executed.append((sql, params))
        
        # Handle CREATE TABLE
        if "CREATE TABLE" in sql:
            return self
        
        # Handle TRUNCATE
        if "TRUNCATE TABLE" in sql:
            self.tables["applicants"] = []
            return self
        
        # Handle INSERT
        if "INSERT INTO applicants" in sql:
            if params:
                # Check for duplicates by URL before inserting
                for existing_row in self.tables["applicants"]:
                    if existing_row["url"] == params[3]:  # URL is at index 3
                        raise Exception("Duplicate URL")
                
                # Store the row as a dict with field names
                row_dict = {
                    "program": params[0],
                    "comments": params[1],
                    "date_added": params[2],
                    "url": params[3],
                    "status": params[4],
                    "term": params[5],
                    "us_or_international": params[6],
                    "gpa": params[7],
                    "gre": params[8],
                    "gre_v": params[9],
                    "gre_aw": params[10],
                    "degree": params[11],
                    "llm_generated_program": params[12],
                    "llm_generated_university": params[13],
                }
                self.tables["applicants"].append(row_dict)
            return self
        
        # Handle SELECT COUNT(*) with WHERE clause for status='accepted'
        if "SELECT COUNT(*) FROM applicants WHERE LOWER(status) = 'accepted'" in sql:
            count = sum(1 for row in self.tables["applicants"] 
                       if row.get("status", "").lower() == "accepted")
            return MockCursor([(count,)])
        
        # Handle SELECT COUNT(*)
        if "SELECT COUNT(*) FROM applicants" in sql:
            count = len(self.tables["applicants"])
            return MockCursor([(count,)])
        
        # Handle SELECT url FROM applicants (for deduplication check)
        if "SELECT url FROM applicants" in sql:
            urls = [(row["url"],) for row in self.tables["applicants"] 
                   if row["url"] and row["url"].strip()]
            return MockCursor(urls)
        
        # Handle SELECT with specific columns
        if "SELECT" in sql and "FROM applicants" in sql and "LIMIT 1" in sql:
            if self.tables["applicants"]:
                row = self.tables["applicants"][-1]
                result = (
                    row["program"],
                    row["comments"],
                    row["date_added"],
                    row["url"],
                    row["status"],
                    row["term"],
                    row["us_or_international"],
                    row["gpa"],
                    row["gre"],
                    row["gre_v"],
                    row["gre_aw"],
                    row["degree"],
                    row["llm_generated_program"],
                    row["llm_generated_university"],
                )
                return MockCursor([result])
            return MockCursor([])
        
        # Handle SELECT for fetch by URL
        if "SELECT" in sql and "FROM applicants" in sql and "WHERE url" in sql:
            for row in self.tables["applicants"]:
                if row["url"] == params[0]:
                    return MockCursor([row])
            return MockCursor([])
        
        return self
    
    def commit(self):
        """Mock commit."""
        self.commit_count += 1
    
    def rollback(self):
        """Mock rollback."""
        self.rollback_count += 1
    
    def close(self):
        """Mock close."""
        pass
    
    def fetchone(self):
        """Compatibility method."""
        return None
    
    def fetchall(self):
        """Compatibility method."""
        return []


class MockCursor:
    """Mock cursor for returning query results."""
    
    def __init__(self, results):
        self.results = results
        self.index = 0
    
    def fetchone(self):
        """Return the first result."""
        if self.results:
            return self.results[0]
        return None
    
    def fetchall(self):
        """Return all results."""
        return self.results


@pytest.fixture
def db_url() -> str:
    value = os.getenv("TEST_DATABASE_URL")
    if not value:
        pytest.fail(
            "TEST_DATABASE_URL is required for DB/integration tests. "
            "Set it to a dedicated test database (do not reuse DATABASE_URL)."
        )
    app_db_url = os.getenv("DATABASE_URL")
    if app_db_url and value == app_db_url:
        pytest.fail(
            "TEST_DATABASE_URL must be different from DATABASE_URL to protect application data."
        )
    return value


@pytest.fixture
def db_ready(db_url: str) -> None:
    try:
        conn = psycopg.connect(db_url)
        conn.close()
    except Exception as exc:
        pytest.skip(f"PostgreSQL not available for db/integration tests: {exc}")


@pytest.fixture(autouse=True)
def reset_pull_state():
    import app.blueprints.dashboard as dashboard

    # Prevent state leakage between tests that touch dashboard pull status.
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


@pytest.fixture(autouse=True)
def block_db_in_non_db_tests(request, monkeypatch):
    marker_names = {mark.name for mark in request.node.iter_markers()}
    # Allow real DB connections only for explicitly marked tests.
    if "db" in marker_names or "integration" in marker_names:
        return

    import app.blueprints.dashboard as dashboard
    import load_data
    import query_data

    def _blocked_connection(*args, **kwargs):
        raise RuntimeError(
            "Non-DB test attempted a real database connection. "
            "Use monkeypatch/fakes or mark test with @pytest.mark.db."
        )

    # Patch all known DB entry points so accidental live connections fail fast.
    monkeypatch.setattr(dashboard, "create_connection", _blocked_connection)
    monkeypatch.setattr(dashboard, "_set_job_status", lambda *args, **kwargs: None)
    monkeypatch.setattr(dashboard, "publish_task", lambda *args, **kwargs: None)
    monkeypatch.setattr(dashboard.psycopg, "connect", _blocked_connection)
    monkeypatch.setattr(load_data.psycopg, "connect", _blocked_connection)
    monkeypatch.setattr(query_data.psycopg, "connect", _blocked_connection)


@pytest.fixture
def fake_applicant_row() -> dict:
    return {
        "program": "Computer Science, Johns Hopkins University",
        "comments": "test row",
        "date_added": "February 10, 2026",
        "url": "https://example.test/row-1",
        "status": "Accepted",
        "term": "Fall 2026",
        "US/International": "American",
        "GPA": "3.9",
        "GRE_SCORE": "330",
        "GRE_V": "165",
        "GRE_AW": "4.5",
        "Degree": "Masters",
        "llm-generated-program": "Computer Science",
        "llm-generated-university": "Johns Hopkins University",
    }


@pytest.fixture
def insert_row_tuple():
    def _build(entry: dict) -> tuple:
        # Keep this order aligned with the INSERT statement used in tests.
        return (
            entry.get("program"),
            entry.get("comments"),
            "2026-02-10",
            entry.get("url"),
            entry.get("status"),
            entry.get("term"),
            entry.get("US/International"),
            float(entry.get("GPA")) if entry.get("GPA") else None,
            float(entry.get("GRE_SCORE")) if entry.get("GRE_SCORE") else None,
            float(entry.get("GRE_V")) if entry.get("GRE_V") else None,
            float(entry.get("GRE_AW")) if entry.get("GRE_AW") else None,
            entry.get("Degree"),
            entry.get("llm-generated-program"),
            entry.get("llm-generated-university"),
        )

    return _build


@pytest.fixture
def reset_applicants_table(db_url: str):
    def _reset() -> None:
        import app.blueprints.dashboard as dashboard

        # Recreate expected schema and clear data for deterministic DB tests.
        conn = dashboard.create_connection(database_url=db_url)
        try:
            create_applicants_table(conn)
            conn.execute("TRUNCATE TABLE applicants;")
            conn.commit()
        finally:
            conn.close()

    return _reset


@pytest.fixture
def fake_pull_records() -> list[tuple]:
    return [
        (
            "Computer Science, Johns Hopkins University",
            "row one",
            "2026-02-10",
            "https://example.test/e2e-1",
            "Accepted",
            "Fall 2026",
            "American",
            3.9,
            330,
            165,
            4.5,
            "Masters",
            "Computer Science",
            "Johns Hopkins University",
        ),
        (
            "Computer Science, MIT",
            "row two",
            "2026-02-11",
            "https://example.test/e2e-2",
            "Rejected",
            "Fall 2026",
            "International",
            3.7,
            325,
            160,
            4.0,
            "PhD",
            "Computer Science",
            "MIT",
        ),
    ]


# Mock-based fixtures for tests that don't require a real database
@pytest.fixture
def mock_db_connection() -> MockConnection:
    """Provide a mock database connection for testing."""
    return MockConnection()


@pytest.fixture
def mock_db_url() -> str:
    """Provide a fake database URL for mock-based tests."""
    return "mock://test-database"


@pytest.fixture
def mock_reset_applicants_table(mock_db_connection: MockConnection):
    """Fixture to reset mock applicants table."""
    def _reset() -> None:
        mock_db_connection._initialize_schema()
    
    return _reset


@pytest.fixture
def mock_create_connection(mock_db_connection: MockConnection, monkeypatch):
    """Fixture to mock the create_connection function."""
    import app.blueprints.dashboard as dashboard
    import load_data
    
    def _mock_create_connection(database_url=None, **kwargs):
        return mock_db_connection
    
    monkeypatch.setattr(dashboard, "create_connection", _mock_create_connection)
    monkeypatch.setattr(load_data, "create_connection", _mock_create_connection)
    
    return _mock_create_connection
