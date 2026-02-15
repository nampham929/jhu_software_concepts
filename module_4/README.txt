1. Name: Nam Pham - JHED ID: npham21
2. Module Info: Module 4: Pytest and Sphinx - Due Date: February 15, 2026
3. Approach:

This assignment is to write a Pytest suite for the Grad Café analytics application that verifies different aspects of the application and documents the service with Sphinx.

Pytest suite includes:
--------------------------------------------------------------------------------
- `tests/test_flask_page.py`
  Flask app and page-rendering validation for required assignment behavior.
  - App factory/config check: confirms a testable Flask app is created and required routes are registered (`/`, `/analysis`, `/pull-data`, `/update-analysis`, `/pull-status`).
  - `GET /analysis` page-load check: asserts HTTP status `200`.
  - Button presence check: page contains both `Pull Data` and `Update Analysis` buttons.
  - Content check: page text includes `Analysis` and at least one `Answer:`

--------------------------------------------------------------------------------  
- `tests/test_buttons.py`
  Buttons and busy-state behavior validation for pull/update endpoints.
  - `POST /pull-data` success path: verifies HTTP `200` in foreground mode and confirms the loader/pull runner is triggered with mocked/faked behavior.
  - `POST /update-analysis` success path: verifies HTTP `200` when the app is not busy.
  - Busy gating for update: when pull is in progress, `POST /update-analysis` returns HTTP `409` and does not perform an update.
  - Busy gating for pull: when pull is in progress, `POST /pull-data` returns HTTP `409`.
  - Additional endpoint behavior coverage:
    - `/pull-status` snapshot response
    - background pull mode returning `202`
    - pull-job status message branches (no new pages and failure path)
    - DB helper and query-loader branch checks

------------------------------------------------------------------------------
- `tests/test_analysis_format.py`
  Analysis/query formatting tests plus selected module utility branch coverage.
  - Test labels & Rounding:
    - test that page output includes `Answer` labels for rendered analysis
    - test that percentages are formatted with two decimals
  - Verifies rendered analysis output includes `Answer:` labels.
  - Validates display formatting modes (`number`, `percent`, `labels`, `pairs`) and fallback behavior.
  - Checks percent formatting precision (always two decimals, including `None`).
  - Covers query execution success and exception path with error logging.
  - Tests `query_data.main()` under successful env setup and failure/missing-env branches.
  - Verifies query catalog and run modes (number/pair/label handling).
  - Exercises `module_2.clean` load-clean-save behavior.
  - Exercises `module_2.scrape` helpers with mocked robots/network and parser branch cases.
  - Validates `module_2.run` works when imported as a package with patched dependencies.

----------------------------------------------------------------------------
- `tests/test_db_insert.py`
  DB-focused and ETL-focused tests for schema integrity and data lifecycle behavior.
  - Database writes requirements:
    - Test insert on pull:
      - before pull: target table is empty
      - after `POST /pull-data`: new rows exist with required non-null fields
    - Test idempotency/constraints:
      - duplicate pulls do not create duplicate rows in the database
    - Test simple query function:
      - querying by URL returns a dict containing expected required keys (Module 3 fields)
  - Pull insert path: ensures required applicant schema columns are written and non-null.
  - Idempotency: confirms duplicate pulls do not duplicate rows.
  - Query helper schema check: validates fetch-by-URL returns all required keys.
  - `load_data` connection path coverage: success and operational error behavior.
  - Table creation coverage: commit on success, rollback on failure.
  - Parser/encoding helpers: date parsing, float parsing, BOM/encoding detection.
  - JSONL loader coverage:
    - normal insert path
    - malformed JSON lines
    - insert failure rollback path
    - missing file exception
    - blank-line handling
    - batch commit behavior (commit every 100 records)
  - `load_data.main()` path coverage:
    - env-var configuration success
    - `DATABASE_URL` direct path
    - missing-env runtime failure output
  - Pull pipeline branch coverage in dashboard:
    - stop URL branch
    - duplicate/missing URL handling
    - insert error handling + rollback
    - progress callback updates
    - module_2 path insertion helper behavior

---------------------------------------------------------------------------    
- `tests/test_integration_end_to_end.py`
  End-to-end integration behavior across pull, analysis update, and rendering.
  - End-to-end (pull -> update -> render):
    - injects a fake scraper/pull runner that returns multiple records
    - `POST /pull-data` succeeds and rows are persisted
    - `POST /update-analysis` succeeds when not busy
    - `GET /analysis` shows updated analysis with correctly formatted values
  - Multiple pulls consistency:
    - running `POST /pull-data` twice with overlapping data remains consistent with uniqueness policy
  - Runs complete workflow: pull data -> update analysis -> render `/analysis`.
  - Verifies output includes expected computed values from DB-backed data.
  - Repeats pulls with overlapping records to ensure consistent row counts and no duplicate growth.
- `tests/conftest.py`
  Shared fixture system and test guardrails.
  - Loads test environment (`.env.test`) and sets `src/` import path.
  - Defines real DB fixtures (`db_url`, `db_ready`, table reset fixture) for DB/integration tests.
  - Defines autouse state reset fixture to prevent pull-status state leakage between tests.
  - Defines autouse DB block fixture to prevent accidental real DB connections in non-DB tests.
  - Provides reusable fake applicant records and insert tuple builder.
  - Provides mock DB connection/cursor fixtures for deterministic DB-like unit tests without a live DB.

Notes
- Besides the core tests to meet the assignment requirements, additional tests are included to ensure 100% coverage.
- `pytest.ini` enforces strict markers and 100% coverage threshold.
- DB/integration tests require a reachable PostgreSQL database.

GitHub Actions CI
- Workflow location: repository root at `.github/workflows/tests.yml`
- Trigger: push and pull_request
- CI job behavior:
  1. Starts PostgreSQL service
  2. Installs dependencies
  3. Creates `postgres_test` database
  4. Runs `pytest` in `module_4`

Sphinx Documentation
- Build docs locally from module_4 root:
  sphinx-build -b html source build

- Sphinx generated HTMLs in build/
 
Read the Docs
- Config file is expected at repository root: `.readthedocs.yaml`
- Current Sphinx config path: `module_4/source/conf.py`
- Published docs URL: `https://jhu-software-concepts-nampham.readthedocs.io/en/latest/`

The SSH URL to my GitHub repository:
git@github.com:nampham929/jhu_software_concepts.git


How to run the program:
-Install the required packages in the requirements.txt file
-In VS Code, run pytest -m "web or buttons or analysis or db or integration"

4. Known Bugs: 

5. Citations:

