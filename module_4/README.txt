Module 4 - Testing and Documentation

Overview
This project is a Flask dashboard app with data loading, analysis queries, and a full pytest suite.
It includes local tests, PostgreSQL-backed tests, GitHub Actions CI, and Sphinx docs.

Project Layout
- src/: application code (Flask app, dashboard blueprint, data/query modules, module_2)
- tests/: pytest test suite
- source/: Sphinx documentation source
- requirements.txt: Python dependencies
- pytest.ini: pytest config, markers, and coverage settings
- .github/workflows/tests.yml (repo root): CI workflow

Requirements
- Python 3.12
- PostgreSQL (local or container)
- pip

Local Setup (Windows PowerShell)
1. Create and activate virtual environment
   py -3.12 -m venv .venv
   .\.venv\Scripts\Activate.ps1

2. Install dependencies
   pip install -r requirements.txt

3. Configure environment variables
   Copy `.env.example` to `.env` and set `DATABASE_URL`.
   Copy `.env.test.example` to `.env.test` and set `TEST_DATABASE_URL`.

Run the Flask App
- From module_4 root:
  python src/flask_app.py

Run Tests
- Full suite:
  pytest

- By marker:
  pytest -m web
  pytest -m buttons
  pytest -m analysis
  pytest -m db
  pytest -m integration

Notes
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

- Open generated docs:
  build/index.html

Read the Docs
- Config file is expected at repository root: `.readthedocs.yaml`
- Current Sphinx config path: `module_4/source/conf.py`

Author
Nam Pham
