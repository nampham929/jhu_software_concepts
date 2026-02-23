# Nam Pham - JHED ID: npham21

## Module Info
Module 5: Pytest and Sphinx  
Due Date: February 23, 2026

## Approach
This assignment is to practice with software assurance workflows: input validation, static analysis, dependency analysis, virtual environments, supply-chain scanning, and least-privilege database configuration.

## Pylint
I ran Pylint on the `src` directory only and fixed all reported issues.  
Final Pylint output: `Your code has been rated at 10.00/10` with no remaining warnings or errors.

### How to run Pylint
- Inside `module_5` folder, in the terminal, run:
  - `.\.venv\Scripts\python.exe -m pylint src`

## SQL Injection Defenses
- Refactored SQL execution so user-controlled values are passed as bound parameters (no f-strings, `+` concatenation, or `.format()` to build raw SQL from input).
- Used psycopg SQL composition (`sql.SQL`, `sql.Identifier`, `sql.Placeholder`) for dynamic query parts in `src/query_data.py`.
- Enforced inherent query limits and a max cap using clamped LIMIT values (1-100), including paginated URL fetches in `src/blueprints/dashboard.py`.

## Database Hardening (Least Privilege)
- Database credentials are read from environment variables (`DATABASE_URL` or `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`) instead of hard-coded values.
- `.env.example` includes placeholder DB variable names for setup, and `.env` remains ignored in `.gitignore` so secrets are not committed.
- Least-privilege SQL scripts are included in `sql/least_privilege_setup.sql` and `sql/verify_least_privilege.sql` to configure and verify a non-superuser runtime role with only required permissions.

## Python Dependency Graph (pydeps + Graphviz)
- Installed `pydeps` in the project environment and verified Graphviz `dot` is available on the system path.
- Generated the dependency graph as an SVG file using pydeps and saved the output as `dependency.svg` in `module_5/`.

### How to run pydeps
- Inside `module_5` folder, in the terminal, run:
  - `pydeps src/flask_app.py --noshow -T svg -o dependency.svg --max-bacon 2 --include-missing`

## Reproducible Environment + Packaging
- Updated `requirements.txt` so a new environment can install everything required to run the Flask app and analysis features.
- Included development tooling in `requirements.txt`, including `pylint` and `pydeps`, so linting and dependency graph generation are reproducible.
- Added `setup.py` in `module_5/` with package metadata, `src`-based package discovery, Python version requirement, and install dependencies to support packaging and editable installs (`pip install -e .`).

## Fresh Install

### Method 1 (pip)
1. `python -m venv .venv`
2. `.\.venv\Scripts\Activate.ps1`
3. `python -m pip install --upgrade pip`
4. `python -m pip install -r requirements.txt`

### Method 2 (uv)
1. `uv venv .venv`
2. `.\.venv\Scripts\Activate.ps1`
3. `uv pip sync requirements.txt`

## Snyk Analysis
As I ran `snyk test` on `module_5`, it found 2 vulnerabilities:
- Flask 3.1.2 has a security issue where sensitive data might unintentionally be cached. Even though the severity is low, this could potentially expose sensitive information if caching is misconfigured.
- Werkzeug 3.1.5 has security issues: improper request handling, header parsing issues, debugger exposure issues, cache/response handling edge cases, and path normalization or routing edge cases.

I patched these issues by using Flask 3.1.3 and Werkzeug 3.1.6. These new versions corrected the issues.

### How to run Snyk test
- Inside `module_5` folder, in the terminal, run:
  - `snyk test`

## GitHub Actions CI
- Workflow file: `.github/workflows/ci.yml`
- Runs on every push and pull request.
- Enforces Pylint with `--fail-under=10` on `src`.
- Generates `dependency.svg` with `pydeps` + Graphviz and fails if the file is missing.
- Runs `snyk test` in CI (outputs scan results; token-based in GitHub Secrets).
- Runs `pytest` with the PostgreSQL service setup used by this project.

## Read the Docs
- Published docs URL: `https://jhu-software-concepts-nampham.readthedocs.io/en/latest/`

## GitHub Repository
The SSH URL to my GitHub repository:  
`git@github.com:nampham929/jhu_software_concepts.git`

## Known Bugs

## Citations
