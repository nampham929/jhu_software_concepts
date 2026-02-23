1. Name: Nam Pham - JHED ID: npham21
2. Module Info: Module 5: Pytest and Sphinx - Due Date: February 23, 2026
3. Approach:

This assignment is to practice with software assurance workflows: input validation, static analysis, dependency analysis, virtual environments, supply-chain scanning, and least-privilege database configuration.


The SSH URL to my GitHub repository:
git@github.com:nampham929/jhu_software_concepts.git


How to run the program:
-Install the required packages in the requirements.txt file

Fresh Install:
- Method 1 (pip):
  1) python -m venv .venv
  2) .\.venv\Scripts\Activate.ps1
  3) python -m pip install --upgrade pip
  4) python -m pip install -r requirements.txt

- Method 2 (uv):
  1) uv venv .venv
  2) .\.venv\Scripts\Activate.ps1
  3) uv pip sync requirements.txt

Note: uv pip sync makes the environment match requirements.txt exactly, which helps reproducibility.

How to run Pylint:
- Inside module_5 folder, in the terminal, run .\.venv\Scripts\python.exe -m pylint src

How to run pydeps:
 - Inside module_5 folder, in the terminal, run pydeps src/flask_app.py --noshow -T svg -o dependency.svg --max-bacon 2 --include-missing

Least-Privilege DB User Setup:
- SQL setup script: sql/least_privilege_setup.sql
- SQL verification script: sql/verify_least_privilege.sql
- This project uses a non-superuser app role for runtime access.

Permissions granted to app role and why:
- CONNECT on database: required to open a DB session.
- USAGE on schema public: required to access objects in schema.
- SELECT on public.applicants: required for dashboard/query reads.
- INSERT on public.applicants: required for pull/load write paths.
- USAGE, SELECT on public.applicants_p_id_seq: required for SERIAL PK inserts.

Permissions intentionally not granted:
- No SUPERUSER, CREATEDB, CREATEROLE, REPLICATION.
- No DROP, ALTER, or ownership grants.

Short SQL snippet (for report/PDF):
CREATE ROLE <APP_ROLE> LOGIN PASSWORD '<APP_PASSWORD>' NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
GRANT CONNECT ON DATABASE <DB_NAME> TO <APP_ROLE>;
GRANT USAGE ON SCHEMA public TO <APP_ROLE>;
GRANT SELECT, INSERT ON TABLE public.applicants TO <APP_ROLE>;
GRANT USAGE, SELECT ON SEQUENCE public.applicants_p_id_seq TO <APP_ROLE>;

Snyk Analysis:
As I ran snyk test on module_5, it found 2 vulnerabilities:
  Flask 3.1.2 has a security issue where sensitive data might unintentionally be cached. Even though the severity is Low, this could potentially expose sensitive information if caching is misconfigured.
  Werkzeug 3.1.5 has security issues: Improper request handling, Header parsing issues, Debugger exposure issues, Cache / response handling edge cases, and Path normalization or routing edge cases.

I patched these issues by using Flask 3.1.3 and Werkzeug 3.1.6. These new versions corrrected the issues.

4. Known Bugs: 

5. Citations:

