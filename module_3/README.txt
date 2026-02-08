Graduate Admissions Query Dashboard (Module 3)
=================================================

Overview
--------
This project loads GradCafe-style applicant data into PostgreSQL and presents
11 analysis queries in a Flask dashboard. It includes:
- A loader to import JSONL data into a PostgreSQL table.
- A CLI query runner for the same SQL questions.
- A Flask dashboard with a "Pull Data" workflow that scrapes new pages via
  module_2 and appends only new entries.

Key Files
---------
- flask_app.py: Starts the Flask app and registers the dashboard blueprint.
- blueprints/dashboard.py: Dashboard routes, pull workflow, and query execution.
- load_data.py: Creates the applicants table and loads JSONL data.
- query_data.py: CLI runner that prints query results.
- queries.py: The 11 SQL questions shown in the dashboard and CLI.
- templates/dashboard.html, static/css/dashboard.css: UI for the dashboard.
- llm_extend_applicant_data.jsonl: Input dataset for the loader.

Requirements
------------
- Python 3.10+ recommended
- PostgreSQL (local instance)

Install dependencies:
    pip install -r requirements.txt

Database Setup
--------------
This project expects a PostgreSQL database named "postgres" by default.
Credentials are hard-coded in these files and should be updated for your
local setup:
- load_data.py
- query_data.py
- blueprints/dashboard.py (DB_CONFIG)

If you change credentials, update all three places to keep them in sync.

Load Initial Data
-----------------
1) Ensure PostgreSQL is running.
2) Run the loader:
    python load_data.py

This will create the "applicants" table and load the JSONL file into it.

Run CLI Queries
---------------
To print the 11 questions and answers in the terminal:
    python query_data.py

Run the Dashboard
-----------------
Start the Flask app:
    python flask_app.py

Then open http://127.0.0.1:5000/ in your browser.

Pull Data Workflow
------------------
The "Pull Data" button scrapes new pages using module_2 (scrape + clean),
then inserts only new rows (by URL) into the applicants table.
It also writes:
- last_pull.json: last page scraped
- new_data.json: entries inserted in the most recent pull

If you have not installed module_2 dependencies, install them using its
requirements.txt in module_2/.

Notes
-----
- The dashboard refreshes query results on page load.
- Progress updates during pull are recorded in batches of 100 records.
