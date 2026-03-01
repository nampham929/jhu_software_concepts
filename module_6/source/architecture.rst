Architecture
============

Web Layer
---------

- ``src/web/app/flask_app.py`` creates and configures the Flask app.
- ``src/web/app/blueprints/dashboard.py`` defines routes and shared pull-job state.
- Templates and static assets are under ``src/web/app/templates`` and ``src/web/app/static``.

ETL Layer
---------

- ``src/module_2/scrape.py`` scrapes pages and parses applicant rows.
- ``src/module_2/clean.py`` normalizes and cleans scraped records.
- ``src/db/load_data.py`` creates the schema and data-loading helpers for PostgreSQL.

Database and Analysis Layer
---------------------------

- PostgreSQL stores applicant data in the ``applicants`` table.
- ``src/web/app/query_data.py`` defines SQL queries and display formatting logic.
- ``dashboard`` routes call query helpers to render analysis output.

Operational Flow
----------------

1. ``/pull-data`` starts a pull job.
2. Scrape and clean pipeline collects new records.
3. New records are inserted into PostgreSQL with duplicate checks.
4. ``/analysis`` renders query summaries from database results.


