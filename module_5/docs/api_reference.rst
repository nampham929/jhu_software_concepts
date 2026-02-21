API Reference
=============

Scraping API
------------

This API scrapes GradCafe survey pages and converts each page into structured applicant records for downstream cleaning and loading.

.. automodule:: module_2.scrape
   :members:
   :undoc-members:

Cleaning API
------------

This API cleans scraped applicant records by normalizing text fields, removing noise, and standardizing values for storage.

.. automodule:: module_2.clean
   :members:
   :undoc-members:

Load Data API
-------------

This API creates the target database schema and loads cleaned applicant JSONL records into PostgreSQL with validation and error handling.

.. automodule:: load_data
   :members:
   :undoc-members:

Query API
---------

This API defines and executes analytics queries against the applicants table and formats results for CLI and dashboard display.

.. automodule:: query_data
   :members:
   :undoc-members:

Flask App Factory
-----------------

.. automodule:: flask_app
   :members:
   :undoc-members:

Dashboard and Routes
--------------------

.. automodule:: blueprints.dashboard
   :members:
   :undoc-members:

Route Summary
-------------

- ``GET /``: dashboard alias.
- ``GET /analysis``: renders analysis page.
- ``POST /pull-data``: starts pull job (foreground or background).
- ``POST /update-analysis``: refresh trigger endpoint.
- ``GET /pull-status``: pull status snapshot for polling.



