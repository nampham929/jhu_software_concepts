Overview and Setup
==================

Overview
--------

This module provides a Flask dashboard and data pipeline for GradCafe applicant data.
The app supports data pulling, analysis updates, and rendered query results.

Environment Variables
---------------------

Application and scripts support two configuration styles:

- Preferred: ``DATABASE_URL``
- Fallback fields: ``DB_NAME``, ``DB_USER``, ``DB_PASSWORD``, ``DB_HOST``, ``DB_PORT``

Testing uses:

- ``TEST_DATABASE_URL`` for DB/integration tests
- ``DATABASE_URL`` must be different from ``TEST_DATABASE_URL``

Run the Application
-------------------

From ``module_4``:

.. code-block:: bash

   python src/flask_app.py

Run Tests
---------

From ``module_4``:

.. code-block:: bash

   pytest

Run by marker:

.. code-block:: bash

   pytest -m web
   pytest -m buttons
   pytest -m analysis
   pytest -m db
   pytest -m integration

