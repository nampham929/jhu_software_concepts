Testing Guide
=============

Markers
-------

Project markers are defined in ``pytest.ini``:

- ``web``: page load and HTML structure tests
- ``buttons``: endpoint and busy-state behavior tests
- ``analysis``: analysis formatting and label tests
- ``db``: schema, insert, and query tests
- ``integration``: end-to-end flows

Run a marker group:

.. code-block:: bash

   pytest -m db

Selectors
---------

UI tests use stable selectors for button assertions, including:

- ``[data-testid="pull-data-btn"]``
- ``[data-testid="update-analysis-btn"]``

Fixtures
--------

Key fixtures in ``tests/conftest.py``:

- ``db_url``: validates and returns ``TEST_DATABASE_URL``
- ``reset_pull_state``: clears shared dashboard pull state per test
- ``block_db_in_non_db_tests``: blocks accidental live DB access for non-DB tests
- ``fake_applicant_row`` and ``insert_row_tuple``: test data helpers
- ``mock_*`` fixtures: in-memory database behavior for non-integration DB logic

Test Isolation Notes
--------------------

- Non-DB tests are forced to use fakes/mocks.
- DB and integration tests explicitly opt in via markers.
- Pull-state reset fixture prevents cross-test state leakage.

