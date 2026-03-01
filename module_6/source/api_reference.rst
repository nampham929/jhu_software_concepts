API Reference
=============

Scraping API
------------

.. automodule:: app.scrape_support
   :members:
   :undoc-members:

Cleaning API
------------

.. automodule:: app.data_cleaning
   :members:
   :undoc-members:

Load Data API
-------------

.. automodule:: load_data
   :members:
   :undoc-members:

Query API
---------

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
