from __future__ import annotations

import os
from typing import Any

from flask import Flask

from blueprints.dashboard import configure_dashboard, dashboard_bp


# Create the Flask application and register the dashboard blueprint.
def create_app(test_config: dict[str, Any] | None = None) -> Flask:
    app = Flask(__name__)
    app.config.from_mapping(
        DATABASE_URL=os.getenv("DATABASE_URL"),
        RUN_PULL_IN_BACKGROUND=True,
        TESTING=False,
        PULL_RUNNER=None,
    )

    if test_config:
        app.config.update(test_config)

    configure_dashboard(app.config)
    app.register_blueprint(dashboard_bp)
    return app


if __name__ == "__main__":  # pragma: no cover
    create_app().run(debug=True)



