"""Flask application factory for the GradCafe analytics dashboard."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from flask import Flask
from dotenv import load_dotenv

from blueprints.dashboard import configure_dashboard, dashboard_bp


# Load local env vars from project root for local development.
load_dotenv(Path(__file__).resolve().parents[1] / ".env")

# Create the Flask application and register the dashboard blueprint.
def create_app(test_config: dict[str, Any] | None = None) -> Flask:
    """Build and configure the Flask app instance."""
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
    debug_mode = os.getenv("FLASK_DEBUG", "0").strip().lower() in {"1", "true", "yes", "on"}
    create_app().run(debug=debug_mode)
