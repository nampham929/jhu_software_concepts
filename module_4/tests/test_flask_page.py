from __future__ import annotations

from bs4 import BeautifulSoup
import pytest

import blueprints.dashboard as dashboard
from flask_app import create_app


@pytest.mark.web
def test_app_factory_and_required_routes(monkeypatch):
    # Avoid DB/query dependencies in this route registration smoke test.
    monkeypatch.setattr(
        dashboard,
        "load_query_results",
        lambda: [
            {
                "title": "Analysis Sample",
                "description": "sample",
                "sql": "SELECT 1",
                "columns": ["value"],
                "rows": [[1]],
                "display": "Answer sample",
                "error": None,
            }
        ],
    )
    # Build the app in testing mode using the normal factory.
    app = create_app({"TESTING": True})

    # Collect every registered route path from Flask's URL map.
    paths = {rule.rule for rule in app.url_map.iter_rules()}
    # Ensure the core endpoints required by the assignment are present.
    assert "/" in paths
    assert "/analysis" in paths
    assert "/pull-data" in paths
    assert "/update-analysis" in paths
    assert "/pull-status" in paths


@pytest.mark.web
def test_get_analysis_page_loads_and_renders_required_components(monkeypatch):
    # Mock analysis data so the page can render deterministically in test.
    monkeypatch.setattr(
        dashboard,
        "load_query_results",
        lambda: [
            {
                "title": "Analysis Q",
                "description": "sample",
                "sql": "SELECT 1",
                "columns": ["value"],
                "rows": [[1]],
                "display": "39.28%",
                "error": None,
            }
        ],
    )
    app = create_app({"TESTING": True})

    # Request the analysis page like a browser would.
    with app.test_client() as client:
        response = client.get("/analysis")

    # Basic page-load expectations.
    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert "Analysis" in html
    assert "Answer:" in html

    # Find buttons by stable test ids, then verify visible button labels.
    soup = BeautifulSoup(html, "html.parser")
    pull_btn = soup.select_one('[data-testid="pull-data-btn"]')
    update_btn = soup.select_one('[data-testid="update-analysis-btn"]')

    assert pull_btn is not None
    assert update_btn is not None
    assert "Pull Data" in pull_btn.get_text(strip=True)
    assert "Update Analysis" in update_btn.get_text(strip=True)
