from __future__ import annotations

from bs4 import BeautifulSoup
import pytest

import blueprints.dashboard as dashboard
from flask_app import create_app


@pytest.mark.web
def test_app_factory_and_required_routes(monkeypatch):
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
    app = create_app({"TESTING": True})

    paths = {rule.rule for rule in app.url_map.iter_rules()}
    assert "/" in paths
    assert "/analysis" in paths
    assert "/pull-data" in paths
    assert "/update-analysis" in paths
    assert "/pull-status" in paths


@pytest.mark.web
def test_get_analysis_page_loads_and_renders_required_components(monkeypatch):
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

    with app.test_client() as client:
        response = client.get("/analysis")

    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert "Analysis" in html
    assert "Answer:" in html

    soup = BeautifulSoup(html, "html.parser")
    pull_btn = soup.select_one('[data-testid="pull-data-btn"]')
    update_btn = soup.select_one('[data-testid="update-analysis-btn"]')

    assert pull_btn is not None
    assert update_btn is not None
    assert "Pull Data" in pull_btn.get_text(strip=True)
    assert "Update Analysis" in update_btn.get_text(strip=True)
