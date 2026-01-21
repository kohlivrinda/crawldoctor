import os
import time
from typing import Dict, Any

import requests


BASE_URL = os.getenv("CRAWLDOCTOR_TEST_BASE_URL", "http://localhost:8000")
USERNAME = os.getenv("CRAWLDOCTOR_TEST_USERNAME", "admin")
PASSWORD = os.getenv("CRAWLDOCTOR_TEST_PASSWORD", "admin123")


def _wait_for_api(timeout: int = 60) -> None:
    start = time.time()
    while time.time() - start < timeout:
        try:
            response = requests.get(f"{BASE_URL}/health", timeout=5)
            if response.status_code == 200:
                return
        except requests.RequestException:
            time.sleep(2)
    raise RuntimeError("API did not become healthy in time")


def _login() -> str:
    response = requests.post(
        f"{BASE_URL}/api/v1/auth/login",
        json={"username": USERNAME, "password": PASSWORD},
        timeout=10,
    )
    response.raise_for_status()
    data = response.json()
    return data["access_token"]


def _headers(token: str) -> Dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def test_funnel_config_roundtrip() -> None:
    _wait_for_api()
    token = _login()

    response = requests.get(
        f"{BASE_URL}/api/v1/analytics/funnels/config",
        headers=_headers(token),
        timeout=10,
    )
    response.raise_for_status()
    config = response.json()

    assert "funnels" in config
    assert len(config["funnels"]) >= 1

    custom_config = {
        "funnels": [
            {
                "key": "custom_demo",
                "label": "Landing → /demo → Submit",
                "steps": [
                    {"label": "Visited /demo", "type": "page", "path": "/demo"},
                    {"label": "Submitted form", "type": "event", "path": "/demo", "event_type": "form_submit"},
                ],
            }
        ]
    }

    update_resp = requests.put(
        f"{BASE_URL}/api/v1/analytics/funnels/config",
        headers=_headers(token),
        json=custom_config,
        timeout=10,
    )
    update_resp.raise_for_status()

    config_after = requests.get(
        f"{BASE_URL}/api/v1/analytics/funnels/config",
        headers=_headers(token),
        timeout=10,
    ).json()

    def normalize(config: Dict[str, Any]) -> Dict[str, Any]:
        normalized = {"funnels": []}
        for funnel in config.get("funnels", []):
            steps = []
            for step in funnel.get("steps", []):
                item = {
                    "label": step.get("label"),
                    "type": step.get("type"),
                    "path": step.get("path"),
                }
                if step.get("type") == "event":
                    item["event_type"] = step.get("event_type")
                steps.append(item)
            normalized["funnels"].append({
                "key": funnel.get("key"),
                "label": funnel.get("label"),
                "steps": steps,
            })
        return normalized

    assert normalize(config_after) == normalize(custom_config)


def test_funnel_summary_matches_config() -> None:
    _wait_for_api()
    token = _login()

    summary = requests.get(
        f"{BASE_URL}/api/v1/analytics/funnels",
        headers=_headers(token),
        timeout=10,
    ).json()

    assert "funnels" in summary
    assert isinstance(summary["funnels"], list)
    assert summary["funnels"]

    for funnel in summary["funnels"]:
        assert "label" in funnel
        assert "stages" in funnel
        assert len(funnel["stages"]) >= 1
        assert "rates" in funnel
        for stage in funnel["stages"]:
            assert "label" in stage
            assert "count" in stage
            assert isinstance(stage["count"], int)
        for rate in funnel["rates"]:
            assert "label" in rate
            assert "rate" in rate
            assert "dropoff_count" in rate


def test_funnel_config_validation() -> None:
    _wait_for_api()
    token = _login()

    invalid_config = {
        "funnels": [
            {
                "key": "invalid",
                "label": "Invalid",
                "steps": [
                    {"label": "Broken", "type": "invalid", "path": "/demo"}
                ],
            }
        ]
    }

    response = requests.put(
        f"{BASE_URL}/api/v1/analytics/funnels/config",
        headers=_headers(token),
        json=invalid_config,
        timeout=10,
    )
    assert response.status_code == 422
