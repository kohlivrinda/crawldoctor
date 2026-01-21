import time

import requests


def _timed_request(url, headers, timeout=60):
    start = time.perf_counter()
    response = requests.get(url, headers=headers, timeout=timeout)
    duration = time.perf_counter() - start
    return response, duration


def test_export_visits_performance(base_url, auth_headers):
    response, duration = _timed_request(
        f"{base_url}/api/v1/analytics/export/csv",
        headers=auth_headers,
        timeout=60,
    )
    assert response.status_code == 200
    assert "text/csv" in response.headers.get("content-type", "")
    assert duration < 15


def test_export_events_performance(base_url, auth_headers):
    response, duration = _timed_request(
        f"{base_url}/api/v1/analytics/export/events.csv",
        headers=auth_headers,
        timeout=60,
    )
    assert response.status_code == 200
    assert "text/csv" in response.headers.get("content-type", "")
    assert duration < 15
