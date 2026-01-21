import requests


def test_summary_endpoint(base_url, auth_headers):
    response = requests.get(
        f"{base_url}/api/v1/analytics/summary",
        headers=auth_headers,
        timeout=15,
    )
    assert response.status_code == 200
    data = response.json()
    assert "total_visits" in data
    assert "unique_visitors" in data
    assert "conversions" in data
    assert "top_sources" in data
    assert "top_campaigns" in data


def test_page_analytics_endpoint(base_url, auth_headers):
    response = requests.get(
        f"{base_url}/api/v1/analytics/pages",
        headers=auth_headers,
        timeout=15,
    )
    assert response.status_code == 200
    data = response.json()
    assert "page_visits" in data


def test_users_endpoint(base_url, auth_headers):
    response = requests.get(
        f"{base_url}/api/v1/analytics/users?limit=5&offset=0",
        headers=auth_headers,
        timeout=15,
    )
    assert response.status_code == 200
    data = response.json()
    assert "users" in data
    assert "total_count" in data


def test_export_visits_csv(base_url, auth_headers):
    response = requests.get(
        f"{base_url}/api/v1/analytics/export/csv",
        headers=auth_headers,
        timeout=30,
    )
    assert response.status_code == 200
    assert "text/csv" in response.headers.get("content-type", "")


def test_export_events_csv(base_url, auth_headers):
    response = requests.get(
        f"{base_url}/api/v1/analytics/export/events.csv",
        headers=auth_headers,
        timeout=30,
    )
    assert response.status_code == 200
    assert "text/csv" in response.headers.get("content-type", "")


def test_invalid_funnel_config_rejected(base_url, auth_headers):
    invalid_config = {
        "funnels": [
            {
                "key": "invalid",
                "label": "Invalid",
                "steps": [
                    {"label": "Step", "type": "invalid", "path": "/demo"}
                ],
            }
        ]
    }
    response = requests.put(
        f"{base_url}/api/v1/analytics/funnels/config",
        headers=auth_headers,
        json=invalid_config,
        timeout=15,
    )
    assert response.status_code == 422


def test_funnel_timing_endpoint(base_url, auth_headers):
    summary = requests.get(
        f"{base_url}/api/v1/analytics/funnels",
        headers=auth_headers,
        timeout=15,
    ).json()
    funnel_key = summary["funnels"][0]["key"]

    response = requests.get(
        f"{base_url}/api/v1/analytics/funnels/{funnel_key}/timing",
        headers=auth_headers,
        timeout=15,
    )
    assert response.status_code == 200
    data = response.json()
    assert "transitions" in data


def test_funnel_dropoffs_endpoint(base_url, auth_headers):
    summary = requests.get(
        f"{base_url}/api/v1/analytics/funnels",
        headers=auth_headers,
        timeout=15,
    ).json()
    funnel_key = summary["funnels"][0]["key"]

    response = requests.get(
        f"{base_url}/api/v1/analytics/funnels/{funnel_key}/dropoffs?step=0",
        headers=auth_headers,
        timeout=15,
    )
    assert response.status_code == 200
    data = response.json()
    assert "users" in data
