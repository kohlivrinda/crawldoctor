import time
from datetime import datetime

import requests


def _post_event(base_url, payload):
    response = requests.post(
        f"{base_url}/track/event",
        json=payload,
        timeout=10,
        headers={"User-Agent": "Mozilla/5.0"},
    )
    assert response.status_code == 200


def test_journey_attribution_seeded(base_url, auth_headers):
    client_id = f"test-client-{int(time.time())}"
    page_url = "https://example.com/demo?utm_source=google&utm_medium=cpc&utm_campaign=demo"

    _post_event(base_url, {
        "event_type": "page_view",
        "page_url": page_url,
        "referrer": "https://google.com",
        "cid": client_id,
    })

    _post_event(base_url, {
        "event_type": "form_submit",
        "page_url": "https://example.com/demo",
        "referrer": page_url,
        "cid": client_id,
        "data": {"form_values": {"email": "test@example.com"}},
    })

    time.sleep(1.0)

    response = requests.get(
        f"{base_url}/api/v1/analytics/users/{client_id}",
        headers=auth_headers,
        timeout=15,
    )
    assert response.status_code == 200
    data = response.json()

    assert data["attribution"]["source"] == "google"
    assert data["attribution"]["medium"] == "cpc"
    assert data["attribution"]["campaign"] == "demo"
    assert data["summary"]["conversions"] >= 1
