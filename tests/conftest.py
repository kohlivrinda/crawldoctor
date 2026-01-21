import os
import time

import pytest
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
    return response.json()["access_token"]


@pytest.fixture(scope="session")
def base_url() -> str:
    return BASE_URL


@pytest.fixture(scope="session")
def auth_token() -> str:
    _wait_for_api()
    return _login()


@pytest.fixture(scope="session")
def auth_headers(auth_token: str) -> dict:
    return {"Authorization": f"Bearer {auth_token}"}
