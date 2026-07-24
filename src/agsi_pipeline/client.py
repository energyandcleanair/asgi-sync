from __future__ import annotations

from datetime import date

import httpx

from agsi_pipeline.models import InvalidResponseError


class AgsiClient:
    def __init__(
        self,
        *,
        base_url: str,
        api_key: str,
        timeout: float = 60.0,
        client: httpx.Client | None = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._api_key = api_key
        self._owns_client = client is None
        self._client = client or httpx.Client(timeout=timeout)

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> AgsiClient:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    def fetch_day(self, gas_day: date) -> tuple[int, bytes]:
        if not self._api_key:
            raise InvalidResponseError("API key is not configured")
        url = f"{self._base_url}/api"
        response = self._client.get(
            url,
            params={"date": gas_day.isoformat()},
            headers={"x-key": self._api_key},
        )
        return response.status_code, response.content
