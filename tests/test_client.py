from __future__ import annotations

from datetime import date

import httpx
import respx

from agsi_pipeline.client import AgsiClient


@respx.mock
def test_fetch_day_uses_date_param_and_api_key() -> None:
    route = respx.get("https://agsi.gie.eu/api").mock(
        return_value=httpx.Response(200, json={"gas_day": "2026-07-21", "data": []})
    )
    client = AgsiClient(base_url="https://agsi.gie.eu", api_key="secret-key")
    status, _ = client.fetch_day(date(2026, 7, 21))
    assert status == 200
    assert route.called
    request = route.calls[0].request
    assert request.url.params["date"] == "2026-07-21"
    assert request.headers["x-key"] == "secret-key"
    client.close()
