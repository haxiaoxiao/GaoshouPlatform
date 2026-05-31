from app.services.tushare_relay import parse_relay_rows
from app.services.tushare_relay_sync import STRUCTURED_RELAY_DATASETS, build_sync_catalog


def test_parse_native_tushare_envelope() -> None:
    payload = {
        "code": 0,
        "data": {
            "fields": ["ts_code", "trade_date", "adj_factor"],
            "items": [["000001.SZ", "20240506", 0.8672]],
        },
    }

    assert parse_relay_rows(payload) == [
        {"ts_code": "000001.SZ", "trade_date": "20240506", "adj_factor": 0.8672}
    ]


def test_sync_catalog_exposes_relay_guardrails() -> None:
    catalog = build_sync_catalog()
    dataset_names = {item["name"] for item in catalog["datasets"]}
    presets = {item["name"]: item for item in catalog["presets"]}

    assert set(STRUCTURED_RELAY_DATASETS).issubset(dataset_names)
    assert "ths_concept" in dataset_names
    assert "dividend" not in dataset_names
    assert presets["relay_structured"]["relay_datasets"] == list(STRUCTURED_RELAY_DATASETS)
    assert presets["relay_text"]["include_by_default"] is False
    assert catalog["guardrails"]["news_default_daily_limit"] == 200
