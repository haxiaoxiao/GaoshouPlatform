import pytest
from httpx import ASGITransport, AsyncClient

from app.api import data_explorer
from app.main import app

PREVIEW_PATH = "/api/explorer/tables/{table_name}/preview"


@pytest.mark.asyncio
async def test_data_explorer_does_not_expose_arbitrary_sql_query_route():
    paths = app.openapi()["paths"]

    assert "post" not in paths.get("/api/explorer/query", {})
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        response = await client.post("/api/explorer/query")
    assert response.status_code == 404

    assert "get" in paths["/api/explorer/tables"]
    assert "post" in paths["/api/explorer/tables/{table_name}/search"]


def test_data_explorer_preview_schema_does_not_accept_raw_where_sql():
    parameters = app.openapi()["paths"][PREVIEW_PATH]["get"].get("parameters", [])
    query_parameters = {
        parameter["name"]
        for parameter in parameters
        if parameter.get("in") == "query"
    }

    assert "where" not in query_parameters


@pytest.mark.asyncio
async def test_data_explorer_preview_rejects_unknown_raw_where_parameter():
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        response = await client.get(
            "/api/explorer/tables/__missing_dataset__/preview",
            params={"where": "true; SELECT 42"},
        )

    assert response.status_code == 422
    assert any(
        error["loc"][-1] == "where" and error["type"] == "extra_forbidden"
        for error in response.json()["detail"]
    )


def test_data_explorer_quotes_schema_identifiers_with_embedded_double_quotes():
    column = 'price"; DROP TABLE research; --'

    quoted = data_explorer._quoted_identifier(column, backend="parquet")
    filter_sql = data_explorer._build_filter_sql(
        data_explorer.ExplorerFilter(column=column, op="=", value="10' OR TRUE --"),
        [column],
        backend="parquet",
    )

    assert quoted == '"price""; DROP TABLE research; --"'
    assert filter_sql == '"price""; DROP TABLE research; --" = \'10\'\' OR TRUE --\''
