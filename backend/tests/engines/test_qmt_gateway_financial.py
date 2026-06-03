from __future__ import annotations

from datetime import date

import pandas as pd

from app.engines.qmt_gateway import QMTGateway


def test_parse_financial_dataframes_extracts_qmt_announcement_date() -> None:
    tables = {
        "PershareIndex": pd.DataFrame({
            "m_timetag": ["20240331"],
            "m_anntime": ["20240425"],
            "s_fa_eps_basic": [1.23],
            "du_return_on_equity": [12.5],
        }),
        "Income": pd.DataFrame({
            "m_timetag": ["20240331"],
            "m_anntime": ["20240426"],
            "revenue_inc": [100_000.0],
            "inc_revenue_rate": [15.0],
            "du_profit_rate": [20.0],
            "sales_gross_profit": [35.0],
        }),
    }

    quarters = QMTGateway._parse_financial_dataframes("000001.SZ", tables, report_count=1)

    assert len(quarters) == 1
    assert quarters[0].report_date == date(2024, 3, 31)
    assert quarters[0].ann_date == date(2024, 4, 26)
    assert quarters[0].revenue_yoy == 15.0
