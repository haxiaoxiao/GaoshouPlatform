from datetime import date

import pandas as pd
import pytest
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

from app.db.models.base import Base
from app.db.models.financial import FinancialData
from app.services.financial_pit_store import FinancialPITStore


@pytest.mark.asyncio
async def test_financial_pit_store_uses_announcement_date_and_latest_relay_statement(tmp_path):
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    async with session_factory() as session:
        session.add_all([
            FinancialData(
                symbol="600519.SH",
                report_date=date(2024, 3, 31),
                ann_date=date(2024, 4, 30),
                report_type="Q1",
                revenue=100.0,
                net_profit=30.0,
                roe=12.0,
            ),
            FinancialData(
                symbol="600519.SH",
                report_date=date(2024, 6, 30),
                ann_date=date(2024, 8, 30),
                report_type="H1",
                revenue=220.0,
                net_profit=80.0,
                roe=18.0,
            ),
        ])
        await session.commit()

        income_dir = tmp_path / "financial_income" / "year=2024" / "month=04"
        income_dir.mkdir(parents=True)
        pd.DataFrame([
            {
                "symbol": "600519.SH",
                "f_ann_date": date(2024, 4, 30),
                "end_date": date(2024, 3, 31),
                "total_revenue": 1000.0,
            },
            {
                "symbol": "600519.SH",
                "f_ann_date": date(2024, 8, 30),
                "end_date": date(2024, 6, 30),
                "total_revenue": 2200.0,
            },
        ]).to_parquet(income_dir / "part-test.parquet", index=False)

        store = FinancialPITStore(session, data_dir=str(tmp_path))
        snapshots = await store.load_snapshots(["600519.SH"], as_of_date=date(2024, 5, 15))

    await engine.dispose()

    snapshot = snapshots["600519.SH"]
    assert snapshot.report_date == date(2024, 3, 31)
    assert snapshot.ann_date == date(2024, 4, 30)
    assert snapshot.sqlite_fields["revenue"] == 100.0
    assert snapshot.statements["income"]["total_revenue"] == 1000.0
    assert snapshot.statements["income"]["end_date"] == "2024-03-31"
