"""Position FIFO 仓位管理单元测试"""
import pytest
from datetime import date
from app.backtest.portfolio.position import Position, PositionLot, PositionManager


class TestPositionLot:
    def test_cost_calculation(self):
        lot = PositionLot(trade_date=date(2024, 1, 2), shares=100, cost_price=10.0)
        assert lot.cost == 1000.0

    def test_zero_shares_cost(self):
        lot = PositionLot(trade_date=date(2024, 1, 2), shares=0, cost_price=10.0)
        assert lot.cost == 0.0


class TestPosition:
    def test_buy_creates_lot(self):
        pos = Position(symbol="000300.SH")
        cost = pos.buy(100, 10.0, date(2024, 1, 2))

        assert cost == 1000.0
        assert pos.total_shares == 100
        assert pos.avg_cost == 10.0
        assert len(pos.lots) == 1

    def test_buy_multiple_lots(self):
        pos = Position(symbol="000300.SH")
        pos.buy(100, 10.0, date(2024, 1, 2))
        pos.buy(200, 12.0, date(2024, 1, 3))

        assert pos.total_shares == 300
        assert abs(pos.avg_cost - (1000 + 2400) / 300) < 0.01
        assert len(pos.lots) == 2

    def test_sell_fifo(self):
        pos = Position(symbol="000300.SH")
        pos.buy(100, 10.0, date(2024, 1, 2))
        pos.buy(200, 12.0, date(2024, 1, 3))

        # Sell 150 shares — should deplete the first lot (100) + 50 from second
        sold, pnl = pos.sell(150, 15.0, date(2024, 1, 5))

        assert sold == 150
        # PnL: (15-10)*100 + (15-12)*50 = 500 + 150 = 650
        assert abs(pnl - 650.0) < 0.01
        assert pos.total_shares == 150
        # Remaining: 150 shares from second lot at cost 12
        assert abs(pos.avg_cost - 12.0) < 0.01

    def test_sell_all(self):
        pos = Position(symbol="000300.SH")
        pos.buy(200, 10.0, date(2024, 1, 2))

        sold, pnl = pos.sell(200, 15.0, date(2024, 1, 4))

        assert sold == 200
        assert abs(pnl - 1000.0) < 0.01
        assert pos.total_shares == 0

    def test_sell_more_than_held_raises(self):
        pos = Position(symbol="000300.SH")
        pos.buy(100, 10.0, date(2024, 1, 2))

        with pytest.raises(ValueError, match="Cannot sell"):
            pos.sell(200, 15.0, date(2024, 1, 3))

    def test_t1_lock_prevents_sell_same_day(self):
        pos = Position(symbol="000300.SH")
        pos.buy(100, 10.0, date(2024, 1, 2))

        with pytest.raises(ValueError, match="locked"):
            pos.sell(100, 15.0, date(2024, 1, 2))

    def test_t1_allows_sell_next_day(self):
        pos = Position(symbol="000300.SH")
        pos.buy(100, 10.0, date(2024, 1, 2))

        # Next day should work
        sold, _ = pos.sell(100, 15.0, date(2024, 1, 3))
        assert sold == 100

    def test_close_all(self):
        pos = Position(symbol="000300.SH")
        pos.buy(100, 10.0, date(2024, 1, 2))
        pos.buy(200, 12.0, date(2024, 1, 3))

        realized = pos.close(13.0, date(2024, 1, 5))
        assert abs(realized - (300 + 200)) < 0.01  # (13-10)*100 + (13-12)*200
        assert pos.total_shares == 0

    def test_close_locked_raises(self):
        pos = Position(symbol="000300.SH")
        pos.buy(100, 10.0, date(2024, 1, 2))

        with pytest.raises(ValueError, match="locked"):
            pos.close(13.0, date(2024, 1, 2))

    def test_market_value_tracks_price(self):
        pos = Position(symbol="000300.SH")
        pos.buy(100, 10.0, date(2024, 1, 2))

        pos.update_price(15.0)
        assert pos.market_value == 1500.0
        assert pos.unrealized_pnl == 500.0

    def test_buy_zero_raises(self):
        pos = Position(symbol="000300.SH")
        with pytest.raises(ValueError, match="positive"):
            pos.buy(0, 10.0, date(2024, 1, 2))

    def test_sell_zero_raises(self):
        pos = Position(symbol="000300.SH")
        pos.buy(100, 10.0, date(2024, 1, 2))
        with pytest.raises(ValueError, match="positive"):
            pos.sell(0, 15.0, date(2024, 1, 3))


class TestPositionManager:
    def test_get_or_create(self):
        pm = PositionManager()
        pos = pm.get_or_create("000300.SH")
        assert pos.symbol == "000300.SH"
        assert pos.total_shares == 0

        # Same instance returned
        assert pm.get_or_create("000300.SH") is pos

    def test_update_prices(self):
        pm = PositionManager()
        pm.get_or_create("A").buy(100, 10.0, date(2024, 1, 2))
        pm.get_or_create("B").buy(200, 20.0, date(2024, 1, 2))

        from app.backtest.event.event_source import Bar
        bars = {
            "A": Bar("A", date(2024, 1, 3), {"open": 15, "high": 16, "low": 9, "close": 14, "volume": 10000, "amount": 140000}),
            "B": Bar("B", date(2024, 1, 3), {"open": 25, "high": 26, "low": 19, "close": 24, "volume": 20000, "amount": 480000}),
        }
        pm.update_prices(bars)

        assert pm.positions["A"].market_value == 1400.0
        assert pm.positions["B"].market_value == 4800.0

    def test_remove_empty_cleans_up(self):
        pm = PositionManager()
        pm.get_or_create("A").buy(100, 10.0, date(2024, 1, 2))
        pm.get_or_create("B")  # empty position

        pm.positions["A"].sell(100, 15.0, date(2024, 1, 4))
        pm.remove_empty()

        assert "A" not in pm.positions
        assert "B" not in pm.positions
