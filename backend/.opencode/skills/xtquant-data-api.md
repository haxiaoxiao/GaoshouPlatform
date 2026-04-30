# XtQuant Data API Skill

## Overview
xtquant.xtdata is the data module in the xtquant library, providing market data (historical & real-time K-lines and ticks), financial statements, contract info, sector classification, etc. It connects to MiniQMT locally — all data must be **downloaded first** (`download_*`), then **queried** (`get_*`).

## Architecture
- MiniQMT must be running before any API call
- `download_*` functions are **synchronous** and block until completion
- `get_*` functions read from local cache (previously downloaded data)
- Data flow: QMT Server → `download_*` → Local .DAT files → `get_*` → Python
- Our platform adds: `get_*` → ClickHouse → Clean local .DAT cache

## Period Values
| Value | Meaning |
|-------|---------|
| `tick` | Per-trade tick data |
| `1m` | 1-minute K-line |
| `3m` | 3-minute K-line |
| `5m` | 5-minute K-line |
| `15m` | 15-minute K-line |
| `30m` | 30-minute K-line |
| `1h` | 1-hour K-line |
| `1d` | Daily K-line |
| `1w` | Weekly K-line |
| `1mon` | Monthly K-line |
| `1q` | Quarterly K-line |
| `1hy` | Half-yearly K-line |
| `1y` | Yearly K-line |

## Dividend Adjustment Types
| Value | Meaning |
|-------|---------|
| `none` | No adjustment (raw prices) |
| `front` | Forward adjustment |
| `back` | Backward adjustment |
| `front_ratio` | Ratio-based forward adjustment |
| `back_ratio` | Ratio-based backward adjustment |

---

## API Reference

### Download Functions (synchronous, must call before get)

#### `download_history_data(stock_code, period, start_time='', end_time='', incrementally=None)`
Download historical market data for a single stock.
- `stock_code`: str — e.g. `'600000.SH'`
- `period`: str — K-line period or `'tick'`
- `start_time`: str — format `YYYYMMDD` or `YYYYMMDDhhmmss`; empty = incremental
- `end_time`: str — same format
- `incrementally`: `None` = auto (empty start_time → incremental), `True` = incremental, `False` = full
- **Blocks until complete, returns None**

#### `download_history_data2(stock_list, period, start_time='', end_time='', callback=None, incrementally=None)`
Batch version of download_history_data.
- `stock_list`: list — e.g. `['600000.SH', '000001.SZ']`
- `callback`: function `on_progress(data)` where `data = {'total': int, 'finished': int, 'stockcode': str, 'message': str}`
- **Blocks until complete, returns None**

#### `download_financial_data(stock_list, table_list=[])`
Download financial statement data — **WARNING: this function hangs/blocks indefinitely on miniQMT. Use `download_financial_data2` instead.**

#### `download_financial_data2(stock_list, table_list=[], start_time='', end_time='', callback=None)`
**Preferred** batch version of download_financial_data. Returns quickly with callback progress.
- `stock_list`: list of stock codes (supports batch, e.g. 200 stocks at once)
- `table_list`: list of table names (default: `[]` = all). Options:
  - `'Balance'` — Balance sheet
  - `'Income'` — Income statement
  - `'CashFlow'` — Cash flow statement
  - `'Capital'` — Share capital table
  - `'Holdernum'` — Shareholder count
  - `'Top10holder'` — Top 10 shareholders
  - `'Top10flowholder'` — Top 10 floating shareholders
  - `'PershareIndex'` — Per-share indicators (note: lowercase 'i' in 'Index')
- `start_time`/`end_time`: filter by disclosure date (`m_anntime`)
- `callback`: `on_progress(data)` with `{'total', 'finished', 'stockcode', 'message'}`
- **Returns quickly (~0.1-0.5s for single stock), works reliably on miniQMT**

#### `download_sector_data()`
Download sector classification info. **Blocks until complete.**

#### `download_index_weight()`
Download index constituent weight data. **Blocks until complete.**

#### `download_history_contracts()`
Download delisted/expired contract info. **Blocks until complete.**

#### `download_cb_data()`
Download convertible bond info. **Blocks until complete.**

#### `download_etf_info()`
Download ETF redemption info. **Blocks until complete.**

#### `download_holiday_data()`
Download holiday data. **Blocks until complete.**

---

### Market Data Get Functions

#### `get_market_data(field_list=[], stock_list=[], period='1d', start_time='', end_time='', count=-1, dividend_type='none', fill_data=True)`
Get market data from local cache (must download first).
- `field_list`: list — e.g. `['open','close','volume']`, `[]` = all fields
- `stock_list`: list — e.g. `['600000.SH']`
- `period`: str — K-line period
- `start_time`/`end_time`: str — `YYYYMMDD` or `YYYYMMDDhhmmss`
- `count`: int — number of bars; `>0` = limit, `0` = none, `-1` = all
- **Returns** (K-line): `dict {field: pd.DataFrame}` — DataFrame index=stock_list, columns=time
- **Returns** (tick): `dict {stock: np.ndarray}` — sorted by time

#### `get_market_data_ex(field_list=[], stock_list=[], period='1d', start_time='', end_time='', count=-1, dividend_type='none', fill_data=True)`
Extended version of get_market_data. Returns DataFrames per stock.
- **Returns**: `dict {stock_code: pd.DataFrame}` — each DataFrame has time as index, fields as columns

#### `get_local_data(field_list=[], stock_list=[], period='1d', start_time='', end_time='', count=-1, dividend_type='none', fill_data=True, data_dir=None)`
Same as get_market_data but reads directly from local file path. Useful when MiniQMT is not running.
- `data_dir`: str — path to `userdata_mini/datadir`, defaults to auto-detected path
- Level-1 data only

#### `get_full_tick(code_list)`
Get real-time tick data for all stocks.
- `code_list`: list — market codes `['SH','SZ']` or contract codes `['600000.SH']`
- **Returns**: `dict {stock: data_dict}` with keys: `lastPrice, open, high, low, lastClose, amount, volume, pvolume, stockStatus, openInt, lastSettlementPrice, askPrice, bidPrice, askVol, bidVol, transactionNum, totalValue, floatValue`

#### `get_full_kline(field_list=[], stock_list=[], period='1m', start_time='', end_time='', count=1, dividend_type='none', fill_data=True)`
Get latest trading day's K-line data (push data, no need to download).
- `count` defaults to 1
- Only supports latest trading day, no historical data

---

### Financial Data Get Functions

#### `get_financial_data(stock_list, table_list=[], start_time='', end_time='', report_type='report_time')`
Get financial data from local cache (must download first).
- `stock_list`: list — e.g. `['600000.SH']`
- `table_list`: list — table names (see `download_financial_data` for list)
- `start_time`/`end_time`: str — time range filter
- `report_type`: `'report_time'` = by deadline date, `'announce_time'` = by disclosure date
- **Returns**: `dict {stock: {table: pd.DataFrame}}`

**IMPORTANT**: On miniQMT, `get_financial_data` returns **pandas DataFrames** (not nested dicts). Each table is a DataFrame with `m_timetag` (str like '20240331') as a column (not the index). The `m_timetag` column contains string dates, NOT integers — always compare as strings.

```python
# Correct usage:
fnd = xt.get_financial_data(['600051.SH'], ['PershareIndex','Balance','Income','Capital'], start_time='20240101')
# Returns: {'600051.SH': {'PershareIndex': DataFrame, 'Balance': DataFrame, ...}}
# Each DataFrame has m_timetag column (str dtype) and m_anntime column
# Filter by: df[df['m_timetag'] == '20240331']
```

---

### Contract/Instrument Info Functions

#### `get_instrument_detail(stock_code, iscomplete=False)`
Get contract basic info.
- `iscomplete=False`: returns core fields (ExchangeID, InstrumentName, PreClose, FloatVolume, TotalVolume, etc.)
- `iscomplete=True`: returns all fields including RegisteredCapital, MaxLimitOrderVolume, HSGTFlag, etc.
- **Returns**: dict or None if not found

#### `get_instrument_type(stock_code)`
Get contract type.
- **Returns**: `{'index': bool, 'stock': bool, 'fund': bool, 'etf': bool}`

#### `get_trading_dates(market, start_time='', end_time='', count=-1)`
Get trading date list.
- `market`: str — e.g. `'SH'`, `'SZ'`
- **Returns**: list of timestamps

#### `get_trading_calendar(market, start_time='', end_time='')`
Get trading calendar (requires `download_holiday_data` first).
- **Returns**: list of trading day strings

#### `get_trading_time(market)`
Get trading time ranges for a market.

---

### Sector/Index Functions

#### `get_sector_list()`
Get all sector names (requires `download_sector_data` first).
- **Returns**: list of sector name strings

#### `get_stock_list_in_sector(sector_name)`
Get stocks in a sector (requires `download_sector_data` first).
- **Returns**: list of stock codes

#### `get_index_weight(index_code)`
Get index constituent weights (requires `download_index_weight` first).
- **Returns**: `dict {stock: weight}`

---

### Other Functions

#### `get_divid_factors(stock_code, start_time='', end_time='')`
Get ex-rights/dividend data.
- **Returns**: pd.DataFrame

#### `get_holidays()`
Get holiday dates (8-digit strings). Requires `download_holiday_data`.

#### `get_period_list()`
Returns available period list.

#### `get_cb_info(stockcode)`
Get convertible bond info (requires `download_cb_data`).

#### `get_ipo_info(start_time, end_time)`
Get IPO info for given date range.
- **Returns**: list of dicts with `securityCode, codeName, market, actIssueQty, onlineIssueQty, onlineSubCode, onlineSubMaxQty, publishPrice, isProfit, industryPe, afterPE`

#### `get_etf_info()`
Get ETF redemption data (requires `download_etf_info`).

#### `subscribe_quote(stock_code, period, start_time='', end_time='', count=0, callback=None)`
Subscribe to real-time quotes for a single stock.
- **Returns**: subscription ID (>0 success, -1 failure)
- Callback: `on_data(datas)` where `datas = {stock_code: [data1, ...]}`
- **Limit: max 50 subscriptions; use `subscribe_whole_quote` for more**

#### `subscribe_whole_quote(code_list, callback=None)`
Subscribe to real-time quotes for markets or stock lists.
- `code_list`: `['SH','SZ']` for whole market, or `['600000.SH']` for specific stocks
- **Returns**: subscription ID

#### `unsubscribe_quote(seq)`
Unsubscribe by subscription ID.

#### `run()`
Block thread to maintain subscription connections. Loops with sleep, throws on disconnect.

---

### K-Line Data Fields
| Field | Description |
|-------|-------------|
| `time` | Timestamp |
| `open` | Open price |
| `high` | High price |
| `low` | Low price |
| `close` | Close price |
| `volume` | Volume |
| `amount` | Turnover amount |
| `settelementPrice` | Settlement price (futures) |
| `openInterest` | Open interest |
| `preClose` | Previous close |
| `suspendFlag` | Suspension flag (0=normal, 1=suspended, -1=resumed today) |

### Full Tick Data Fields
| Field | Description |
|-------|-------------|
| `lastPrice` | Latest price |
| `open` / `high` / `low` | Price extremes |
| `lastClose` | Previous close |
| `amount` | Total turnover |
| `volume` | Total volume |
| `pvolume` | Raw volume |
| `stockStatus` | Security status code |
| `openInt` | Open interest |
| `askPrice` / `bidPrice` | Ask/bid price arrays |
| `askVol` / `bidVol` | Ask/bid volume arrays |
| `totalValue` | Total market value |
| `floatValue` | Float market value |
| `transactionNum` | Number of trades |

---

### Financial Data Field Lists

#### PershareIndex (Per-Share Indicators) — Most commonly used
| Field | Description |
|-------|-------------|
| `s_fa_eps_basic` | Basic EPS |
| `s_fa_eps_diluted` | Diluted EPS |
| `s_fa_bps` | Net asset value per share (BVPS) |
| `s_fa_ocfps` | Operating cash flow per share |
| `s_fa_undistributedps` | Undistributed profit per share |
| `s_fa_surpluscapitalps` | Capital reserve per share |
| `adjusted_earnings_per_share` | Deducted non-recurring EPS |
| `du_return_on_equity` | ROE |
| `net_roe` | Diluted ROE |
| `equity_roe` | Weighted average ROE |
| `sales_gross_profit` | Gross margin |
| `gross_profit` | Gross margin (alt) |
| `net_profit` | Net margin |
| `inc_revenue_rate` | YoY revenue growth |
| `du_profit_rate` | YoY net profit growth |
| `inc_net_profit_rate` | YoY net profit growth (parent) |
| `adjusted_net_profit_rate` | YoY deducted non-recurring net profit growth |
| `actual_tax_rate` | Effective tax rate |
| `gear_ratio` | Asset-liability ratio |
| `inventory_turnover` | Inventory turnover |
| `pre_pay_operate_income` | Prepayments/Operating revenue |
| `sales_cash_flow` | Sales cash flow/Operating revenue |

#### Balance (Balance Sheet) — Key Fields
| Field | Description |
|-------|-------------|
| `cash_equivalents` | Cash & equivalents |
| `total_current_assets` | Total current assets |
| `total_non_current_assets` | Total non-current assets |
| `tot_assets` | Total assets |
| `shortterm_loan` | Short-term loans |
| `total_current_liability` | Total current liabilities |
| `non_current_liabilities` | Total non-current liabilities |
| `tot_liab` | Total liabilities |
| `cap_stk` | Share capital |
| `cap_rsrv` | Capital surplus |
| `surplus_rsrv` | Surplus reserve |
| `undistributed_profit` | Undistributed profit |
| `total_equity` | Total equity |
| `tot_shrhldr_eqy_excl_min_int` | Parent company equity |
| `minority_int` | Minority interest |
| `tot_liab_shrhldr_eqy` | Total liabilities + equity |
| `account_receivable` | Accounts receivable |
| `inventories` | Inventories |
| `fix_assets` | Fixed assets |
| `long_term_loans` | Long-term loans |
| `bonds_payable` | Bonds payable |

#### Income (Income Statement) — Key Fields
| Field | Description |
|-------|-------------|
| `revenue` / `revenue_inc` | Operating revenue |
| `total_operating_cost` | Total operating cost |
| `total_expense` | Operating cost |
| `sale_expense` | Selling expenses |
| `less_gerl_admin_exp` | Admin expenses |
| `financial_expense` | Financial expenses |
| `research_expenses` | R&D expenses |
| `oper_profit` | Operating profit |
| `tot_profit` | Total profit |
| `net_profit_incl_min_int_inc` | Net profit |
| `net_profit_excl_min_int_inc` | Net profit (parent) |
| `inc_tax` | Income tax |
| `s_fa_eps_basic` | Basic EPS |
| `s_fa_eps_diluted` | Diluted EPS |

#### Capital (Share Capital)
| Field | Description |
|-------|-------------|
| `total_capital` | Total share capital |
| `circulating_capital` | Circulating A-shares |
| `restrict_circulating_capital` | Restricted circulating shares |

#### Holdernum (Shareholder Count)
| Field | Description |
|-------|-------------|
| `shareholder` | Total shareholders |
| `shareholderA` | A-share holders |
| `shareholderFloat` | Floating shareholders |

---

## Implementation Notes for This Project

1. **`download_financial_data` hangs on miniQMT** — Use `download_financial_data2` instead (with callback parameter). NEVER use the old version.
2. **`get_financial_data` returns pandas DataFrames (not nested dicts)** — Each table is a DataFrame with `m_timetag` column (STRING dtype like '20240331'). Always compare as strings: `df[df['m_timetag'] == '20240331']`. Use `QMTGateway._parse_financial_dataframes()` for batch parsing.
3. **`get_instrument_detail(iscomplete=True)`** returns TotalVolume, FloatVolume, PreClose — use these for market cap calc: `mv = TotalVolume * PreClose / 10000`
4. **Local .DAT files are cleaned after syncing to ClickHouse** — `QMTGateway.clean_local_cache()` handles this
5. **`download_history_data` is synchronous** — must run in executor from async code: `await loop.run_in_executor(None, lambda: xt.download_history_data(...))`
6. **Stock code format**: always `code.market` e.g. `600000.SH`, `000001.SZ`
7. **`incrementally=True`**: downloads only new data from last local record; `incrementally=False` (with start_time): full download; `incrementally=None` (default): auto
8. **`get_full_tick`** works without prior download — returns real-time snapshot data
9. **`get_market_data_ex`** vs `get_market_data`: `ex` returns `{stock: DataFrame}` (preferred), non-ex returns `{field: DataFrame}` (cross-sectional)