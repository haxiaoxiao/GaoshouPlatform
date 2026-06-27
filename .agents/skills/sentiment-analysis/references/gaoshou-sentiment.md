# Gaoshou Sentiment Reference

## Source Matrix

Default order in `backend/app/services/sentiment.py`:

```python
("xueqiu_spyder", "eastmoney_guba", "taoguba", "tieba_stock", "laohu8_stock", "jisilu", "wechat_sogou", "flocktrader")
```

| Source | What It Crawls | Symbol Required | Login/Cookie | Main Config |
|---|---|---:|---|---|
| `xueqiu_spyder` | 雪球个股页 `https://xueqiu.com/S/{symbol}` | Yes | Usually yes | `XUEQIU_COOKIE`, `XUEQIU_CHROME_PATH`, `XUEQIU_DEBUG_PORT`, `XUEQIU_USER_DATA_DIR` |
| `eastmoney_guba` | 东方财富股吧；无 symbol 时跑热门吧 | No | Usually no | none |
| `taoguba` | 淘股吧配置 blog 的文章流 | No | Optional | `TAOGUBA_BLOG_IDS`, `TAOGUBA_COOKIE` |
| `tieba_stock` | 百度贴吧 mobile JSON：股票/股市/A股等吧 | No | Optional | `TIEBA_STOCK_BARS`, `TIEBA_COOKIE` |
| `laohu8_stock` | 老虎社区个股 SSR 页 | No | Optional | `LAOHU8_STOCK_SYMBOLS`, `LAOHU8_COOKIE` |
| `jisilu` | 集思录股票分类 | No | Usually no | none |
| `wechat_sogou` | 搜狗微信文章搜索和指定公众号 | No | Often yes | `WECHAT_SOGOU_QUERIES`, `WECHAT_SOGOU_ACCOUNTS`, `WECHAT_SOGOU_COOKIE` |
| `flocktrader` | NGA 股票/交易板块，经 flocktrader/cache 接入 | No | Often yes | `NGA_DATA_DIR`, `NGA_COOKIE`, `NGA_BOARD_FID` |

Useful aliases include `nga -> flocktrader`, `公众号/微信/搜狗微信 -> wechat_sogou`, `股吧 -> eastmoney_guba`, `百度贴吧/贴吧 -> tieba_stock`, `老虎社区 -> laohu8_stock`, and `集思录 -> jisilu`.

## Current WeChat Account Defaults

Keep these accounts in `WECHAT_SOGOU_ACCOUNTS` unless the user revises the list:

```text
陈小群周策略,余哥牛弹琴,佛总晚评,饭统戴老板,表舅是养基大户,奶员外,海里的小龙龙,小群知识营,投资明见,徐小明,天津股侠,王金生,小红帽爱股票,空空道人
```

## API Usage

Run an ingest task:

```powershell
$body = @{
  sources = @("tieba_stock")
  max_pages = 1
  min_reply = 0
} | ConvertTo-Json
Invoke-RestMethod -Method Post -Uri "http://127.0.0.1:8800/api/sentiment/ingest/run" -Body $body -ContentType "application/json"
```

Poll task status:

```powershell
Invoke-RestMethod "http://127.0.0.1:8800/api/system/tasks/<task_id>"
```

Read cached data:

```powershell
Invoke-RestMethod "http://127.0.0.1:8800/api/sentiment/overview"
Invoke-RestMethod "http://127.0.0.1:8800/api/sentiment/summary/600519.SH?sources=tieba_stock,wechat_sogou"
Invoke-RestMethod "http://127.0.0.1:8800/api/sentiment/posts/600519.SH?limit=50"
Invoke-RestMethod "http://127.0.0.1:8800/api/sentiment/threads?sources=flocktrader&limit=50"
```

`POST /api/sentiment/ingest/run` accepts:

```json
{
  "source": "tieba_stock",
  "sources": ["tieba_stock", "wechat_sogou"],
  "symbol": "600519.SH",
  "max_pages": 3,
  "min_reply": 20,
  "start_date": "2026-06-01",
  "end_date": "2026-06-27",
  "force_refresh": false
}
```

Only `xueqiu_spyder` requires `symbol`. NGA uses `start_date`, `end_date`, and `force_refresh` for daily cache/range crawls.

## Direct Service Smoke

Use direct service calls when the API queue or frontend hides too much detail:

```powershell
Push-Location backend
@'
import asyncio
from app.db.sqlite import init_db, async_session_factory
from app.services.sentiment import SentimentIngestService, SentimentService

async def main():
    await init_db()
    async with async_session_factory() as session:
        result = await SentimentIngestService(session).run(
            "tieba_stock", None, max_pages=1, min_reply=0
        )
        await session.commit()
        overview = await SentimentService(session).overview(["tieba_stock"])
        print(result)
        print(overview)

asyncio.run(main())
'@ | .\.venv\Scripts\python.exe -
Pop-Location
```

Commit the async session after direct ingest, otherwise persisted rows may appear empty.

## Empty Result Diagnosis

Check these in order:

1. Task state: `/api/system/tasks/{task_id}` may still be queued/running; inspect `meta.crawler_progress`.
2. Runtime readiness: `/api/sentiment/overview` exposes `project_ready`, `cookie_configured`, cache dirs/counts, and latest data per source.
3. Verification/login: WeChat/Sogou, NGA, Xueqiu, Taoguba, Tieba, or Laohu8 may need a fresh cookie. Emit or look for `*.verification_required`.
4. Thread vs post mismatch: source-level threads may exist but no symbol-expanded posts. Check `/api/sentiment/threads` before `/summary/{symbol}`.
5. Filters: `min_reply`, date range, `max_pages`, configured bars/blogs/accounts/symbols may be too narrow.
6. Source requirement: Xueqiu requires a symbol; most other sources can run without one.
7. Cache behavior: NGA can skip network when daily cache exists; use `force_refresh=true` only when intentionally recrawling.

## Adding A New Source

Follow this checklist so the source is visible in ingest, overview, progress, cache, tests, and frontend:

1. Choose one canonical source id in snake case; add it to `DEFAULT_SOURCE_ORDER`, `SOURCE_ALIASES`, and `LEGACY_SOURCE_NAMES`.
2. Add config fields in `backend/app/core/config.py` and defaults in `.env.example`; never commit real cookies or accounts.
3. Implement fetch helpers and parsers near related helpers in `backend/app/services/sentiment.py`.
4. Prefer stable public APIs or SSR pages. If captcha/login blocks the crawl, emit a progress event such as `<source>.verification_required` and return a clear error.
5. Add `_collect_<source>` and call `_emit_progress` for plan, page/query/thread fetch, parsed counts, and done stages. Include `source`, `current_step`, and the current page/date/thread title when available.
6. Add normalization helpers: `normalize_<source>_thread` for source-level rows and `normalize_<source>_data_post(s)` for symbol-expanded rows.
7. Wire a `SentimentIngestService.run()` branch and make sure `run_many()` handles success/failure without masking other sources.
8. Extend `_source_runtime_status` so overview shows readiness, cookie state, config counts, cache counts, and project URL/path.
9. Update API/source descriptions if request validation changes.
10. Update frontend `SentimentSource`, source labels/options, and display text in `frontend/src/views/DataManage/SentimentPanel.vue` if the source should be selectable.
11. Add service tests for aliases, parser/collector behavior, normalization, verification handling, and upsert/list behavior.
12. Add API tests for route validation, especially whether symbol is required.
13. Run a real smoke test with `max_pages=1` and `min_reply=0`, then inspect overview, threads, posts, and symbol summary.

## Validation Commands

Backend sentiment tests:

```powershell
backend\.venv\Scripts\python.exe -m pytest backend\tests\services\test_sentiment_service.py backend\tests\api\test_sentiment_routes.py -q
```

Syntax check after editing the service/API:

```powershell
backend\.venv\Scripts\python.exe -m py_compile backend\app\services\sentiment.py backend\app\api\sentiment.py
```

Frontend build when touching frontend source types or panel:

```powershell
Push-Location frontend
npm run build
Pop-Location
```

On Windows, pytest may exit successfully while printing a temporary-directory `PermissionError` during atexit cleanup; treat that as benign only when the pytest exit code is 0.
