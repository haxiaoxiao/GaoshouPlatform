from dataclasses import dataclass, field
from datetime import date, datetime
import json

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.db.models.base import Base
from app.db.models.sentiment import SentimentPost
from app.db.models.stock import Stock
from app.services.sentiment import (
    NgaIngestStats,
    SentimentIngestService,
    SentimentPostInput,
    SentimentService,
    _inject_xueqiu_cookie,
    _parse_laohu8_stock_posts,
    _parse_jisilu_detail,
    _parse_jisilu_list_posts,
    _parse_nga_board_topics,
    _parse_taoguba_article_detail,
    _parse_taoguba_blog_articles,
    _parse_tieba_stock_threads,
    _parse_wechat_sogou_articles,
    _wechat_sogou_queries,
    _post_mentions_symbol,
    _resolve_chrome_path,
    _verify_xueqiu_login,
    normalize_eastmoney_guba_post,
    normalize_flocktrader_post,
    normalize_jisilu_data_post,
    normalize_laohu8_stock_data_post,
    normalize_laohu8_stock_thread,
    normalize_nga_data_post,
    normalize_nga_data_posts,
    normalize_nga_thread,
    normalize_sentiment_source,
    normalize_taoguba_data_post,
    normalize_taoguba_thread,
    normalize_tieba_stock_data_post,
    normalize_tieba_stock_thread,
    normalize_wechat_sogou_data_post,
    normalize_wechat_sogou_thread,
    normalize_xueqiu_spyder_post,
    parse_sources,
    serialize_post,
)


@dataclass
class FlockPostStub:
    tid: str
    title: str
    content: str
    author: str
    publish_time: datetime
    reply_count: int = 0
    sentiment_score: float | None = None
    sentiment_label: str | None = None
    keywords: list[str] = field(default_factory=list)
    comments: list[object] = field(default_factory=list)

    def to_dict(self):
        return {
            "tid": self.tid,
            "title": self.title,
            "content": self.content,
            "author": self.author,
            "publish_time": self.publish_time.isoformat(),
        }


@pytest.fixture
async def sentiment_session():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", future=True)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as session:
        yield session
    await engine.dispose()


def test_parse_sources_validates_supported_values():
    assert parse_sources("xueqiu,nga") == ["xueqiu_spyder", "flocktrader"]
    assert parse_sources("xueqiu_spyder,eastmoney,taoguba,贴吧,老虎社区,jisilu,wechat,flocktrader,xueqiu") == ["xueqiu_spyder", "eastmoney_guba", "taoguba", "tieba_stock", "laohu8_stock", "jisilu", "wechat_sogou", "flocktrader"]
    assert normalize_sentiment_source("xueqiu-spyder") == "xueqiu_spyder"
    assert normalize_sentiment_source("股吧") == "eastmoney_guba"
    assert normalize_sentiment_source("淘股吧") == "taoguba"
    assert normalize_sentiment_source("百度贴吧") == "tieba_stock"
    assert normalize_sentiment_source("老虎社区") == "laohu8_stock"
    assert normalize_sentiment_source("集思录") == "jisilu"
    assert normalize_sentiment_source("公众号") == "wechat_sogou"
    with pytest.raises(ValueError):
        parse_sources("reddit")


def test_wechat_sogou_queries_include_configured_accounts(monkeypatch):
    monkeypatch.setenv("WECHAT_SOGOU_QUERIES", "龙虎榜 A股 游资")
    monkeypatch.setenv("WECHAT_SOGOU_ACCOUNTS", "陈小群周策略,海里的小龙龙、空空道人")

    assert _wechat_sogou_queries() == [
        "龙虎榜 A股 游资",
        "陈小群周策略 股票",
        "海里的小龙龙 股票",
        "空空道人 股票",
    ]


@pytest.mark.asyncio
async def test_sentiment_service_upserts_lists_and_summarizes(sentiment_session):
    service = SentimentService(sentiment_session)
    await service.upsert_posts(
        [
            SentimentPostInput(
                source="xueqiu",
                source_post_id="xq-1",
                symbol="600519.SH",
                title="valuation debate",
                content="good dividend",
                published_at=datetime(2026, 5, 10, 9, 0),
                reply_count=20,
                sentiment_score=0.8,
                keywords=["dividend", "valuation"],
            ),
            SentimentPostInput(
                source="nga",
                source_post_id="nga-1",
                symbol="SH600519",
                title="risk debate",
                published_at=datetime(2026, 5, 10, 10, 0),
                reply_count=5,
                sentiment_score=0.2,
                keywords=["risk"],
            ),
        ]
    )

    posts = await service.list_posts("600519.SH", date(2026, 5, 9), date(2026, 5, 10))
    summary = await service.summary("600519.SH", date(2026, 5, 9), date(2026, 5, 10))

    assert len(posts) == 2
    assert posts[0].source == "xueqiu_spyder"
    assert summary["symbol"] == "600519.SH"
    assert {row["source"] for row in summary["sources"]} == {"xueqiu_spyder", "flocktrader"}
    assert summary["sources"][0]["post_count"] == 1
    assert summary["hottest_posts"][0]["title"] == "valuation debate"


@pytest.mark.asyncio
async def test_sentiment_service_updates_duplicate_source_post(sentiment_session):
    service = SentimentService(sentiment_session)
    await service.upsert_posts(
        [
            SentimentPostInput(
                source="xueqiu",
                source_post_id="xq-1",
                symbol="600519.SH",
                title="old",
                sentiment_score=0.5,
            )
        ]
    )
    await service.upsert_posts(
        [
            SentimentPostInput(
                source="xueqiu",
                source_post_id="xq-1",
                symbol="600519.SH",
                title="new",
                sentiment_score=0.9,
            )
        ]
    )

    posts = await service.list_posts("600519.SH")

    assert len(posts) == 1
    assert posts[0].title == "new"
    assert posts[0].source == "xueqiu_spyder"
    assert posts[0].sentiment_label == "bullish"


@pytest.mark.asyncio
async def test_sentiment_service_migrates_legacy_source_alias(sentiment_session):
    sentiment_session.add(
        SentimentPost(
            source="nga",
            source_post_id="100",
            symbol="600519.SH",
            title="old legacy",
        )
    )
    await sentiment_session.flush()

    service = SentimentService(sentiment_session)
    await service.upsert_posts(
        [
            SentimentPostInput(
                source="flocktrader",
                source_post_id="100",
                symbol="600519.SH",
                title="new canonical",
                sentiment_score=0.3,
            )
        ]
    )

    posts = await service.list_posts("600519.SH", sources=["flocktrader"])

    assert len(posts) == 1
    assert posts[0].source == "flocktrader"
    assert posts[0].title == "new canonical"
    assert posts[0].sentiment_label == "bearish"


@pytest.mark.asyncio
async def test_sentiment_service_upserts_and_lists_threads(sentiment_session):
    raw = {
        "tid": "46800010",
        "title": "SH600519 讨论串",
        "content": "看多 SH600519，关注业绩。",
        "author": "nga-user",
        "publish_time": "2026-05-20T10:00:00",
        "last_reply_time": "2026-05-20T11:30:00",
        "reply_count": 8,
        "comments": [{"content": "继续看多"}],
        "keywords": ["业绩"],
        "sentiment_score": 0.8,
    }
    thread = normalize_nga_thread(raw)
    assert thread is not None

    service = SentimentService(sentiment_session)
    await service.upsert_threads([thread])
    threads = await service.list_threads(
        date(2026, 5, 20),
        date(2026, 5, 20),
        sources=["nga"],
        symbol="600519.SH",
    )

    assert len(threads) == 1
    assert threads[0].source == "flocktrader"
    assert threads[0].source_thread_id == "46800010"
    assert threads[0].sentiment_label == "bullish"
    assert "600519.SH" in threads[0].symbols_json


@pytest.mark.asyncio
async def test_sentiment_service_overview_counts_sources_and_runtime_status(sentiment_session, monkeypatch, tmp_path):
    xueqiu_dir = tmp_path / "xueqiu-spyder"
    flock_dir = tmp_path / "flocktrader"
    nga_cache_dir = tmp_path / "nga-cache"
    xueqiu_dir.mkdir()
    flock_dir.mkdir()
    nga_cache_dir.mkdir()
    (nga_cache_dir / "posts_2026-05-10.json").write_text("[]", encoding="utf-8")

    monkeypatch.setenv("XUEQIU_SPYDER_DIR", str(xueqiu_dir))
    monkeypatch.setenv("FLOCKTRADER_DIR", str(flock_dir))
    monkeypatch.setenv("NGA_DATA_DIR", str(nga_cache_dir))
    monkeypatch.setenv("XUEQIU_COOKIE", "xq_a_token=abc")
    monkeypatch.delenv("NGA_COOKIE", raising=False)

    service = SentimentService(sentiment_session)
    await service.upsert_posts(
        [
            SentimentPostInput(
                source="xueqiu",
                source_post_id="xq-overview-1",
                symbol="600519.SH",
                published_at=datetime(2026, 5, 10, 9, 0),
                sentiment_score=0.8,
            ),
            SentimentPostInput(
                source="nga",
                source_post_id="nga-overview-1",
                symbol="000001.SZ",
                published_at=datetime(2026, 5, 11, 10, 0),
                sentiment_score=0.3,
            ),
        ]
    )

    overview = await service.overview()
    rows = {row["source"]: row for row in overview["sources"]}

    assert overview["total_posts"] == 2
    assert overview["symbol_count"] == 2
    assert overview["latest_published_at"] == "2026-05-11T10:00:00"
    assert rows["xueqiu_spyder"]["ready"] is True
    assert rows["xueqiu_spyder"]["cookie_configured"] is True
    assert rows["taoguba"]["ready"] is True
    assert rows["tieba_stock"]["ready"] is True
    assert rows["tieba_stock"]["bar_count"] >= 3
    assert rows["laohu8_stock"]["ready"] is True
    assert rows["laohu8_stock"]["symbol_count_configured"] >= 5
    assert rows["wechat_sogou"]["ready"] is True
    assert rows["wechat_sogou"]["account_count"] >= 10
    assert rows["flocktrader"]["ready"] is True
    assert rows["flocktrader"]["cache_file_count"] == 1


@pytest.mark.asyncio
async def test_sentiment_ingest_service_run_many_combines_sources(sentiment_session):
    service = SentimentIngestService(sentiment_session)

    async def fake_run(
        source: str,
        symbol: str,
        max_pages: int = 3,
        min_reply: int = 20,
        start_date: date | None = None,
        end_date: date | None = None,
        force_refresh: bool = False,
    ):
        assert symbol == "600519.SH"
        if source == "flocktrader":
            raise RuntimeError("NGA cookie missing")
        return {
            "source": source,
            "symbol": symbol,
            "collected": 3,
            "matched": 2,
            "upserted": 2,
        }

    service.run = fake_run  # type: ignore[method-assign]
    result = await service.run_many("600519.SH")

    assert result["requested_sources"] == ["xueqiu_spyder", "eastmoney_guba", "taoguba", "tieba_stock", "laohu8_stock", "jisilu", "wechat_sogou", "flocktrader"]
    assert result["succeeded_sources"] == ["xueqiu_spyder", "eastmoney_guba", "taoguba", "tieba_stock", "laohu8_stock", "jisilu", "wechat_sogou"]
    assert result["failed_sources"] == ["flocktrader"]
    assert result["all_succeeded"] is False
    assert result["total_upserted"] == 14
    assert result["results"][0]["ok"] is True
    assert result["results"][1]["ok"] is True
    assert result["results"][2]["ok"] is True
    assert result["results"][3]["ok"] is True
    assert result["results"][4]["ok"] is True
    assert result["results"][5]["ok"] is True
    assert result["results"][6]["ok"] is True
    assert result["results"][7]["ok"] is False
    assert "NGA cookie missing" in result["results"][7]["error"]


@pytest.mark.asyncio
async def test_sentiment_ingest_service_run_many_allows_flocktrader_without_symbol(sentiment_session):
    service = SentimentIngestService(sentiment_session)

    async def fake_run(
        source: str,
        symbol: str | None,
        max_pages: int = 3,
        min_reply: int = 20,
        start_date: date | None = None,
        end_date: date | None = None,
        force_refresh: bool = False,
    ):
        assert source == "flocktrader"
        assert symbol is None
        return {
            "source": source,
            "symbol": symbol,
            "collected": 8,
            "matched": 5,
            "upserted": 5,
        }

    service.run = fake_run  # type: ignore[method-assign]
    result = await service.run_many(None, sources=["flocktrader"])

    assert result["requested_sources"] == ["flocktrader"]
    assert result["succeeded_sources"] == ["flocktrader"]
    assert result["failed_sources"] == []
    assert result["symbol"] is None
    assert result["total_upserted"] == 5


@pytest.mark.asyncio
async def test_sentiment_ingest_run_persists_nga_threads_and_symbol_posts(sentiment_session):
    service = SentimentIngestService(sentiment_session)
    raw = {
        "tid": "46800100",
        "title": "SH600519 与 SZ000001 一起异动",
        "content": "今天重点看 SH600519，同时 SZ000001 也有放量。",
        "author": "nga-user",
        "publish_time": "2026-05-20T10:00:00",
        "last_reply_time": "2026-05-20T10:30:00",
        "reply_count": 2,
        "comments": [{"content": "看多 SH600519"}],
        "stock_codes": ["600519", "000001"],
        "keywords": ["白酒", "银行"],
        "sentiment_score": 0.7,
    }

    def fake_load_or_crawl(start, end, *, max_pages, force_refresh):
        return [raw], NgaIngestStats(loaded_dates=["2026-05-20"], date_files=["posts_2026-05-20.json"])

    async def fake_load_stock_aliases():
        return []

    service._load_or_crawl_nga_date_posts = fake_load_or_crawl  # type: ignore[method-assign]
    service._load_stock_aliases = fake_load_stock_aliases  # type: ignore[method-assign]

    result = await service.run(
        "flocktrader",
        None,
        start_date=date(2026, 5, 20),
        end_date=date(2026, 5, 20),
    )
    sentiment_service = SentimentService(sentiment_session)
    threads = await sentiment_service.list_threads(sources=["flocktrader"])
    posts = await sentiment_service.list_posts("600519.SH", sources=["flocktrader"])

    assert result["threads_upserted"] == 1
    assert result["upserted"] == 2
    assert threads[0].source_thread_id == "46800100"
    assert posts[0].source_post_id == "46800100:600519.SH"


def test_parse_nga_board_topics_reads_last_reply_time():
    topics = _parse_nga_board_topics(
        """
        <tr class='row1 topicrow'>
            <td class='c1'><a href='/read.php?tid=46872529' class='replies'>10355</a></td>
            <td class='c2'><a href='/read.php?tid=46872529' class='topic'>存储人哀嚎酒馆。</a></td>
            <td class='c3'><a title='用户ID 61395264'>村上吹树</a><span class='silver postdate'>1779952664</span></td>
            <td class='c4'><a href='/read.php?tid=46872529&page=e' class='silver replydate'></a><span class='replyer'>ShirasagiYin</span></td>
        </tr>
        <script type='text/javascript'>
        commonui.topicArg.add(
        't_rc1_2','t_tt1_2','t_ta1_2','t_pt1_2',
        't_tr1_2','t_rt1_2','t_pc1_2',
        '706',46872529,'','','0',1779952664,1782532787,10355,
        8224,'','',
        null,'',null,'',''
        )
        </script>
        """
    )

    assert len(topics) == 1
    assert topics[0]["tid"] == "46872529"
    assert topics[0]["reply_count"] == 10355
    assert topics[0]["publish_time"] == datetime.fromtimestamp(1779952664)
    assert topics[0]["last_reply_time"] == datetime.fromtimestamp(1782532787)


@pytest.mark.asyncio
async def test_nga_range_crawl_buckets_missing_days_once(sentiment_session, monkeypatch, tmp_path):
    progress_events: list[dict] = []
    service = SentimentIngestService(sentiment_session, progress_callback=progress_events.append)
    monkeypatch.setenv("NGA_COOKIE", "nga_uid=1")
    monkeypatch.setenv("NGA_DATA_DIR", str(tmp_path))

    topics_by_page = {
        1: [{"tid": "1", "title": "today", "author": "a", "reply_count": 1, "publish_time": datetime(2026, 6, 22, 10), "last_reply_time": datetime(2026, 6, 22, 10)}],
        2: [{"tid": "2", "title": "yesterday", "author": "b", "reply_count": 1, "publish_time": datetime(2026, 6, 21, 10), "last_reply_time": datetime(2026, 6, 22, 9)}],
        3: [{"tid": "3", "title": "older", "author": "c", "reply_count": 1, "publish_time": datetime(2026, 6, 14, 10), "last_reply_time": datetime(2026, 6, 14, 10)}],
    }
    posts_by_tid = {
        "1": [{"title": "today", "content": "SH600519 今日", "publish_time": "2026-06-22 10:00"}],
        "2": [
            {"title": "yesterday", "content": "SZ000001 昨日", "publish_time": "2026-06-21 10:00"},
            {"title": "", "content": "6月22日仍有回复", "publish_time": "2026-06-22 09:00"},
        ],
        "3": [{"title": "older", "content": "older", "publish_time": "2026-06-14 10:00"}],
    }
    fetched_pages: list[int] = []

    def fake_fetch(url, *, params=None):
        if "thread.php" in url:
            fetched_pages.append(int(params["page"]))
            return ""
        return ""

    monkeypatch.setattr("app.services.sentiment._fetch_nga_html", fake_fetch)
    monkeypatch.setattr("app.services.sentiment._parse_nga_board_topics", lambda html: topics_by_page.get(fetched_pages[-1], []))
    monkeypatch.setattr("app.services.sentiment._parse_nga_thread_posts", lambda html: posts_by_tid[html] if html in posts_by_tid else [])

    def fake_fetch_with_tid(url, *, params=None):
        if "thread.php" in url:
            fetched_pages.append(int(params["page"]))
            return "board"
        return str(params["tid"])

    monkeypatch.setattr("app.services.sentiment._fetch_nga_html", fake_fetch_with_tid)

    posts, stats = service._load_or_crawl_nga_date_posts(
        date(2026, 6, 21),
        date(2026, 6, 22),
        max_pages=1,
        force_refresh=True,
    )

    assert fetched_pages == [1, 2, 1, 2]
    assert [post["tid"] for post in posts] == ["1", "2"]
    assert stats.crawled_dates == ["2026-06-21", "2026-06-22"]
    assert stats.scan_time_basis == "last_reply_time"
    assert stats.cache_partition == "last_reply_time"
    assert json.loads((tmp_path / "posts_2026-06-21.json").read_text(encoding="utf-8")) == []
    assert [post["tid"] for post in json.loads((tmp_path / "posts_2026-06-22.json").read_text(encoding="utf-8"))] == ["1", "2"]
    assert any(event["stage"] == "nga.board.page_fetch" and event["board_page"] == 1 for event in progress_events)
    assert any(
        event["stage"] == "nga.thread.fetch"
        and event["current_tid"] == "2"
        and event["current_title"] == "yesterday"
        and event["detail_page"] == "e"
        for event in progress_events
    )
    assert any(
        event["stage"] == "nga.cache.write"
        and event["current_date"] == "2026-06-22"
        and event["cache_posts"] == 2
        for event in progress_events
    )


@pytest.mark.asyncio
async def test_nga_range_crawl_stops_at_first_topic_before_start(sentiment_session, monkeypatch, tmp_path):
    progress_events: list[dict] = []
    service = SentimentIngestService(sentiment_session, progress_callback=progress_events.append)
    monkeypatch.setenv("NGA_COOKIE", "nga_uid=1")
    monkeypatch.setenv("NGA_DATA_DIR", str(tmp_path))

    topics_by_page = {
        1: [
            {"tid": "1", "title": "today", "author": "a", "reply_count": 1, "publish_time": datetime(2026, 6, 22, 10), "last_reply_time": datetime(2026, 6, 22, 10)},
            {"tid": "2", "title": "yesterday", "author": "b", "reply_count": 1, "publish_time": datetime(2026, 6, 21, 10), "last_reply_time": datetime(2026, 6, 21, 23, 59)},
            {"tid": "3", "title": "should not fetch", "author": "c", "reply_count": 1, "publish_time": datetime(2026, 6, 22, 9), "last_reply_time": datetime(2026, 6, 22, 9)},
        ],
        2: [{"tid": "4", "title": "should not page", "author": "d", "reply_count": 1, "publish_time": datetime(2026, 6, 22, 8), "last_reply_time": datetime(2026, 6, 22, 8)}],
    }
    posts_by_tid = {
        "1": [{"title": "today", "content": "SH600519 今日", "publish_time": "2026-06-22 10:00"}],
    }
    fetched_pages: list[int] = []
    fetched_tids: list[str] = []

    def fake_fetch(url, *, params=None):
        if "thread.php" in url:
            fetched_pages.append(int(params["page"]))
            return "board"
        fetched_tids.append(str(params["tid"]))
        return str(params["tid"])

    monkeypatch.setattr("app.services.sentiment._fetch_nga_html", fake_fetch)
    monkeypatch.setattr("app.services.sentiment._parse_nga_board_topics", lambda html: topics_by_page.get(fetched_pages[-1], []))
    monkeypatch.setattr("app.services.sentiment._parse_nga_thread_posts", lambda html: posts_by_tid.get(html, []))

    posts, stats = service._load_or_crawl_nga_date_posts(
        date(2026, 6, 22),
        date(2026, 6, 22),
        max_pages=5,
        force_refresh=True,
    )

    assert fetched_pages == [1]
    assert fetched_tids == ["1"]
    assert [post["tid"] for post in posts] == ["1"]
    assert stats.scan_time_basis == "last_reply_time"
    assert stats.cache_partition == "last_reply_time"
    assert any(
        event["stage"] == "nga.crawl.stop_before_start"
        and event["current_tid"] == "2"
        and event["stop_time"] == "2026-06-21T23:59:00"
        for event in progress_events
    )


@pytest.mark.asyncio
async def test_nga_symbol_search_uses_recent_reply_window(sentiment_session, monkeypatch):
    progress_events: list[dict] = []
    service = SentimentIngestService(sentiment_session, progress_callback=progress_events.append)

    stock = Stock(symbol="603629.SH", name="利通电子", company_name="江苏利通电子股份有限公司")
    sentiment_session.add(stock)
    await sentiment_session.flush()

    def fake_search_html(query: str, *, page: int = 1):
        if query == "利通电子" and page == 1:
            return """
            <tr class='row1 topicrow'>
                <td class='c1'><a href='/read.php?tid=1001' class='replies'>3</a></td>
                <td class='c2'><a href='/read.php?tid=1001' class='topic'>利通电子(603629)后续大家怎么看待？</a></td>
                <td class='c3'><a title='用户ID 1'>甲</a><span class='silver postdate'>1739915247</span></td>
                <td class='c4'><a href='/read.php?tid=1001&page=e' class='silver replydate'></a><span class='replyer'>乙</span></td>
            </tr>
            <script type='text/javascript'>
            commonui.topicArg.add(
            'a','b','c','d',
            'e','f','g',
            '706',1001,'','','0',1739915247,1782528000,3,
            8192,'','',
            null,'',null,'',''
            )
            </script>
            <tr class='row2 topicrow'>
                <td class='c1'><a href='/read.php?tid=1002' class='replies'>1</a></td>
                <td class='c2'><a href='/read.php?tid=1002' class='topic'>利通电子旧帖</a></td>
                <td class='c3'><a title='用户ID 2'>丙</a><span class='silver postdate'>1739000000</span></td>
                <td class='c4'><a href='/read.php?tid=1002&page=e' class='silver replydate'></a><span class='replyer'>丁</span></td>
            </tr>
            <script type='text/javascript'>
            commonui.topicArg.add(
            'a','b','c','d',
            'e','f','g',
            '706',1002,'','','0',1739000000,1782000000,1,
            8192,'','',
            null,'',null,'',''
            )
            </script>
            """
        return "没有符合条件的结果"

    def fake_fetch(url, *, params=None):
        return str(params["tid"])

    def fake_parse_thread_posts(html_text: str):
        if html_text == "1001":
            return [
                {"title": "利通电子(603629)后续大家怎么看待？", "content": "感觉还能反弹 SH603629", "publish_time": "2026-06-26 10:00"},
                {"title": "", "content": "昨天跌停太伤了", "publish_time": "2026-06-27 09:00"},
            ]
        if html_text == "1002":
            return [
                {"title": "利通电子旧帖", "content": "很老的内容 SH603629", "publish_time": "2026-06-10 10:00"},
                {"title": "", "content": "旧回复", "publish_time": "2026-06-20 09:00"},
            ]
        return []

    monkeypatch.setattr("app.services.sentiment._fetch_nga_search_html", fake_search_html)
    monkeypatch.setattr("app.services.sentiment._fetch_nga_html", fake_fetch)
    monkeypatch.setattr("app.services.sentiment._parse_nga_thread_posts", fake_parse_thread_posts)

    posts, threads, stats = await service._collect_flocktrader_by_date(
        "603629.SH",
        max_pages=2,
        start_date=date(2026, 6, 25),
        end_date=date(2026, 6, 27),
        force_refresh=False,
    )

    assert stats.mode == "search_recent_reply+daily_cache"
    assert stats.search_queries[0] == "利通电子"
    assert stats.search_pages[0]["matched"] == 1
    assert len(posts) >= 1
    assert "1001:603629.SH" in {post.source_post_id for post in posts}
    assert "1001" in {thread.source_thread_id for thread in threads}
    assert any(
        event["stage"] == "nga.search.thread_collected"
        and event["current_tid"] == "1001"
        for event in progress_events
    )


@pytest.mark.asyncio
async def test_nga_symbol_mode_merges_search_and_daily_cache(sentiment_session, monkeypatch):
    service = SentimentIngestService(sentiment_session)
    stock = Stock(symbol="603629.SH", name="利通电子", company_name="江苏利通电子股份有限公司")
    sentiment_session.add(stock)
    await sentiment_session.flush()

    search_stats = NgaIngestStats(mode="search_recent_reply")
    search_stats.search_queries = ["利通电子", "603629"]
    search_stats.search_pages = [{"query": "利通电子", "page": 1, "raw_count": 1, "matched": 1}]
    date_stats = NgaIngestStats(mode="daily_cache")
    date_stats.loaded_dates = ["2026-06-26"]
    date_stats.date_files = ["posts_2026-06-26.json"]

    monkeypatch.setattr(
        service,
        "_load_or_search_nga_symbol_posts",
        lambda *args, **kwargs: (
            [
                {
                    "tid": "2001",
                    "title": "search hit",
                    "content": "SH603629 反弹",
                    "publish_time": "2026-06-26 10:00",
                    "last_reply_time": "2026-06-26 11:00",
                    "reply_count": 2,
                    "comments": [],
                    "stock_codes": ["603629.SH"],
                }
            ],
            search_stats,
        ),
    )
    monkeypatch.setattr(
        service,
        "_load_or_crawl_nga_date_posts",
        lambda *args, **kwargs: (
            [
                {
                    "tid": "2001",
                    "title": "search hit",
                    "content": "SH603629 反弹",
                    "publish_time": "2026-06-26 10:00",
                    "last_reply_time": "2026-06-26 11:00",
                    "reply_count": 2,
                    "comments": [],
                    "stock_codes": ["603629.SH"],
                },
                {
                    "tid": "2002",
                    "title": "date cache hit",
                    "content": "利通电子 跌停",
                    "publish_time": "2026-06-27 09:00",
                    "last_reply_time": "2026-06-27 09:30",
                    "reply_count": 3,
                    "comments": [],
                    "stock_codes": ["603629.SH"],
                },
            ],
            date_stats,
        ),
    )

    posts, threads, stats = await service._collect_flocktrader_by_date(
        "603629.SH",
        max_pages=2,
        start_date=date(2026, 6, 25),
        end_date=date(2026, 6, 27),
        force_refresh=False,
    )

    assert stats.mode == "search_recent_reply+daily_cache"
    assert stats.search_queries == ["利通电子", "603629"]
    assert stats.loaded_dates == ["2026-06-26"]
    assert len(posts) == 2
    assert {post.source_post_id for post in posts} == {"2001:603629.SH", "2002:603629.SH"}
    assert {thread.source_thread_id for thread in threads} == {"2001", "2002"}


def test_normalize_xueqiu_spyder_post_maps_raw_payload():
    post = normalize_xueqiu_spyder_post(
        {
            "id": 123,
            "title": "<b>茅台</b>",
            "text": "<p>看多 $贵州茅台(SH600519)$</p>",
            "user": {"screen_name": "analyst"},
            "created_at": 1_779_552_000_000,
            "target": "/123/456",
            "reply_count": "12",
            "like_count": "7",
        },
        symbol="600519.SH",
    )

    assert post is not None
    assert post.source == "xueqiu_spyder"
    assert post.source_post_id == "123"
    assert post.title == "茅台"
    assert post.content == "看多 贵州茅台(SH600519)"
    assert post.author == "analyst"
    assert post.url == "https://xueqiu.com/123/456"
    assert post.reply_count == 12
    assert post.like_count == 7


def test_serialize_post_exposes_xueqiu_source_meta():
    post = SentimentPost(
        source="xueqiu_spyder",
        source_post_id="123",
        symbol="600519.SH",
        title="雪球样本",
        raw_json=json.dumps(
            {
                "view_count": 760,
                "retweet_count": 2,
                "fav_count": 3,
                "source": "Android",
                "timeBefore": "19分钟前",
                "user": {
                    "id": 42,
                    "profile": "/u/42",
                    "followers_count": 395,
                    "friends_count": 18,
                    "status_count": 1001,
                    "verified": True,
                    "verified_description": "财经博主",
                    "verified_type": 7,
                    "verified_infos": [{"type": "stock"}],
                },
            },
            ensure_ascii=False,
        ),
    )

    meta = serialize_post(post)["source_meta"]

    assert meta["view_count"] == 760
    assert meta["retweet_count"] == 2
    assert meta["fav_count"] == 3
    assert meta["source_device"] == "Android"
    assert meta["time_before"] == "19分钟前"
    assert meta["user_id"] == "42"
    assert meta["user_followers_count"] == 395
    assert meta["user_verified"] is True
    assert meta["user_verified_description"] == "财经博主"
    assert meta["user_verified_infos"] == [{"type": "stock"}]


def test_normalize_eastmoney_guba_post_maps_list_payload():
    post = normalize_eastmoney_guba_post(
        {
            "post_id": 1730994689,
            "post_title": "明天就靠你护盘了",
            "stockbar_code": "600519",
            "user_nickname": "散户甲",
            "post_comment_count": 3,
            "post_publish_time": "2026-06-23 20:56:01",
        },
        symbol="600519.SH",
    )

    assert post is not None
    assert post.source == "eastmoney_guba"
    assert post.source_post_id == "1730994689"
    assert post.symbol == "600519.SH"
    assert post.title == "明天就靠你护盘了"
    assert post.author == "散户甲"
    assert post.published_at == datetime(2026, 6, 23, 20, 56, 1)
    assert post.comment_count == 3
    assert post.url == "https://guba.eastmoney.com/news,600519,1730994689.html"
    assert normalize_eastmoney_guba_post(
        {"post_id": 1730999999, "post_title": "跨吧噪声", "stockbar_code": "000661"},
        symbol="600519.SH",
    ) is None


@pytest.mark.asyncio
async def test_collect_eastmoney_guba_filters_pages_dates_and_min_reply(sentiment_session, monkeypatch):
    service = SentimentIngestService(sentiment_session)
    payloads = {
        1: {
            "re": [
                {
                    "post_id": 1,
                    "post_title": "今天加仓",
                    "stockbar_code": "600519",
                    "user_nickname": "a",
                    "post_comment_count": 2,
                    "post_publish_time": "2026-06-23 10:00:00",
                },
                {
                    "post_id": 2,
                    "post_title": "评论太少",
                    "stockbar_code": "600519",
                    "user_nickname": "b",
                    "post_comment_count": 0,
                    "post_publish_time": "2026-06-23 11:00:00",
                },
            ]
        },
        2: {
            "re": [
                {
                    "post_id": 3,
                    "post_title": "旧帖",
                    "stockbar_code": "600519",
                    "user_nickname": "c",
                    "post_comment_count": 3,
                    "post_publish_time": "2026-06-20 10:00:00",
                }
            ]
        },
    }
    seen_pages: list[int] = []

    def fake_fetch(code: str, page: int, page_size: int = 20):
        assert code == "600519"
        seen_pages.append(page)
        return payloads.get(page, {"re": []})

    monkeypatch.setattr("app.services.sentiment._fetch_eastmoney_guba_page", fake_fetch)

    posts, stats = service._collect_eastmoney_guba(
        "600519.SH",
        max_pages=2,
        min_reply=1,
        start_date=date(2026, 6, 23),
        end_date=date(2026, 6, 23),
    )

    assert seen_pages == [1, 2]
    assert stats["collected"] == 3
    assert [post.source_post_id for post in posts] == ["1"]
    assert stats["pages"] == [
        {"page": 1, "raw_count": 2, "matched": 1},
        {"page": 2, "raw_count": 1, "matched": 0},
    ]


@pytest.mark.asyncio
async def test_collect_eastmoney_hot_bars_without_symbol(sentiment_session, monkeypatch):
    service = SentimentIngestService(sentiment_session)
    monkeypatch.setattr(
        "app.services.sentiment._fetch_eastmoney_hot_bars",
        lambda limit: [
            {"code": "600519", "name": "贵州茅台吧"},
            {"code": "300750", "name": "宁德时代吧"},
        ][:limit],
    )
    payloads = {
        "600519": {
            "re": [
                {
                    "post_id": 1,
                    "post_title": "茅台看多",
                    "stockbar_code": "600519",
                    "user_nickname": "a",
                    "post_comment_count": 2,
                    "post_publish_time": "2026-06-23 10:00:00",
                },
                {
                    "post_id": 2,
                    "post_title": "评论太少",
                    "stockbar_code": "600519",
                    "user_nickname": "b",
                    "post_comment_count": 0,
                    "post_publish_time": "2026-06-23 11:00:00",
                },
            ]
        },
        "300750": {
            "re": [
                {
                    "post_id": 3,
                    "post_title": "宁德反弹",
                    "stockbar_code": "300750",
                    "user_nickname": "c",
                    "post_comment_count": 3,
                    "post_publish_time": "2026-06-23 12:00:00",
                }
            ]
        },
    }
    seen_codes: list[str] = []

    def fake_fetch(code: str, page: int, page_size: int = 20):
        assert page == 1
        seen_codes.append(code)
        return payloads[code]

    monkeypatch.setattr("app.services.sentiment._fetch_eastmoney_guba_page", fake_fetch)

    posts, stats = service._collect_eastmoney_guba_hot_bars(
        max_pages=1,
        min_reply=1,
        start_date=date(2026, 6, 23),
        end_date=date(2026, 6, 23),
    )

    assert seen_codes == ["600519", "300750"]
    assert stats["bar_limit"] == 10
    assert stats["collected"] == 3
    assert [(post.symbol, post.source_post_id) for post in posts] == [
        ("600519.SH", "1"),
        ("300750.SZ", "3"),
    ]
    assert stats["bars"] == [
        {"symbol": "600519.SH", "code": "600519", "name": "贵州茅台吧", "raw_count": 2, "matched": 1},
        {"symbol": "300750.SZ", "code": "300750", "name": "宁德时代吧", "raw_count": 1, "matched": 1},
    ]


def test_parse_and_normalize_taoguba_payload():
    blog_html = """
    <title>zarili_博客_淘股吧</title>
    <div class="allblog_article">
      <div class="article_tittle">
        <div class="tittle_data left"><span>[原]</span><a href="a/2sNTQK9UyWZ" title="主升进行时">主升进行时</a></div>
        <div class="tittle_llhf left">54131/483</div>
        <div class="tittle_fbshijian left">2026-06-21</div>
        <div class="clear"></div>
      </div>
    </div>
    """
    detail_html = """
    <div class="article-content">
      <div class="article-tittle" id="stockTitle">主升进行时<div id="gioMsg" subject="主升进行时" userID="2577911" userName="zarili"></div></div>
      <div class="article-data">
        <span><a href="/blog/2577911">zarili</a></span>
        <span>淘股吧原创&nbsp;2026-06-21 09:30&nbsp;</span>
        <span>|</span><span>浏览 54131 </span><span>|</span><span>评论 483 </span>
      </div>
      <div class="article-text p_coten" id="first"><div class=" gradient">贵州茅台 600519 和科技主线都在修复。</div></div>
    </div>
    """
    articles = _parse_taoguba_blog_articles(blog_html, blog_id="2577911")
    detail = _parse_taoguba_article_detail(detail_html, article_id="2sNTQK9UyWZ", fallback=articles[0])
    raw = {
        **detail,
        "stock_codes": ["600519.SH"],
        "keywords": ["贵州茅台", "600519.SH"],
        "sentiment_score": 0.6,
        "sentiment_label": "bullish",
    }
    post = normalize_taoguba_data_post(raw, symbol="600519.SH")
    thread = normalize_taoguba_thread(raw)

    assert articles == [
        {
            "article_id": "2sNTQK9UyWZ",
            "source_thread_id": "2sNTQK9UyWZ",
            "title": "主升进行时",
            "author": "zarili",
            "blog_id": "2577911",
            "url": "https://www.tgb.cn/a/2sNTQK9UyWZ",
            "view_count": 54131,
            "reply_count": 483,
            "published_at": "2026-06-21",
        }
    ]
    assert detail["published_at"] == "2026-06-21 09:30"
    assert detail["content"] == "贵州茅台 600519 和科技主线都在修复。"
    assert post is not None
    assert post.source == "taoguba"
    assert post.source_post_id == "2sNTQK9UyWZ:600519.SH"
    assert post.comment_count == 483
    assert thread is not None
    assert thread.source == "taoguba"
    assert thread.symbols == ["600519.SH"]


@pytest.mark.asyncio
async def test_collect_taoguba_matches_symbols_and_threads(sentiment_session, monkeypatch):
    service = SentimentIngestService(sentiment_session)
    blog_html = """
    <title>zarili_博客_淘股吧</title>
    <div class="article_tittle">
      <div class="tittle_data left"><a href="a/abc123" title="主升进行时">主升进行时</a></div>
      <div class="tittle_llhf left">100/5</div>
      <div class="tittle_fbshijian left">2026-06-21</div>
      <div class="clear"></div>
    </div>
    <div class="article_tittle">
      <div class="tittle_data left"><a href="a/tooquiet" title="回复太少">回复太少</a></div>
      <div class="tittle_llhf left">10/0</div>
      <div class="tittle_fbshijian left">2026-06-21</div>
      <div class="clear"></div>
    </div>
    """
    detail_html = """
    <div class="article-content">
      <div class="article-tittle">主升进行时<div id="gioMsg" userName="zarili"></div></div>
      <div class="article-data"><span>2026-06-21 09:30&nbsp;</span><span>浏览 100 </span><span>评论 5 </span></div>
      <div class="article-text p_coten"><div class=" gradient">贵州茅台 600519 看多，科技也活跃。</div></div>
    </div>
    """
    seen_urls: list[str] = []

    def fake_fetch(url: str, *, referer: str = "https://www.tgb.cn/") -> str:
        seen_urls.append(url)
        if "/a/" in url:
            return detail_html
        return blog_html

    monkeypatch.setattr("app.services.sentiment._fetch_taoguba_html", fake_fetch)
    posts, threads, stats = service._collect_taoguba(
        None,
        None,
        [("贵州茅台", "600519.SH")],
        [{"blog_id": "2577911", "name": "zarili"}],
        max_pages=2,
        min_reply=1,
        start_date=date(2026, 6, 21),
        end_date=date(2026, 6, 21),
    )

    assert seen_urls == ["https://www.tgb.cn/blog/2577911", "https://www.tgb.cn/a/abc123"]
    assert stats["collected"] == 2
    assert stats["article_limit_per_blog"] == 2
    assert len(threads) == 1
    assert [(post.source, post.symbol, post.source_post_id) for post in posts] == [
        ("taoguba", "600519.SH", "abc123:600519.SH")
    ]


def test_parse_and_normalize_tieba_stock_payload():
    payload = {
        "errno": 0,
        "data": {
            "forum": {"name": "股票"},
            "thread_list": [
                {
                    "tid": 10813189797,
                    "title": "贵州茅台 600519 全线反弹",
                    "abstract": [{"text": "白酒和科技都看多，600519 有反弹预期。"}],
                    "author": {"name_show": "散户老李"},
                    "last_time_int": 1782233188,
                    "reply_num": 4,
                    "agree": {"agree_num": 8, "disagree_num": 1},
                }
            ],
        },
    }

    rows = _parse_tieba_stock_threads(payload, bar="股票", page=1)
    raw = {
        **rows[0],
        "stock_codes": ["600519.SH"],
        "keywords": ["贵州茅台", "600519.SH"],
        "sentiment_score": 1.0,
        "sentiment_label": "bullish",
    }
    post = normalize_tieba_stock_data_post(raw, symbol="600519.SH")
    thread = normalize_tieba_stock_thread(raw)

    assert rows[0]["source_thread_id"] == "10813189797"
    assert rows[0]["forum_name"] == "股票"
    assert rows[0]["reply_count"] == 4
    assert post is not None
    assert post.source == "tieba_stock"
    assert post.source_post_id == "10813189797:600519.SH"
    assert post.like_count == 8
    assert thread is not None
    assert thread.source == "tieba_stock"
    assert thread.symbols == ["600519.SH"]


@pytest.mark.asyncio
async def test_collect_tieba_stock_matches_symbols_and_threads(sentiment_session, monkeypatch):
    service = SentimentIngestService(sentiment_session)
    payload = {
        "errno": 0,
        "data": {
            "forum": {"name": "股票"},
            "thread_list": [
                {
                    "tid": 10813189797,
                    "title": "贵州茅台 600519 全线反弹",
                    "abstract": [{"text": "白酒和科技都看多，600519 有反弹预期。"}],
                    "author": {"name_show": "散户老李"},
                    "last_time_int": 1782233188,
                    "reply_num": 4,
                },
                {
                    "tid": 10813180000,
                    "title": "无关闲聊",
                    "abstract": [{"text": "不提股票代码。"}],
                    "author": {"name_show": "路人"},
                    "last_time_int": 1782233188,
                    "reply_num": 9,
                },
            ],
        },
    }
    seen: list[tuple[str, int]] = []

    def fake_fetch(bar: str, page: int = 1, page_size: int = 30) -> dict:
        seen.append((bar, page))
        return payload if page == 1 else {"errno": 0, "data": {"forum": {"name": bar}, "thread_list": []}}

    monkeypatch.setattr("app.services.sentiment._fetch_tieba_stock_json", fake_fetch)
    posts, threads, stats = service._collect_tieba_stock(
        None,
        None,
        [("贵州茅台", "600519.SH")],
        ["股票"],
        max_pages=2,
        min_reply=1,
        start_date=date(2026, 6, 24),
        end_date=date(2026, 6, 24),
    )

    assert seen == [("股票", 1), ("股票", 2)]
    assert stats["collected"] == 2
    assert len(threads) == 2
    assert [(post.source, post.symbol, post.source_post_id) for post in posts] == [
        ("tieba_stock", "600519.SH", "10813189797:600519.SH")
    ]


def test_parse_and_normalize_laohu8_stock_payload():
    html = """
    <div class="tweet-item-root">
      <div class="tweet-content-container">
        <header class="tweet-item-header">
          <a class="tweet-author" href="/personal/1/" title="散户小虎"><span>散户小虎</span></a>
          <div class="publish-time">06-23 16:47</div>
        </header>
        <div class="tweet-content"><div class="tweet-content-left">
          <h3 class="tweet-title text-truncate">$贵州茅台(600519)$ 泡沫要破了</h3>
          <div> $贵州茅台(600519)$ 泡沫要破了，还在努力喊韭菜接盘。</div>
        </div></div>
        <a href="/post/578335739913248" target="_blank" class="tweet-link stretched-link" title="$贵州茅台(600519)$ 泡沫要破了">详情</a>
        <div class="action-bar"><div class="action-item">回复 2</div></div>
      </div>
    </div>
    """

    rows = _parse_laohu8_stock_posts(html, symbol="600519.SH")
    raw = {
        **rows[0],
        "keywords": ["600519.SH"],
        "sentiment_score": 0.0,
        "sentiment_label": "bearish",
    }
    post = normalize_laohu8_stock_data_post(raw, symbol="600519.SH")
    thread = normalize_laohu8_stock_thread(raw)

    assert rows[0]["source_thread_id"] == "578335739913248"
    assert rows[0]["author"] == "散户小虎"
    assert rows[0]["reply_count"] == 2
    assert post is not None
    assert post.source == "laohu8_stock"
    assert post.source_post_id == "578335739913248:600519.SH"
    assert post.symbol == "600519.SH"
    assert thread is not None
    assert thread.source == "laohu8_stock"
    assert thread.symbols == ["600519.SH"]


@pytest.mark.asyncio
async def test_collect_laohu8_stock_persists_symbol_page_posts(sentiment_session, monkeypatch):
    service = SentimentIngestService(sentiment_session)
    html = """
    <div class="tweet-item-root">
      <header class="tweet-item-header">
        <a class="tweet-author" href="/personal/1/" title="散户小虎"><span>散户小虎</span></a>
        <div class="publish-time">06-23 16:47</div>
      </header>
      <div class="tweet-content"><div class="tweet-content-left">
        <h3 class="tweet-title">$贵州茅台(600519)$ 感觉低估</h3>
        <div>$贵州茅台(600519)$ 感觉被低估了，看多反弹。</div>
      </div></div>
      <a href="/post/578335739913248" class="tweet-link stretched-link" title="$贵州茅台(600519)$ 感觉低估">详情</a>
      <div class="action-bar"><div class="action-item">回复 1</div></div>
    </div>
    """
    seen_symbols: list[str] = []

    def fake_fetch(symbol: str) -> str:
        seen_symbols.append(symbol)
        return html

    monkeypatch.setattr("app.services.sentiment._fetch_laohu8_stock_html", fake_fetch)
    posts, threads, stats = service._collect_laohu8_stock(
        ["600519.SH"],
        min_reply=0,
        start_date=date(2026, 6, 23),
        end_date=date(2026, 6, 23),
    )

    assert seen_symbols == ["600519.SH"]
    assert stats["collected"] == 1
    assert len(threads) == 1
    assert [(post.source, post.symbol, post.source_post_id) for post in posts] == [
        ("laohu8_stock", "600519.SH", "578335739913248:600519.SH")
    ]


def test_parse_and_normalize_jisilu_payload():
    list_html = """
    <div class="aw-item">
      <span class="aw-question-replay-count aw-border-radius-5 active"><em>40</em> 回复</span>
      <div class="aw-questoin-content">
        <h4>
          <a target="_blank" href="https://www.jisilu.cn/question/522460">600519 还能加仓吗？</a>
          <a href="https://www.jisilu.cn/topic/%E8%82%A1%E7%A5%A8" class="aw-topic-name"><span>股票</span></a>
        </h4>
        <span class="aw-text-color-999">
          <span class="aw-question-tags"><a href="https://www.jisilu.cn/category/8">股票</a></span> •
          <a href="https://www.jisilu.cn/people/alice" class="aw-user-name" data-id="1">alice</a>
          回复 • 2026-06-22 23:33 • 4628 次浏览
        </span>
      </div>
    </div>
    """
    detail_html = """
      <h1>600519 还能加仓吗？</h1>
      <div class="aw-question-detail-txt markitup-box">茅台估值回调，想加仓 600519。</div>
      <div class="aw-question-detail-meta"><span class="aw-text-color-999">发表时间 2026-06-17 20:57</span></div>
      <div class="markitup-box" >看多，继续等反弹</div>
      <span class="pull-left aw-text-color-999">2026-06-22 23:33 来自广东</span>
    """

    rows = _parse_jisilu_list_posts(list_html)
    detail = _parse_jisilu_detail(detail_html)
    raw = {**rows[0], **detail, "published_time": detail["question_published_time"]}
    raw["stock_codes"] = ["600519.SH"]
    raw["keywords"] = ["600519.SH"]
    raw["sentiment_score"] = 1.0
    raw["sentiment_label"] = "bullish"
    post = normalize_jisilu_data_post(raw, symbol="600519.SH")

    assert rows == [
        {
            "question_id": "522460",
            "title": "600519 还能加仓吗？",
            "url": "https://www.jisilu.cn/question/522460",
            "reply_count": 40,
            "author": "alice",
            "active_time": "2026-06-22 23:33",
            "view_count": 4628,
            "meta_text": "alice 回复 • 2026-06-22 23:33 • 4628 次浏览",
        }
    ]
    assert detail["content"] == "茅台估值回调，想加仓 600519。"
    assert detail["comments"] == [{"content": "看多，继续等反弹", "publish_time": "2026-06-22 23:33"}]
    assert post is not None
    assert post.source == "jisilu"
    assert post.source_post_id == "522460:600519.SH"
    assert post.symbol == "600519.SH"
    assert post.author == "alice"
    assert post.published_at == datetime(2026, 6, 17, 20, 57)
    assert post.comment_count == 40


@pytest.mark.asyncio
async def test_collect_jisilu_filters_pages_dates_and_matches_symbols(sentiment_session, monkeypatch):
    service = SentimentIngestService(sentiment_session)
    list_html = """
    <div class="aw-item">
      <span class="aw-question-replay-count aw-border-radius-5 active"><em>2</em> 回复</span>
      <div class="aw-questoin-content">
        <h4><a target="_blank" href="https://www.jisilu.cn/question/1">600519 讨论</a></h4>
        <span class="aw-text-color-999"><span>股票</span> • <a class="aw-user-name">alice</a> 回复 • 2026-06-23 10:00 • 20 次浏览</span>
      </div>
    </div>
    <div class="aw-item">
      <span class="aw-question-replay-count aw-border-radius-5 active"><em>0</em> 回复</span>
      <div class="aw-questoin-content">
        <h4><a target="_blank" href="https://www.jisilu.cn/question/2">评论太少</a></h4>
        <span class="aw-text-color-999"><span>股票</span> • <a class="aw-user-name">bob</a> 回复 • 2026-06-23 11:00 • 3 次浏览</span>
      </div>
    </div>
    """
    detail_html = """
      <h1>600519 讨论</h1>
      <div class="aw-question-detail-txt markitup-box">贵州茅台 600519 看多反弹。</div>
      <div class="aw-question-detail-meta"><span class="aw-text-color-999">发表时间 2026-06-23 09:30</span></div>
    """
    seen_urls: list[str] = []

    def fake_fetch(url: str):
        seen_urls.append(url)
        if "question/1" in url:
            return detail_html
        if "page-2" in url:
            return ""
        return list_html

    monkeypatch.setattr("app.services.sentiment._fetch_jisilu_html", fake_fetch)

    posts, stats = service._collect_jisilu(
        "600519.SH",
        aliases=["600519", "贵州茅台"],
        stock_aliases=None,
        max_pages=2,
        min_reply=1,
        start_date=date(2026, 6, 23),
        end_date=date(2026, 6, 23),
    )

    assert any(url == "https://www.jisilu.cn/category/8" for url in seen_urls)
    assert any(url == "https://www.jisilu.cn/question/1" for url in seen_urls)
    assert stats["collected"] == 2
    assert [post.source_post_id for post in posts] == ["1:600519.SH"]
    assert posts[0].content == "贵州茅台 600519 看多反弹。"


def test_parse_and_normalize_wechat_sogou_payload():
    html = """
    <ul class="news-list">
      <li d="wechat-article-1" id="sogou_vr_11002601_box_0">
        <div class="txt-box">
          <h3>
            <a href="/link?url=abc&amp;type=2" target="_blank">
              开盘啦创始人: 贵州茅台 600519 短线情绪观察
            </a>
          </h3>
          <p class="txt-info">狂龙十八段聊A股，贵州茅台今天资金分歧。</p>
          <div class="s-p">
            <span class="all-time-y2">撬动木星</span>
            <span class="s2"><script>document.write(timeConvert('1773582544'))</script></span>
          </div>
        </div>
      </li>
    </ul>
    """
    rows = _parse_wechat_sogou_articles(html, query="开盘啦 创始人 股票", page=1)

    assert len(rows) == 1
    assert rows[0]["source_thread_id"] == "wechat-article-1"
    assert rows[0]["author"] == "撬动木星"
    assert rows[0]["url"] == "https://weixin.sogou.com/link?url=abc&type=2"

    raw = {
        **rows[0],
        "stock_codes": ["600519.SH"],
        "keywords": ["贵州茅台", "600519.SH"],
        "sentiment_score": 0.55,
        "sentiment_label": "neutral",
    }
    post = normalize_wechat_sogou_data_post(raw, symbol="600519.SH")
    thread = normalize_wechat_sogou_thread(raw)

    assert post is not None
    assert post.source == "wechat_sogou"
    assert post.source_post_id == "wechat-article-1:600519.SH"
    assert post.author == "撬动木星"
    assert thread is not None
    assert thread.source == "wechat_sogou"
    assert thread.symbols == ["600519.SH"]


@pytest.mark.asyncio
async def test_collect_wechat_sogou_matches_symbols_and_threads(sentiment_session, monkeypatch):
    service = SentimentIngestService(sentiment_session)
    html = """
    <ul class="news-list">
      <li d="wechat-article-1" id="sogou_vr_11002601_box_0">
        <div class="txt-box">
          <h3><a href="/link?url=abc&amp;type=2">开盘啦创始人聊贵州茅台 600519</a></h3>
          <p class="txt-info">A股短线资金对贵州茅台有分歧。</p>
          <div class="s-p"><span class="all-time-y2">撬动木星</span><span class="s2"><script>document.write(timeConvert('1773582544'))</script></span></div>
        </div>
      </li>
      <li d="wechat-article-2" id="sogou_vr_11002601_box_1">
        <div class="txt-box">
          <h3><a href="/link?url=def&amp;type=2">无关楼盘开盘</a></h3>
          <p class="txt-info">这是一篇楼市文章。</p>
          <div class="s-p"><span class="all-time-y2">楼市号</span></div>
        </div>
      </li>
    </ul>
    """

    def fake_fetch(query: str, page: int = 1) -> str:
        assert query == "开盘啦 创始人 股票"
        return html if page == 1 else ""

    monkeypatch.setattr("app.services.sentiment._fetch_wechat_sogou_page", fake_fetch)
    posts, threads, stats = service._collect_wechat_sogou(
        None,
        None,
        [("贵州茅台", "600519.SH")],
        ["开盘啦 创始人 股票"],
        2,
    )

    assert stats["collected"] == 2
    assert len(threads) == 2
    assert len(posts) == 1
    assert posts[0].symbol == "600519.SH"
    assert posts[0].source == "wechat_sogou"


@pytest.mark.asyncio
async def test_collect_wechat_sogou_reports_verification_required(sentiment_session, monkeypatch):
    progress_events = []
    service = SentimentIngestService(sentiment_session, progress_callback=progress_events.append)

    def fake_fetch(query: str, page: int = 1) -> str:
        raise RuntimeError("Sogou WeChat returned a verification page; configure WECHAT_SOGOU_COOKIE or retry later")

    monkeypatch.setattr("app.services.sentiment._fetch_wechat_sogou_page", fake_fetch)

    with pytest.raises(RuntimeError, match="verification page"):
        service._collect_wechat_sogou(None, None, [], ["陈小群周策略 股票"], 1)

    assert any(event["stage"] == "wechat_sogou.verification_required" for event in progress_events)


def test_normalize_flocktrader_post_maps_analyzed_post():
    post = normalize_flocktrader_post(
        FlockPostStub(
            tid="999",
            title="600519 讨论",
            content="基本面不错",
            author="nga-user",
            publish_time=datetime(2026, 5, 24, 10, 30),
            reply_count=5,
            sentiment_score=0.7,
            sentiment_label="看多",
            keywords=["白酒", "估值"],
        ),
        symbol="600519.SH",
    )

    assert post is not None
    assert post.source == "flocktrader"
    assert post.source_post_id == "999"
    assert post.sentiment_label == "bullish"
    assert post.comment_count == 5
    assert post.keywords == ["白酒", "估值"]


def test_nga_data_post_matches_code_and_name_aliases():
    raw = {
        "tid": "46800001",
        "title": "中国卫星 今天分歧很大",
        "author": "nga-user",
        "content": "600118 回撤后关注商业航天订单兑现",
        "publish_time": "2026-05-20T10:00:00",
        "last_reply_time": "2026-05-20T11:00:00",
        "reply_count": 3,
        "comments": [
            {"content": "如果中国卫星能站回均线，情绪会改善"}
        ],
        "sentiment_score": 0.62,
        "sentiment_label": "看多",
        "keywords": ["商业航天"],
        "stock_codes": [],
    }

    aliases = ["600118", "中国卫星"]
    assert _post_mentions_symbol(raw, "600118.SH", aliases)

    post = normalize_nga_data_post(raw, symbol="600118.SH", aliases=aliases)

    assert post is not None
    assert post.source == "flocktrader"
    assert post.source_post_id == "46800001:600118.SH"
    assert post.symbol == "600118.SH"
    assert post.published_at == datetime(2026, 5, 20, 11, 0)
    assert post.raw and post.raw["published_at_original"] == "2026-05-20T10:00:00"
    assert post.sentiment_label == "bullish"
    assert post.url == "https://bbs.nga.cn/read.php?tid=46800001"
    assert "商业航天" in post.keywords


def test_normalize_nga_data_posts_expands_detected_symbols_without_explicit_symbol():
    raw = {
        "tid": "46800002",
        "title": "SH600519 与 SZ000001 一起异动",
        "content": "今天重点看 SH600519，同时 SZ000001 也有放量。",
        "author": "nga-user",
        "publish_time": "2026-05-20T10:00:00",
        "reply_count": 2,
        "stock_codes": ["600519", "000001"],
        "keywords": ["白酒", "银行"],
    }

    posts = normalize_nga_data_posts(raw)

    assert [post.symbol for post in posts] == ["600519.SH", "000001.SZ"]
    assert [post.source_post_id for post in posts] == ["46800002:600519.SH", "46800002:000001.SZ"]


def test_normalize_nga_data_posts_matches_stock_aliases_when_codes_missing():
    raw = {
        "tid": "46800003",
        "title": "发车 新北洋",
        "content": "今天盯一下新北洋的走势。",
        "author": "nga-user",
        "publish_time": "2026-05-20T10:00:00",
        "reply_count": 2,
        "stock_codes": [],
        "keywords": [],
    }

    posts = normalize_nga_data_posts(raw, stock_aliases=[("新北洋", "002376.SZ")])

    assert [post.symbol for post in posts] == ["002376.SZ"]


def test_inject_xueqiu_cookie_from_environment(monkeypatch):
    class Context:
        def __init__(self):
            self.cookies = None

        def add_cookies(self, cookies):
            self.cookies = cookies

    class Browser:
        def __init__(self):
            self.contexts = [Context()]

    class Crawler:
        def __init__(self):
            self._browser = Browser()

    crawler = Crawler()
    monkeypatch.setenv("XUEQIU_COOKIE", "xq_a_token=abc; u=123; badpart")

    auth = _inject_xueqiu_cookie(crawler)

    assert crawler._browser.contexts[0].cookies == [
        {
            "name": "xq_a_token",
            "value": "abc",
            "domain": ".xueqiu.com",
            "path": "/",
            "secure": True,
            "httpOnly": False,
        },
        {
            "name": "u",
            "value": "123",
            "domain": ".xueqiu.com",
            "path": "/",
            "secure": True,
            "httpOnly": False,
        },
    ]
    assert auth == {
        "cookie_present": True,
        "cookie_count": 2,
        "login_cookie_present": False,
    }


def test_verify_xueqiu_login_reports_sanitized_status():
    class Page:
        def evaluate(self, script):
            return {"ok": True, "status": 200, "endpoint": "/user/status.json"}

    class Context:
        def cookies(self, url=None):
            return [
                {"name": "xq_a_token", "value": "secret"},
                {"name": "xq_is_login", "value": "1"},
                {"name": "u", "value": "123"},
            ]

    class Browser:
        contexts = [Context()]

    class Crawler:
        _browser = Browser()
        _page = Page()

    auth = _verify_xueqiu_login(
        Crawler(),
        {"cookie_present": True, "cookie_count": 3, "login_cookie_present": True},
    )

    assert auth["cookie_present"] is True
    assert auth["context_login_cookie_present"] is True
    assert auth["server_verified"] is True
    assert "secret" not in str(auth)


def test_resolve_chrome_path_prefers_env(monkeypatch, tmp_path):
    chrome = tmp_path / "chrome.exe"
    chrome.write_text("", encoding="utf-8")
    monkeypatch.setenv("CHROME_PATH", str(chrome))

    assert _resolve_chrome_path("missing.exe") == str(chrome)
