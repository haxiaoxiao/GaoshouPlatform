from dataclasses import dataclass, field
from datetime import date, datetime

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.db.models.base import Base
from app.db.models.sentiment import SentimentPost
from app.services.sentiment import (
    SentimentPostInput,
    SentimentService,
    _inject_xueqiu_cookie,
    _post_mentions_symbol,
    _resolve_chrome_path,
    _verify_xueqiu_login,
    normalize_flocktrader_post,
    normalize_nga_data_post,
    normalize_sentiment_source,
    normalize_xueqiu_spyder_post,
    parse_sources,
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
    assert parse_sources("xueqiu_spyder,flocktrader,xueqiu") == ["xueqiu_spyder", "flocktrader"]
    assert normalize_sentiment_source("xueqiu-spyder") == "xueqiu_spyder"
    with pytest.raises(ValueError):
        parse_sources("reddit")


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
    assert post.sentiment_label == "bullish"
    assert post.url == "https://bbs.nga.cn/read.php?tid=46800001"
    assert "商业航天" in post.keywords


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
