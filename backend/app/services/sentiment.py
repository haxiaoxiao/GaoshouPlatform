from __future__ import annotations

import asyncio
import json
import os
import re
import sys
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any, Iterable

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.sentiment import SentimentPost
from app.db.models.stock import Stock
from app.services.security_symbols import normalize_security_symbol


CANONICAL_SOURCES = {"xueqiu_spyder", "flocktrader"}
SOURCE_ALIASES = {
    "xueqiu": "xueqiu_spyder",
    "xueqiu-spyder": "xueqiu_spyder",
    "xueqiu_spyder": "xueqiu_spyder",
    "flocktrader": "flocktrader",
    "flock-trader": "flocktrader",
    "nga": "flocktrader",
}
LEGACY_SOURCE_NAMES = {
    "xueqiu_spyder": {"xueqiu", "xueqiu_spyder"},
    "flocktrader": {"nga", "flocktrader"},
}
SUPPORTED_SOURCES = set(SOURCE_ALIASES)


@dataclass
class SentimentPostInput:
    source: str
    source_post_id: str
    symbol: str
    title: str | None = None
    content: str | None = None
    author: str | None = None
    published_at: datetime | None = None
    url: str | None = None
    reply_count: int = 0
    like_count: int = 0
    comment_count: int = 0
    sentiment_score: float | None = None
    sentiment_label: str | None = None
    keywords: list[str] = field(default_factory=list)
    raw: dict[str, Any] | None = None


@dataclass
class NgaIngestStats:
    loaded_dates: list[str] = field(default_factory=list)
    crawled_dates: list[str] = field(default_factory=list)
    date_files: list[str] = field(default_factory=list)
    total_posts: int = 0
    analyzed_posts: int = 0
    matched_posts: int = 0


def normalize_sentiment_source(source: str) -> str:
    normalized = SOURCE_ALIASES.get(str(source or "").strip().lower().replace(" ", "_"))
    if not normalized:
        raise ValueError(f"unsupported sentiment source: {source}")
    return normalized


def parse_sources(raw: str | None) -> list[str] | None:
    if not raw:
        return None
    sources = [normalize_sentiment_source(item) for item in raw.split(",") if item.strip()]
    return list(dict.fromkeys(sources))


def _source_storage_values(sources: list[str] | None) -> list[str] | None:
    if not sources:
        return None
    values: list[str] = []
    for source in sources:
        canonical = normalize_sentiment_source(source)
        values.extend(sorted(LEGACY_SOURCE_NAMES.get(canonical, {canonical})))
    return list(dict.fromkeys(values))


def normalize_sentiment_symbol(symbol: str) -> str:
    normalized = normalize_security_symbol(symbol)
    if not normalized:
        raise ValueError(f"invalid security symbol: {symbol!r}")
    return normalized


def _date_bounds(start_date: date | None, end_date: date | None) -> tuple[datetime | None, datetime | None]:
    start = datetime.combine(start_date, time.min) if start_date else None
    end = datetime.combine(end_date + timedelta(days=1), time.min) if end_date else None
    return start, end


def _json_list(value: list[str]) -> str:
    return json.dumps([str(item) for item in value if str(item).strip()], ensure_ascii=False)


def _loads_list(value: str | None) -> list[str]:
    if not value:
        return []
    try:
        loaded = json.loads(value)
    except json.JSONDecodeError:
        return []
    return loaded if isinstance(loaded, list) else []


def _label_from_score(score: float | None) -> str | None:
    if score is None:
        return None
    if score >= 0.6:
        return "bullish"
    if score <= 0.4:
        return "bearish"
    return "neutral"


def _normalize_sentiment_label(label: str | None, score: float | None = None) -> str:
    text = str(label or "").strip().lower()
    if text in {"bullish", "看多", "positive", "long"}:
        return "bullish"
    if text in {"bearish", "看空", "negative", "short"}:
        return "bearish"
    if text in {"neutral", "中性", "mixed"}:
        return "neutral"
    return _label_from_score(score) or "neutral"


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return max(int(value or default), 0)
    except (TypeError, ValueError):
        return default


def _strip_html_fallback(text: str | None) -> str:
    if not text:
        return ""
    clean = re.sub(r"<[^>]+>", "", str(text))
    clean = re.sub(r"\$([^$]+)\$", r"\1", clean)
    return clean.strip()


def _read_attr_or_key(value: Any, name: str, default: Any = None) -> Any:
    if isinstance(value, dict):
        return value.get(name, default)
    return getattr(value, name, default)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _nga_data_dir() -> Path:
    raw = os.environ.get("NGA_DATA_DIR")
    return Path(raw).expanduser() if raw else _repo_root() / "data" / "sentiment" / "NGAdata"


def _date_range(start: date, end: date) -> Iterable[date]:
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def _date_file_name(day: date) -> str:
    return f"posts_{day.isoformat()}.json"


def _read_json_list(path: Path) -> list[dict[str, Any]]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise
    except json.JSONDecodeError as exc:
        raise ValueError(f"NGA data file is not valid JSON: {path}") from exc
    if not isinstance(payload, list):
        raise ValueError(f"NGA data file must contain a JSON list: {path}")
    return [item for item in payload if isinstance(item, dict)]


def _write_json_list(path: Path, posts: list[Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    serializable = [
        post.to_dict() if hasattr(post, "to_dict") else dict(post)
        for post in posts
    ]
    path.write_text(json.dumps(serializable, ensure_ascii=False, indent=2), encoding="utf-8")


def _text_parts(raw: dict[str, Any]) -> list[str]:
    parts = [str(raw.get("title") or ""), str(raw.get("content") or "")]
    comments = raw.get("comments") or []
    if isinstance(comments, list):
        for comment in comments:
            if isinstance(comment, dict):
                parts.append(str(comment.get("content") or ""))
    return [part for part in parts if part]


def _post_text(raw: dict[str, Any]) -> str:
    return " ".join(_text_parts(raw))


def _symbol_aliases_from_parts(symbol: str, stock: Stock | None = None) -> list[str]:
    normalized = normalize_sentiment_symbol(symbol)
    code, _, suffix = normalized.partition(".")
    aliases = [normalized, code, f"{suffix}{code}" if suffix else code]
    if stock is not None:
        aliases.extend(
            [
                stock.name,
                stock.company_name,
                _short_company_name(stock.company_name),
            ]
        )
    return [item for item in dict.fromkeys(str(v).strip() for v in aliases if v) if item]


def _short_company_name(name: str | None) -> str | None:
    if not name:
        return None
    short = str(name).strip()
    for suffix in ("股份有限公司", "有限责任公司", "有限公司", "集团股份", "集团"):
        short = short.replace(suffix, "")
    return short or None


def _post_mentions_symbol(raw: dict[str, Any], symbol: str, aliases: list[str] | None = None) -> bool:
    normalized = normalize_sentiment_symbol(symbol)
    code = normalized.split(".", 1)[0]
    raw_codes = raw.get("stock_codes") or []
    codes = {str(item).strip().upper() for item in raw_codes if str(item).strip()}
    if code in codes or normalized in codes:
        return True

    text = _post_text(raw).upper()
    if not text:
        return False
    for alias in aliases or _symbol_aliases_from_parts(normalized):
        alias_text = str(alias).strip()
        if alias_text and alias_text.upper() in text:
            return True
    return False


def _in_date_window(value: datetime | None, start_date: date | None, end_date: date | None) -> bool:
    if value is None or (start_date is None and end_date is None):
        return True
    if start_date is not None and value.date() < start_date:
        return False
    if end_date is not None and value.date() > end_date:
        return False
    return True


def _open_xueqiu_stock_page(crawler: Any, xq_symbol: str) -> None:
    page = getattr(crawler, "_page", None)
    if page is None:
        raise RuntimeError("xueqiu-spyder crawler does not expose a browser page")
    url = f"https://xueqiu.com/S/{xq_symbol}"
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=15_000)
        page.wait_for_timeout(500)
    except Exception as exc:
        raise RuntimeError(f"failed to open Xueqiu stock page {url}: {exc}") from exc


def _resolve_chrome_path(default_path: str | None = None) -> str:
    candidates = [
        os.environ.get("CHROME_PATH"),
        default_path,
        os.path.expandvars(r"%LOCALAPPDATA%\Google\Chrome\Application\chrome.exe"),
        os.path.expandvars(r"%ProgramFiles%\Google\Chrome\Application\chrome.exe"),
        os.path.expandvars(r"%ProgramFiles(x86)%\Google\Chrome\Application\chrome.exe"),
        os.path.expandvars(r"%LOCALAPPDATA%\Microsoft\Edge\Application\msedge.exe"),
        os.path.expandvars(r"%ProgramFiles%\Microsoft\Edge\Application\msedge.exe"),
        os.path.expandvars(r"%ProgramFiles(x86)%\Microsoft\Edge\Application\msedge.exe"),
    ]
    for candidate in candidates:
        if candidate and Path(candidate).exists():
            return str(candidate)
    raise RuntimeError("Chrome or Edge executable was not found; set CHROME_PATH")


def _inject_xueqiu_cookie(crawler: Any) -> dict[str, Any]:
    raw_cookie = os.environ.get("XUEQIU_COOKIE", "").strip()
    if not raw_cookie:
        return {
            "cookie_present": False,
            "cookie_count": 0,
            "login_cookie_present": False,
        }

    browser = getattr(crawler, "_browser", None)
    contexts = getattr(browser, "contexts", None) if browser is not None else None
    if not contexts:
        raise RuntimeError("xueqiu-spyder crawler does not expose a browser context")

    cookies = []
    for item in raw_cookie.split(";"):
        if "=" not in item:
            continue
        name, value = item.split("=", 1)
        name = name.strip()
        if not name:
            continue
        cookies.append(
            {
                "name": name,
                "value": value.strip(),
                "domain": ".xueqiu.com",
                "path": "/",
                "secure": True,
                "httpOnly": False,
            }
        )
    if not cookies:
        raise RuntimeError("XUEQIU_COOKIE did not contain any parsable cookies")
    contexts[0].add_cookies(cookies)
    cookie_names = {cookie["name"] for cookie in cookies}
    login_cookie_present = bool(
        {"xq_a_token", "xqat", "xq_id_token", "u"} & cookie_names
    ) and any(cookie["name"] == "xq_is_login" and cookie["value"] == "1" for cookie in cookies)
    return {
        "cookie_present": True,
        "cookie_count": len(cookies),
        "login_cookie_present": login_cookie_present,
    }


def _verify_xueqiu_login(crawler: Any, injected_auth: dict[str, Any] | None = None) -> dict[str, Any]:
    """Return sanitized Xueqiu auth diagnostics without exposing cookie values."""
    auth = dict(injected_auth or {})
    page = getattr(crawler, "_page", None)
    if page is None:
        auth.update({"server_verified": False, "server_error": "crawler has no page"})
        return auth

    browser = getattr(crawler, "_browser", None)
    contexts = getattr(browser, "contexts", None) if browser is not None else None
    context = contexts[0] if contexts else None
    try:
        context_cookies = context.cookies("https://xueqiu.com") if context is not None else []
    except TypeError:
        context_cookies = context.cookies() if context is not None else []
    except Exception as exc:
        context_cookies = []
        auth["context_cookie_error"] = str(exc)

    cookie_names = {str(cookie.get("name")) for cookie in context_cookies if isinstance(cookie, dict)}
    auth["context_cookie_count"] = len(context_cookies)
    auth["context_login_cookie_present"] = bool(
        {"xq_a_token", "xqat", "xq_id_token", "u"} & cookie_names
    ) and "xq_is_login" in cookie_names

    try:
        result = page.evaluate(
            """async () => {
                const candidates = [
                    '/statuses/home_timeline.json?page=1&count=1',
                    '/v4/statuses/home_timeline.json?page=1&count=1',
                    '/user/status.json',
                    '/users/self.json'
                ];
                for (const url of candidates) {
                    try {
                        const resp = await fetch(url, {credentials: 'include'});
                        const ct = resp.headers.get('content-type') || '';
                        let payload = null;
                        if (ct.includes('json')) {
                            payload = await resp.json();
                        }
                        const hasUser =
                            payload && (
                                payload.id || payload.user_id || payload.screen_name ||
                                payload.profile || payload.user || payload.uid
                            );
                        if (resp.ok && hasUser) {
                            return {ok: true, status: resp.status, endpoint: url};
                        }
                        if (resp.ok && payload && payload.error_code === 0) {
                            return {ok: true, status: resp.status, endpoint: url};
                        }
                        if (payload && payload.error_code) {
                            return {
                                ok: false,
                                status: resp.status,
                                endpoint: url,
                                error_code: payload.error_code,
                                error_description: payload.error_description
                            };
                        }
                    } catch (e) {
                        // Try the next endpoint before giving up.
                    }
                }
                return {ok: false, status: null, endpoint: null};
            }"""
        )
        auth["server_verified"] = bool(result.get("ok")) if isinstance(result, dict) else False
        auth["server_status"] = result.get("status") if isinstance(result, dict) else None
        auth["server_endpoint"] = result.get("endpoint") if isinstance(result, dict) else None
        if isinstance(result, dict) and result.get("error_code"):
            auth["server_error_code"] = result.get("error_code")
            auth["server_error_description"] = result.get("error_description")
    except Exception as exc:
        auth["server_verified"] = False
        auth["server_error"] = str(exc)

    return auth


@contextmanager
def _temporary_project_imports(project_dir: Path, module_names: list[str]):
    """Import external projects that use generic module names such as config.py."""
    project_text = str(project_dir)
    previous_modules = {name: sys.modules.get(name) for name in module_names}
    previous_path = list(sys.path)
    for name in module_names:
        sys.modules.pop(name, None)
    sys.path.insert(0, project_text)
    try:
        yield
    finally:
        sys.path = previous_path
        for name in module_names:
            old_module = previous_modules[name]
            if old_module is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = old_module


def _parse_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    if isinstance(value, (int, float)):
        raw = float(value)
        if raw > 10_000_000_000:
            raw = raw / 1000
        return datetime.fromtimestamp(raw)
    if isinstance(value, str):
        text = value.strip()
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
            try:
                return datetime.strptime(text, fmt)
            except ValueError:
                continue
        try:
            return datetime.fromisoformat(text.replace("Z", "+00:00")).replace(tzinfo=None)
        except ValueError:
            return None
    return None


class SentimentService:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def upsert_posts(self, posts: list[SentimentPostInput]) -> int:
        count = 0
        for post in posts:
            post.source = normalize_sentiment_source(post.source)
            post.symbol = normalize_sentiment_symbol(post.symbol)
            if not post.source_post_id:
                raise ValueError("source_post_id is required")
            label = _normalize_sentiment_label(post.sentiment_label, post.sentiment_score)
            storage_sources = _source_storage_values([post.source]) or [post.source]
            stmt = select(SentimentPost).where(
                SentimentPost.source.in_(storage_sources),
                SentimentPost.source_post_id == str(post.source_post_id),
            )
            existing_rows = list((await self.session.execute(stmt)).scalars().all())
            existing = next((row for row in existing_rows if row.source == post.source), None)
            existing = existing or (existing_rows[0] if existing_rows else None)
            for duplicate in existing_rows:
                if existing is not None and duplicate.id != existing.id:
                    await self.session.delete(duplicate)
            target = existing or SentimentPost(
                source=post.source,
                source_post_id=str(post.source_post_id),
                symbol=post.symbol,
            )
            target.source = post.source
            target.symbol = post.symbol
            target.title = post.title
            target.content = post.content
            target.author = post.author
            target.published_at = post.published_at
            target.url = post.url
            target.reply_count = max(int(post.reply_count or 0), 0)
            target.like_count = max(int(post.like_count or 0), 0)
            target.comment_count = max(int(post.comment_count or 0), 0)
            target.sentiment_score = post.sentiment_score
            target.sentiment_label = label
            target.keywords_json = _json_list(post.keywords)
            target.raw_json = json.dumps(post.raw or {}, ensure_ascii=False)
            if existing is None:
                self.session.add(target)
            count += 1
        await self.session.flush()
        return count

    async def list_posts(
        self,
        symbol: str,
        start_date: date | None = None,
        end_date: date | None = None,
        sources: list[str] | None = None,
        limit: int = 50,
    ) -> list[SentimentPost]:
        symbol = normalize_sentiment_symbol(symbol)
        start, end = _date_bounds(start_date, end_date)
        stmt = select(SentimentPost).where(SentimentPost.symbol == symbol)
        if start is not None:
            stmt = stmt.where(SentimentPost.published_at >= start)
        if end is not None:
            stmt = stmt.where(SentimentPost.published_at < end)
        storage_sources = _source_storage_values(sources)
        if storage_sources:
            stmt = stmt.where(SentimentPost.source.in_(storage_sources))
        stmt = stmt.order_by(
            SentimentPost.reply_count.desc(),
            SentimentPost.like_count.desc(),
            SentimentPost.published_at.desc().nullslast(),
        ).limit(max(1, min(limit, 200)))
        return list((await self.session.execute(stmt)).scalars().all())

    async def summary(
        self,
        symbol: str,
        start_date: date | None = None,
        end_date: date | None = None,
        sources: list[str] | None = None,
    ) -> dict[str, Any]:
        posts = await self.list_posts(symbol, start_date, end_date, sources, limit=200)
        by_source: dict[str, list[SentimentPost]] = {}
        for post in posts:
            source = normalize_sentiment_source(post.source)
            by_source.setdefault(source, []).append(post)

        rows = []
        for source, source_posts in sorted(by_source.items()):
            labels = [str(p.sentiment_label or "neutral").lower() for p in source_posts]
            scores = [p.sentiment_score for p in source_posts if p.sentiment_score is not None]
            keywords: dict[str, int] = {}
            for post in source_posts:
                for keyword in _loads_list(post.keywords_json):
                    keywords[str(keyword)] = keywords.get(str(keyword), 0) + 1
            top_keywords = sorted(keywords, key=keywords.get, reverse=True)[:10]
            rows.append(
                {
                    "source": source,
                    "post_count": len(source_posts),
                    "comment_count": sum(p.comment_count or p.reply_count or 0 for p in source_posts),
                    "bullish_ratio": round(labels.count("bullish") / len(labels), 4) if labels else 0,
                    "bearish_ratio": round(labels.count("bearish") / len(labels), 4) if labels else 0,
                    "avg_sentiment": round(sum(scores) / len(scores), 4) if scores else None,
                    "top_keywords": ",".join(top_keywords),
                }
            )

        return {
            "symbol": normalize_sentiment_symbol(symbol),
            "start_date": start_date.isoformat() if start_date else None,
            "end_date": end_date.isoformat() if end_date else None,
            "sources": rows,
            "hottest_posts": [serialize_post(post) for post in posts[:5]],
        }


def serialize_post(post: SentimentPost) -> dict[str, Any]:
    return {
        "id": post.id,
        "source": normalize_sentiment_source(post.source),
        "source_post_id": post.source_post_id,
        "symbol": post.symbol,
        "title": post.title,
        "content": post.content,
        "author": post.author,
        "published_at": post.published_at.isoformat() if post.published_at else None,
        "url": post.url,
        "reply_count": post.reply_count,
        "like_count": post.like_count,
        "comment_count": post.comment_count,
        "sentiment_score": post.sentiment_score,
        "sentiment_label": post.sentiment_label,
        "keywords": _loads_list(post.keywords_json),
    }


class SentimentIngestService:
    """Best-effort wrapper around local external crawler projects."""

    def __init__(self, session: AsyncSession):
        self.session = session
        self.service = SentimentService(session)

    async def run(
        self,
        source: str,
        symbol: str,
        max_pages: int = 3,
        min_reply: int = 20,
        start_date: date | None = None,
        end_date: date | None = None,
        force_refresh: bool = False,
    ) -> dict[str, Any]:
        source = normalize_sentiment_source(source)
        symbol = normalize_sentiment_symbol(symbol)
        if source == "xueqiu_spyder":
            posts, xueqiu_stats = await asyncio.to_thread(
                self._collect_xueqiu,
                symbol,
                max_pages,
                min_reply,
                start_date,
                end_date,
            )
            stats: dict[str, Any] = {
                "mode": "stock_page",
                "collected": len(posts),
                "matched": len(posts),
                "page_url": f"https://xueqiu.com/S/{_to_xueqiu_symbol(symbol)}",
                **xueqiu_stats,
            }
        elif source == "flocktrader":
            posts, nga_stats = await self._collect_flocktrader_by_date(
                symbol,
                max_pages=max_pages,
                start_date=start_date,
                end_date=end_date,
                force_refresh=force_refresh,
            )
            stats = {
                "mode": "daily_cache",
                "collected": nga_stats.total_posts,
                "analyzed": nga_stats.analyzed_posts,
                "matched": nga_stats.matched_posts,
                "loaded_dates": nga_stats.loaded_dates,
                "crawled_dates": nga_stats.crawled_dates,
                "date_files": nga_stats.date_files,
            }
        else:
            raise ValueError(f"unsupported sentiment source: {source}")
        inserted = await self.service.upsert_posts(posts)
        return {"source": source, "symbol": symbol, **stats, "upserted": inserted}

    def _collect_xueqiu(
        self,
        symbol: str,
        max_pages: int,
        min_reply: int,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> tuple[list[SentimentPostInput], dict[str, Any]]:
        project_dir = Path(os.environ.get("XUEQIU_SPYDER_DIR", r"E:\Projects\xueqiu-spyder"))
        if not project_dir.exists():
            raise RuntimeError(f"xueqiu-spyder directory not found: {project_dir}")
        with _temporary_project_imports(project_dir, ["config", "crawler", "analyzer"]):
            import crawler as xueqiu_crawler_module
            try:
                from analyzer import _strip_html
            except ImportError:
                _strip_html = _strip_html_fallback

            xueqiu_crawler_module.CHROME_PATH = _resolve_chrome_path(
                getattr(xueqiu_crawler_module, "CHROME_PATH", None)
            )
            crawler = xueqiu_crawler_module.XueqiuCrawler()
            try:
                xq_symbol = _to_xueqiu_symbol(symbol)
                auth = _inject_xueqiu_cookie(crawler)
                _open_xueqiu_stock_page(crawler, xq_symbol)
                auth = _verify_xueqiu_login(crawler, auth)
                raw_posts: list[dict[str, Any]] = []
                for page in range(1, max_pages + 1):
                    raw_posts.extend(crawler.get_stock_posts(xq_symbol, page=page))
                normalized: list[SentimentPostInput] = []
                for raw in raw_posts:
                    post = normalize_xueqiu_spyder_post(raw, symbol=symbol, strip_html=_strip_html)
                    if post is None:
                        continue
                    reply_count = post.reply_count
                    if reply_count < min_reply:
                        continue
                    if not _in_date_window(post.published_at, start_date, end_date):
                        continue
                    normalized.append(post)
                return [post for post in normalized if post.source_post_id], {"auth": auth}
            finally:
                crawler.close()

    def _collect_flocktrader(self, symbol: str, max_pages: int) -> list[SentimentPostInput]:
        project_dir = Path(os.environ.get("FLOCKTRADER_DIR", r"E:\Projects\flocktrader"))
        if not project_dir.exists():
            raise RuntimeError(f"flocktrader directory not found: {project_dir}")
        with _temporary_project_imports(project_dir, ["config", "models", "crawler", "analyzer"]):
            from analyzer import KeywordExtractor, SentimentAnalyzer
            from crawler import NGACrawler

            sentiment_analyzer = SentimentAnalyzer()
            keyword_extractor = KeywordExtractor()
            crawler = NGACrawler(cookies=os.environ.get("NGA_COOKIE", ""))
            topics = crawler.scan_topics(max_pages=max_pages)
            result: list[SentimentPostInput] = []
            for topic in topics:
                post = crawler.crawl_topic(
                    str(topic.get("tid", "")),
                    title=topic.get("title", ""),
                    last_reply_time=topic.get("last_reply_time"),
                )
                if post is None:
                    continue
                post = sentiment_analyzer.analyze_post(post)
                post = keyword_extractor.analyze_post(post)
                stock_codes = getattr(post, "stock_codes", None) or keyword_extractor.extract_stock_codes(
                    f"{getattr(post, 'title', '')} {getattr(post, 'content', '')}"
                )
                if symbol[:6] not in stock_codes and symbol not in stock_codes:
                    continue
                normalized = normalize_flocktrader_post(post, symbol=symbol)
                if normalized is not None:
                    result.append(normalized)
            return [post for post in result if post.source_post_id]

    async def _collect_flocktrader_by_date(
        self,
        symbol: str,
        *,
        max_pages: int,
        start_date: date | None,
        end_date: date | None,
        force_refresh: bool,
    ) -> tuple[list[SentimentPostInput], NgaIngestStats]:
        today = date.today()
        start = start_date or end_date or today
        end = end_date or start
        if start > end:
            raise ValueError("start_date cannot be after end_date")

        stock = await self.session.get(Stock, symbol)
        aliases = _symbol_aliases_from_parts(symbol, stock)
        raw_posts, stats = self._load_or_crawl_nga_date_posts(
            start,
            end,
            max_pages=max_pages,
            force_refresh=force_refresh,
        )
        stats.total_posts = len(raw_posts)

        analyzed = self._analyze_nga_posts(raw_posts)
        stats.analyzed_posts = len(analyzed)

        normalized: list[SentimentPostInput] = []
        for raw in analyzed:
            if not _post_mentions_symbol(raw, symbol, aliases):
                continue
            post = normalize_nga_data_post(raw, symbol=symbol, aliases=aliases)
            if post is not None:
                normalized.append(post)
        stats.matched_posts = len(normalized)
        return normalized, stats

    def _load_or_crawl_nga_date_posts(
        self,
        start: date,
        end: date,
        *,
        max_pages: int,
        force_refresh: bool,
    ) -> tuple[list[dict[str, Any]], NgaIngestStats]:
        stats = NgaIngestStats()
        data_dir = _nga_data_dir()
        raw_posts: list[dict[str, Any]] = []
        for day in _date_range(start, end):
            path = data_dir / _date_file_name(day)
            if path.exists() and not force_refresh:
                posts = _read_json_list(path)
                stats.loaded_dates.append(day.isoformat())
            else:
                posts = self._crawl_nga_day(day, max_pages=max_pages)
                _write_json_list(path, posts)
                stats.crawled_dates.append(day.isoformat())
            stats.date_files.append(str(path))
            raw_posts.extend(posts)
        return raw_posts, stats

    def _crawl_nga_day(self, day: date, *, max_pages: int) -> list[Any]:
        project_dir = Path(os.environ.get("FLOCKTRADER_DIR", r"E:\Projects\flocktrader"))
        if not project_dir.exists():
            raise RuntimeError(f"flocktrader directory not found: {project_dir}")
        cookie = os.environ.get("NGA_COOKIE", "")
        if not cookie:
            raise RuntimeError(
                "NGA_COOKIE is required to crawl missing NGAdata files; "
                "set it in the environment or pre-populate data/sentiment/NGAdata"
            )

        since = datetime.combine(day, time.min)
        until = datetime.combine(day, time.max.replace(microsecond=0))
        with _temporary_project_imports(project_dir, ["config", "models", "crawler"]):
            from config import get_fid, load_config
            from crawler import NGACrawler

            fid = get_fid(load_config())
            crawler = NGACrawler(cookies=cookie)
            topics = crawler.scan_topics(
                fid=fid,
                since_date=since,
                until_date=until,
                max_pages=max_pages,
            )
            posts = []
            for topic in topics:
                post = crawler.crawl_topic(
                    tid=str(topic.get("tid", "")),
                    title=str(topic.get("title", "")),
                    last_reply_time=topic.get("last_reply_time"),
                    since_date=since,
                )
                if post is not None:
                    posts.append(post)
            return posts

    def _analyze_nga_posts(self, raw_posts: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not raw_posts:
            return []
        project_dir = Path(os.environ.get("FLOCKTRADER_DIR", r"E:\Projects\flocktrader"))
        if not project_dir.exists():
            raise RuntimeError(f"flocktrader directory not found: {project_dir}")
        with _temporary_project_imports(project_dir, ["config", "models", "analyzer"]):
            from analyzer import DailyAnalyzer
            from models import Post

            posts = [Post.from_dict(raw) for raw in raw_posts]
            analyzed = DailyAnalyzer().analyze_posts(posts)
            return [post.to_dict() for post in analyzed]


def _to_xueqiu_symbol(symbol: str) -> str:
    code, exchange = symbol.split(".")
    return f"{exchange}{code}"


def normalize_xueqiu_spyder_post(
    raw: dict[str, Any],
    *,
    symbol: str,
    strip_html: Any = _strip_html_fallback,
) -> SentimentPostInput | None:
    source_post_id = str(raw.get("id") or raw.get("target") or "").strip()
    if not source_post_id:
        return None
    target = str(raw.get("target") or "")
    user = raw.get("user") or {}
    title = strip_html(raw.get("title") or "")
    content = strip_html(raw.get("text") or raw.get("description") or "")
    reply_count = _to_int(raw.get("reply_count"))
    like_count = _to_int(raw.get("like_count"))
    return SentimentPostInput(
        source="xueqiu_spyder",
        source_post_id=source_post_id,
        symbol=symbol,
        title=title or None,
        content=content or None,
        author=user.get("screen_name") if isinstance(user, dict) else None,
        published_at=_parse_datetime(raw.get("created_at")),
        url=f"https://xueqiu.com{target}" if target.startswith("/") else None,
        reply_count=reply_count,
        like_count=like_count,
        comment_count=reply_count,
        raw=raw,
    )


def normalize_flocktrader_post(post: Any, *, symbol: str) -> SentimentPostInput | None:
    source_post_id = str(_read_attr_or_key(post, "tid", "") or "").strip()
    if not source_post_id:
        return None
    comments = list(_read_attr_or_key(post, "comments", []) or [])
    reply_count = _to_int(_read_attr_or_key(post, "reply_count", len(comments)))
    score = _read_attr_or_key(post, "sentiment_score", None)
    try:
        score = float(score) if score is not None else None
    except (TypeError, ValueError):
        score = None
    raw = post.to_dict() if hasattr(post, "to_dict") else dict(_read_attr_or_key(post, "__dict__", {}) or {})
    return SentimentPostInput(
        source="flocktrader",
        source_post_id=source_post_id,
        symbol=symbol,
        title=_read_attr_or_key(post, "title", None),
        content=_read_attr_or_key(post, "content", None),
        author=_read_attr_or_key(post, "author", None),
        published_at=_parse_datetime(_read_attr_or_key(post, "publish_time", None)),
        reply_count=reply_count,
        comment_count=reply_count,
        sentiment_score=score,
        sentiment_label=_normalize_sentiment_label(_read_attr_or_key(post, "sentiment_label", None), score),
        keywords=[str(item) for item in (_read_attr_or_key(post, "keywords", []) or []) if str(item).strip()],
        raw=raw,
    )


def normalize_nga_data_post(
    raw: dict[str, Any],
    *,
    symbol: str,
    aliases: list[str] | None = None,
) -> SentimentPostInput | None:
    tid = str(raw.get("tid") or "").strip()
    if not tid:
        return None
    symbol = normalize_sentiment_symbol(symbol)
    comments = raw.get("comments") or []
    reply_count = _to_int(raw.get("reply_count"), len(comments) if isinstance(comments, list) else 0)
    score = raw.get("sentiment_score")
    try:
        score = float(score) if score is not None else None
    except (TypeError, ValueError):
        score = None
    keywords = [str(item) for item in (raw.get("keywords") or []) if str(item).strip()]
    if aliases:
        for alias in aliases:
            if alias and alias not in keywords and len(str(alias)) >= 2:
                keywords.append(str(alias))
    return SentimentPostInput(
        source="flocktrader",
        source_post_id=f"{tid}:{symbol}",
        symbol=symbol,
        title=str(raw.get("title") or "") or None,
        content=str(raw.get("content") or "") or None,
        author=str(raw.get("author") or "") or None,
        published_at=_parse_datetime(raw.get("publish_time") or raw.get("last_reply_time")),
        url=f"https://bbs.nga.cn/read.php?tid={tid}",
        reply_count=reply_count,
        comment_count=reply_count,
        sentiment_score=score,
        sentiment_label=_normalize_sentiment_label(str(raw.get("sentiment_label") or ""), score),
        keywords=keywords,
        raw=raw,
    )
