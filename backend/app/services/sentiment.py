from __future__ import annotations

import asyncio
import html
import json
import os
import re
import subprocess
import sys
import time as time_module
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any, Iterable

import requests
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.db.models.sentiment import SentimentPost
from app.db.models.stock import Stock
from app.services.security_symbols import normalize_security_symbol

DEFAULT_SOURCE_ORDER = ("xueqiu_spyder", "flocktrader")
CANONICAL_SOURCES = set(DEFAULT_SOURCE_ORDER)
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


def ordered_sentiment_sources(sources: Iterable[str] | None = None) -> list[str]:
    raw_sources = list(sources) if sources else list(DEFAULT_SOURCE_ORDER)
    ordered: list[str] = []
    for source in raw_sources:
        normalized = normalize_sentiment_source(source)
        if normalized not in ordered:
            ordered.append(normalized)
    return ordered


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


def _config_value(env_name: str, setting_name: str, default: str = "") -> str:
    env_value = os.environ.get(env_name)
    if env_value is not None and str(env_value).strip():
        return str(env_value).strip()
    value = getattr(settings, setting_name, default)
    return str(value).strip() if value is not None else default


def _nga_data_dir() -> Path:
    raw = _config_value("NGA_DATA_DIR", "nga_data_dir", str(_repo_root() / "data" / "sentiment" / "NGAdata"))
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


def _xueqiu_profile_dir() -> Path:
    return Path(_config_value("XUEQIU_USER_DATA_DIR", "xueqiu_user_data_dir", str(_repo_root() / "data" / "sentiment" / "xueqiu-profile"))).expanduser()


def _xueqiu_project_dir() -> Path:
    raw = _config_value("XUEQIU_SPYDER_DIR", "xueqiu_spyder_dir", "")
    if raw:
        return Path(raw).expanduser()
    return _xueqiu_profile_dir()


def _flocktrader_project_dir() -> Path:
    return Path(_config_value("FLOCKTRADER_DIR", "flocktrader_dir", r"E:\Projects\flocktrader")).expanduser()


def _xueqiu_debug_port() -> int:
    try:
        return max(int(_config_value("XUEQIU_DEBUG_PORT", "xueqiu_debug_port", "9222")), 1)
    except ValueError:
        return 9222


class _BuiltinXueqiuCrawler:
    def __init__(self):
        self._playwright = None
        self._browser = None
        self._page = None
        self._connect()

    def _connect(self) -> None:
        try:
            from playwright.sync_api import sync_playwright
        except ImportError as exc:
            raise RuntimeError("playwright is not installed; install backend requirements to enable built-in Xueqiu sync") from exc

        port = _xueqiu_debug_port()
        self._playwright = sync_playwright().start()
        try:
            self._browser = self._playwright.chromium.connect_over_cdp(f"http://127.0.0.1:{port}")
        except Exception:
            chrome_path = _resolve_chrome_path(_config_value("XUEQIU_CHROME_PATH", "xueqiu_chrome_path"))
            profile_dir = _xueqiu_profile_dir()
            profile_dir.mkdir(parents=True, exist_ok=True)
            subprocess.Popen(
                [
                    chrome_path,
                    f"--remote-debugging-port={port}",
                    f"--user-data-dir={profile_dir}",
                    "--no-first-run",
                    "https://xueqiu.com/",
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            time_module.sleep(3)
            self._browser = self._playwright.chromium.connect_over_cdp(f"http://127.0.0.1:{port}")

        contexts = getattr(self._browser, "contexts", [])
        if contexts and contexts[0].pages:
            self._page = contexts[0].pages[0]
        elif contexts:
            self._page = contexts[0].new_page()
        else:
            context = self._browser.new_context()
            self._page = context.new_page()

        if "xueqiu.com" not in str(self._page.url or ""):
            self._page.goto("https://xueqiu.com/", wait_until="domcontentloaded", timeout=15_000)

    def get_stock_posts(self, symbol: str, *, sort: str = "reply", page: int = 1, count: int = 20) -> list[dict[str, Any]]:
        result = self._page.evaluate(
            """async ({symbol, sort, page, count}) => {
                const params = new URLSearchParams({
                    symbol,
                    sort,
                    source: 'all',
                    count: String(count),
                    page: String(page),
                });
                const url = `/query/v1/symbol/search/status.json?${params.toString()}`;
                const resp = await fetch(url, {credentials: 'include'});
                const ct = resp.headers.get('content-type') || '';
                const text = await resp.text();
                if (!ct.includes('json')) {
                    return {ok: false, error: 'non_json_response', text: text.slice(0, 240)};
                }
                try {
                    const payload = JSON.parse(text);
                    return {ok: true, data: payload};
                } catch (error) {
                    return {ok: false, error: String(error), text: text.slice(0, 240)};
                }
            }""",
            {"symbol": symbol, "sort": sort, "page": page, "count": count},
        )
        if not isinstance(result, dict) or not result.get("ok"):
            snippet = result.get("text") if isinstance(result, dict) else None
            raise RuntimeError(f"Xueqiu stock posts request failed: {result!r} snippet={snippet!r}")
        payload = result.get("data") or {}
        return list(payload.get("list") or [])

    def close(self) -> None:
        try:
            if self._browser is not None:
                self._browser.close()
        finally:
            if self._playwright is not None:
                self._playwright.stop()


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
    raw_cookie = _config_value("XUEQIU_COOKIE", "xueqiu_cookie").strip()
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

    async def overview(self, sources: list[str] | None = None) -> dict[str, Any]:
        requested_sources = ordered_sentiment_sources(sources)
        storage_sources = _source_storage_values(requested_sources)

        totals_stmt = select(
            func.count(SentimentPost.id),
            func.count(func.distinct(SentimentPost.symbol)),
            func.max(SentimentPost.published_at),
        )
        if storage_sources:
            totals_stmt = totals_stmt.where(SentimentPost.source.in_(storage_sources))
        total_posts, total_symbols, latest_published_at = (await self.session.execute(totals_stmt)).one()

        source_rows = []
        for source in requested_sources:
            source_storage = _source_storage_values([source]) or [source]
            source_stmt = select(
                func.count(SentimentPost.id),
                func.count(func.distinct(SentimentPost.symbol)),
                func.max(SentimentPost.published_at),
            ).where(SentimentPost.source.in_(source_storage))
            post_count, symbol_count, latest_source_published_at = (await self.session.execute(source_stmt)).one()
            source_rows.append(
                {
                    "source": source,
                    **_source_runtime_status(source),
                    "post_count": int(post_count or 0),
                    "symbol_count": int(symbol_count or 0),
                    "latest_published_at": (
                        latest_source_published_at.isoformat() if latest_source_published_at else None
                    ),
                }
            )

        return {
            "sources": source_rows,
            "total_posts": int(total_posts or 0),
            "symbol_count": int(total_symbols or 0),
            "latest_published_at": latest_published_at.isoformat() if latest_published_at else None,
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

    async def run_many(
        self,
        symbol: str | None,
        *,
        sources: list[str] | None = None,
        max_pages: int = 3,
        min_reply: int = 20,
        start_date: date | None = None,
        end_date: date | None = None,
        force_refresh: bool = False,
    ) -> dict[str, Any]:
        normalized_symbol = normalize_sentiment_symbol(symbol) if symbol else None
        requested_sources = ordered_sentiment_sources(sources)

        results: list[dict[str, Any]] = []
        succeeded_sources: list[str] = []
        failed_sources: list[str] = []
        total_upserted = 0
        total_collected = 0
        total_matched = 0

        for source in requested_sources:
            try:
                result = await self.run(
                    source,
                    normalized_symbol,
                    max_pages=max_pages,
                    min_reply=min_reply,
                    start_date=start_date,
                    end_date=end_date,
                    force_refresh=force_refresh,
                )
                results.append({"ok": True, **result})
                succeeded_sources.append(source)
                total_upserted += int(result.get("upserted") or 0)
                total_collected += int(result.get("collected") or 0)
                total_matched += int(result.get("matched") or 0)
            except Exception as exc:
                failed_sources.append(source)
                results.append(
                    {
                        "ok": False,
                        "source": source,
                        "symbol": normalized_symbol,
                        "error": str(exc),
                    }
                )

        return {
            "symbol": normalized_symbol,
            "requested_sources": requested_sources,
            "succeeded_sources": succeeded_sources,
            "failed_sources": failed_sources,
            "all_succeeded": len(failed_sources) == 0,
            "total_upserted": total_upserted,
            "total_collected": total_collected,
            "total_matched": total_matched,
            "results": results,
        }

    async def run(
        self,
        source: str,
        symbol: str | None,
        max_pages: int = 3,
        min_reply: int = 20,
        start_date: date | None = None,
        end_date: date | None = None,
        force_refresh: bool = False,
    ) -> dict[str, Any]:
        source = normalize_sentiment_source(source)
        symbol = normalize_sentiment_symbol(symbol) if symbol else None
        if source == "xueqiu_spyder":
            if not symbol:
                raise ValueError("xueqiu_spyder ingest requires a symbol")
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
        crawler = _BuiltinXueqiuCrawler()
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
                post = normalize_xueqiu_spyder_post(raw, symbol=symbol, strip_html=_strip_html_fallback)
                if post is None:
                    continue
                if post.reply_count < min_reply:
                    continue
                if not _in_date_window(post.published_at, start_date, end_date):
                    continue
                normalized.append(post)
            return [post for post in normalized if post.source_post_id], {"auth": auth}
        finally:
            crawler.close()

    def _collect_flocktrader(self, symbol: str, max_pages: int) -> list[SentimentPostInput]:
        project_dir = Path(_config_value("FLOCKTRADER_DIR", "flocktrader_dir", r"E:\Projects\flocktrader"))
        if not project_dir.exists():
            raise RuntimeError(f"flocktrader directory not found: {project_dir}")
        with _temporary_project_imports(project_dir, ["config", "models", "crawler", "analyzer"]):
            from analyzer import KeywordExtractor, SentimentAnalyzer
            from crawler import NGACrawler

            sentiment_analyzer = SentimentAnalyzer()
            keyword_extractor = KeywordExtractor()
            crawler = NGACrawler(cookies=_config_value("NGA_COOKIE", "nga_cookie"))
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
        symbol: str | None,
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

        aliases: list[str] | None = None
        if symbol:
            stock = await self.session.get(Stock, symbol)
            aliases = _symbol_aliases_from_parts(symbol, stock)
        stock_aliases = await self._load_stock_aliases() if not symbol else None
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
            if symbol:
                if not _post_mentions_symbol(raw, symbol, aliases):
                    continue
                normalized.extend(normalize_nga_data_posts(raw, symbol=symbol, aliases=aliases))
                continue
            normalized.extend(normalize_nga_data_posts(raw, stock_aliases=stock_aliases))
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
        cookie = _config_value("NGA_COOKIE", "nga_cookie")
        if not cookie:
            raise RuntimeError(
                "NGA_COOKIE is required to crawl missing NGAdata files; "
                "set it in the environment or pre-populate the shared sentiment data directory"
            )
        start_dt = datetime.combine(day, time.min)
        end_dt = datetime.combine(day, time.max.replace(microsecond=0))
        board_fid = int(getattr(settings, "nga_board_fid", 706) or 706)
        collected: list[dict[str, Any]] = []
        seen_tids: set[str] = set()
        for page in range(1, max_pages + 1):
            board_html = _fetch_nga_html("https://nga.178.com/thread.php", params={"fid": board_fid, "page": page})
            topics = _parse_nga_board_topics(board_html)
            if not topics:
                break
            for topic in topics:
                tid = str(topic.get("tid") or "")
                if not tid or tid in seen_tids:
                    continue
                seen_tids.add(tid)
                detail_html = _fetch_nga_html("https://nga.178.com/read.php", params={"tid": tid, "page": "e"})
                thread_posts = _parse_nga_thread_posts(detail_html)
                if not thread_posts:
                    continue
                publish_time = _parse_datetime(thread_posts[0].get("publish_time")) or topic.get("publish_time")
                last_reply_time = _parse_datetime(thread_posts[-1].get("publish_time")) or publish_time
                if last_reply_time is None:
                    continue
                if last_reply_time < start_dt or last_reply_time > end_dt:
                    continue
                title = thread_posts[0].get("title") or topic.get("title") or ""
                content = thread_posts[0].get("content") or title
                comments = [
                    {"content": post.get("content") or "", "publish_time": post.get("publish_time")}
                    for post in thread_posts[1:]
                    if str(post.get("content") or "").strip()
                ]
                stock_codes = _extract_nga_post_symbols({"title": title, "content": content, "comments": comments})
                sentiment_score, sentiment_label = _nga_sentiment(" ".join([title, content, *[str(item.get('content') or '') for item in comments]]))
                collected.append(
                    {
                        "tid": tid,
                        "title": title,
                        "author": topic.get("author"),
                        "content": content,
                        "publish_time": publish_time.isoformat() if isinstance(publish_time, datetime) else publish_time,
                        "last_reply_time": last_reply_time.isoformat() if isinstance(last_reply_time, datetime) else last_reply_time,
                        "reply_count": topic.get("reply_count", len(comments)),
                        "comments": comments,
                        "stock_codes": stock_codes,
                        "keywords": _nga_keywords(" ".join([title, content]), stock_codes),
                        "sentiment_score": sentiment_score,
                        "sentiment_label": sentiment_label,
                    }
                )
            if all((_parse_datetime(item.get("publish_time")) or start_dt) < start_dt for item in topics):
                break
        return collected

    def _analyze_nga_posts(self, raw_posts: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not raw_posts:
            return []
        analyzed: list[dict[str, Any]] = []
        for raw in raw_posts:
            text = " ".join(_text_parts(raw))
            stock_codes = _extract_nga_post_symbols(raw)
            score, label = _nga_sentiment(text)
            enriched = dict(raw)
            enriched["stock_codes"] = stock_codes
            enriched["keywords"] = _nga_keywords(text, stock_codes)
            enriched["sentiment_score"] = score
            enriched["sentiment_label"] = label
            analyzed.append(enriched)
        return analyzed

    async def _load_stock_aliases(self) -> list[tuple[str, str]]:
        result = await self.session.execute(select(Stock.symbol, Stock.name, Stock.company_name))
        aliases: list[tuple[str, str]] = []
        seen: set[tuple[str, str]] = set()
        for symbol, name, company_name in result.all():
            normalized_symbol = normalize_sentiment_symbol(symbol)
            if not normalized_symbol:
                continue
            for alias in {str(name or "").strip(), str(company_name or "").strip(), str(_short_company_name(company_name) or "").strip()}:
                if len(alias) < 3:
                    continue
                key = (alias, normalized_symbol)
                if key in seen:
                    continue
                seen.add(key)
                aliases.append(key)
        aliases.sort(key=lambda item: len(item[0]), reverse=True)
        return aliases


def _to_xueqiu_symbol(symbol: str) -> str:
    code, exchange = symbol.split(".")
    return f"{exchange}{code}"


def _infer_market_from_code(code: str) -> str:
    text = str(code or "").strip().upper()
    if not text:
        return ""
    if text.startswith(("SH", "SZ", "BJ")):
        return ""
    if text.startswith(("6", "9", "5")):
        return "SH"
    if text.startswith(("0", "2", "3")):
        return "SZ"
    if text.startswith(("4", "8")):
        return "BJ"
    return ""


def _extract_nga_post_symbols(
    raw: dict[str, Any],
    stock_aliases: list[tuple[str, str]] | None = None,
) -> list[str]:
    candidates: list[str] = []
    raw_codes = raw.get("stock_codes") or []
    if isinstance(raw_codes, list):
        candidates.extend(str(item).strip() for item in raw_codes if str(item).strip())
    text = " ".join(_text_parts(raw))
    candidates.extend(re.findall(r"\b(?:SH|SZ|BJ)\d{6}\b", text.upper()))

    normalized: list[str] = []
    for candidate in candidates:
        symbol = normalize_security_symbol(candidate, _infer_market_from_code(candidate))
        if symbol and symbol not in normalized and "." in symbol:
            normalized.append(symbol)

    if stock_aliases:
        text_upper = text.upper()
        for alias, symbol in stock_aliases:
            alias_text = str(alias or "").strip()
            if len(alias_text) < 3:
                continue
            if alias_text.upper() in text_upper and symbol not in normalized:
                normalized.append(symbol)
    return normalized


def _nga_headers() -> dict[str, str]:
    cookie = _config_value("NGA_COOKIE", "nga_cookie")
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": f"https://nga.178.com/thread.php?fid={int(getattr(settings, 'nga_board_fid', 706) or 706)}",
    }
    if cookie:
        headers["Cookie"] = cookie
    return headers


def _decode_nga_response(content: bytes) -> str:
    return content.decode("gbk", errors="ignore")


def _nga_clean_text(value: str | None) -> str:
    text = str(value or "")
    text = text.replace("<br/>", "\n").replace("<br>", "\n")
    text = re.sub(r"\[img\].*?\[/img\]", " ", text, flags=re.I | re.S)
    text = re.sub(r"\[[^\]]+\]", " ", text)
    text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _fetch_nga_html(url: str, *, params: dict[str, Any] | None = None) -> str:
    response = requests.get(url, headers=_nga_headers(), params=params, timeout=20)
    response.raise_for_status()
    return _decode_nga_response(response.content)


def _parse_nga_board_topics(html_text: str) -> list[dict[str, Any]]:
    pattern = re.compile(
        r"<tr class='row\d+ topicrow'>.*?"
        r"href='/read\.php\?tid=(?P<tid>\d+)'.*?class='replies'>(?P<replies>\d+)</a>.*?"
        r"<a href='/read\.php\?tid=(?P=tid)'[^>]*class='topic'>(?P<title>.*?)</a>.*?"
        r"title='用户ID (?P<author_id>\d+)'>(?P<author>.*?)</a>"
        r"<span class='silver postdate'[^>]*>(?P<postdate>\d+)</span>",
        re.S,
    )
    topics: list[dict[str, Any]] = []
    for match in pattern.finditer(html_text):
        title = _nga_clean_text(match.group("title"))
        if not title:
            continue
        post_ts = int(match.group("postdate"))
        topics.append(
            {
                "tid": match.group("tid"),
                "title": title,
                "author": _nga_clean_text(match.group("author")) or match.group("author_id"),
                "author_id": match.group("author_id"),
                "reply_count": _to_int(match.group("replies")),
                "publish_time": datetime.fromtimestamp(post_ts),
            }
        )
    return topics


def _parse_nga_thread_posts(html_text: str) -> list[dict[str, Any]]:
    pattern = re.compile(
        r"href='nuke\.php\?func=ucp&uid=(?P<author_id>\d+)'[^>]*id='postauthor(?P<idx>\d+)'[^>]*>.*?</a>.*?"
        r"<span id='postdate(?P=idx)'[^>]*>(?P<publish_time>[^<]+)</span>.*?"
        r"<span id='postcontentandsubject(?P=idx)'>.*?"
        r"<h3 id='postsubject(?P=idx)'>(?P<title>.*?)</h3><br/>.*?"
        r"<(?:p|span) id='postcontent(?P=idx)' class='postcontent ubbcode'>(?P<content>.*?)</(?:p|span)>",
        re.S,
    )
    posts: list[dict[str, Any]] = []
    for match in pattern.finditer(html_text):
        posts.append(
            {
                "author": match.group("author_id"),
                "publish_time": match.group("publish_time"),
                "title": _nga_clean_text(match.group("title")),
                "content": _nga_clean_text(match.group("content")),
            }
        )
    return posts


def _nga_sentiment(text: str) -> tuple[float | None, str | None]:
    normalized = str(text or "")
    if not normalized:
        return None, None
    bullish_terms = ("看多", "买入", "加仓", "看涨", "利好", "反弹", "突破", "新高")
    bearish_terms = ("看空", "卖出", "减仓", "利空", "下跌", "回调", "暴雷", "新低")
    bullish = sum(normalized.count(term) for term in bullish_terms)
    bearish = sum(normalized.count(term) for term in bearish_terms)
    total = bullish + bearish
    if total == 0:
        return None, None
    score = bullish / total
    return score, _normalize_sentiment_label(None, score)


def _nga_keywords(text: str, stock_codes: list[str]) -> list[str]:
    candidates = ["业绩", "估值", "涨停", "回调", "反弹", "芯片", "白酒", "银行", "存储", "科技", "军工"]
    keywords = [term for term in candidates if term in text]
    for code in stock_codes:
        if code not in keywords:
            keywords.append(code)
    return keywords[:12]


def _source_runtime_status(source: str) -> dict[str, Any]:
    source = normalize_sentiment_source(source)
    if source == "xueqiu_spyder":
        try:
            from playwright.sync_api import sync_playwright as _unused  # noqa: F401
            playwright_ready = True
        except ImportError:
            playwright_ready = False
        try:
            chrome_path = _resolve_chrome_path(_config_value("XUEQIU_CHROME_PATH", "xueqiu_chrome_path"))
            chrome_ready = Path(chrome_path).exists()
        except Exception:
            chrome_path = _config_value("XUEQIU_CHROME_PATH", "xueqiu_chrome_path")
            chrome_ready = False
        cookie_configured = bool(_config_value("XUEQIU_COOKIE", "xueqiu_cookie").strip())
        project_dir = _xueqiu_project_dir()
        profile_dir = _xueqiu_profile_dir()
        project_ready = project_dir.exists() or (playwright_ready and chrome_ready)
        return {
            "label": "Xueqiu",
            "project_dir": str(project_dir),
            "project_ready": project_ready,
            "cookie_configured": cookie_configured,
            "cache_dir": str(profile_dir),
            "cache_file_count": 0,
            "ready": project_ready and cookie_configured,
            "chrome_path": chrome_path,
        }

    if source == "flocktrader":
        project_dir = _flocktrader_project_dir()
        cache_dir = _nga_data_dir()
        cache_file_count = len(list(cache_dir.glob("posts_*.json"))) if cache_dir.exists() else 0
        cookie_configured = bool(_config_value("NGA_COOKIE", "nga_cookie").strip())
        project_ready = project_dir.exists()
        return {
            "label": "NGA",
            "project_dir": str(project_dir),
            "project_ready": project_ready,
            "cookie_configured": cookie_configured,
            "cache_dir": str(cache_dir),
            "cache_file_count": cache_file_count,
            "ready": project_ready and (cookie_configured or cache_file_count > 0),
        }

    cache_dir = _nga_data_dir()
    cache_file_count = len(list(cache_dir.glob("posts_*.json"))) if cache_dir.exists() else 0
    cookie_configured = bool(_config_value("NGA_COOKIE", "nga_cookie").strip())
    return {
        "label": "NGA",
        "project_dir": f"https://nga.178.com/thread.php?fid={int(getattr(settings, 'nga_board_fid', 706) or 706)}",
        "project_ready": True,
        "cookie_configured": cookie_configured,
        "cache_dir": str(cache_dir),
        "cache_file_count": cache_file_count,
        "ready": cookie_configured or cache_file_count > 0,
    }


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


def normalize_nga_data_posts(
    raw: dict[str, Any],
    *,
    symbol: str | None = None,
    aliases: list[str] | None = None,
    stock_aliases: list[tuple[str, str]] | None = None,
) -> list[SentimentPostInput]:
    target_symbols = [normalize_sentiment_symbol(symbol)] if symbol else _extract_nga_post_symbols(raw, stock_aliases=stock_aliases)
    normalized: list[SentimentPostInput] = []
    for target_symbol in target_symbols:
        post = normalize_nga_data_post(raw, symbol=target_symbol, aliases=aliases if symbol else None)
        if post is not None:
            normalized.append(post)
    return normalized
