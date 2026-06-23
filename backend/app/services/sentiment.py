from __future__ import annotations

import asyncio
import hashlib
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
from typing import Any, Callable, Iterable
from urllib.parse import urlencode, urljoin

import requests
from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.db.models.sentiment import SentimentPost, SentimentThread
from app.db.models.stock import Stock
from app.services.security_symbols import normalize_security_symbol

DEFAULT_SOURCE_ORDER = ("xueqiu_spyder", "eastmoney_guba", "jisilu", "wechat_sogou", "flocktrader")
CANONICAL_SOURCES = set(DEFAULT_SOURCE_ORDER)
SOURCE_ALIASES = {
    "xueqiu": "xueqiu_spyder",
    "xueqiu-spyder": "xueqiu_spyder",
    "xueqiu_spyder": "xueqiu_spyder",
    "eastmoney": "eastmoney_guba",
    "eastmoney-guba": "eastmoney_guba",
    "eastmoney_guba": "eastmoney_guba",
    "dfcfw": "eastmoney_guba",
    "guba": "eastmoney_guba",
    "股吧": "eastmoney_guba",
    "jisilu": "jisilu",
    "jsl": "jisilu",
    "集思录": "jisilu",
    "wechat": "wechat_sogou",
    "weixin": "wechat_sogou",
    "wechat-sogou": "wechat_sogou",
    "wechat_sogou": "wechat_sogou",
    "sogou_wechat": "wechat_sogou",
    "weixin_sogou": "wechat_sogou",
    "公众号": "wechat_sogou",
    "微信": "wechat_sogou",
    "搜狗微信": "wechat_sogou",
    "flocktrader": "flocktrader",
    "flock-trader": "flocktrader",
    "nga": "flocktrader",
}
LEGACY_SOURCE_NAMES = {
    "xueqiu_spyder": {"xueqiu", "xueqiu_spyder"},
    "eastmoney_guba": {"eastmoney_guba"},
    "jisilu": {"jisilu"},
    "wechat_sogou": {"wechat_sogou"},
    "flocktrader": {"nga", "flocktrader"},
}
SUPPORTED_SOURCES = set(SOURCE_ALIASES)
SentimentProgressCallback = Callable[[dict[str, Any]], None]
DEFAULT_WECHAT_SOGOU_QUERIES = (
    "开盘啦 创始人 股票",
    "开盘啦 A股 股票",
    "龙虎榜 A股 游资",
    "短线 A股 股票",
    "涨停板 A股",
)


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
class SentimentThreadInput:
    source: str
    source_thread_id: str
    title: str | None = None
    content: str | None = None
    author: str | None = None
    published_at: datetime | None = None
    last_reply_at: datetime | None = None
    url: str | None = None
    reply_count: int = 0
    comment_count: int = 0
    sentiment_score: float | None = None
    sentiment_label: str | None = None
    symbols: list[str] = field(default_factory=list)
    keywords: list[str] = field(default_factory=list)
    raw: dict[str, Any] | None = None


@dataclass
class NgaIngestStats:
    loaded_dates: list[str] = field(default_factory=list)
    crawled_dates: list[str] = field(default_factory=list)
    date_files: list[str] = field(default_factory=list)
    extra_date_files: list[str] = field(default_factory=list)
    empty_dates: list[str] = field(default_factory=list)
    scan_time_basis: str = "last_reply_time"
    cache_partition: str = "publish_time"
    total_posts: int = 0
    analyzed_posts: int = 0
    matched_posts: int = 0
    thread_upserted: int = 0


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


def _nga_post_key(post: dict[str, Any]) -> str:
    tid = str(post.get("tid") or "").strip()
    if tid:
        return f"tid:{tid}"
    title = str(post.get("title") or "").strip()
    publish_time = str(post.get("publish_time") or "").strip()
    return f"fallback:{title}:{publish_time}"


def _merge_nga_post_lists(*groups: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    order: list[str] = []
    for group in groups:
        for post in group:
            if not isinstance(post, dict):
                continue
            key = _nga_post_key(post)
            if key not in merged:
                order.append(key)
            merged[key] = post
    return [merged[key] for key in order]


def _nga_publish_day(post: dict[str, Any]) -> date | None:
    publish_time = _parse_datetime(post.get("publish_time"))
    if publish_time is not None:
        return publish_time.date()
    fallback_time = _parse_datetime(post.get("last_reply_time"))
    return fallback_time.date() if fallback_time is not None else None


def _bucket_nga_posts_by_publish_day(posts: list[dict[str, Any]]) -> dict[date, list[dict[str, Any]]]:
    buckets: dict[date, list[dict[str, Any]]] = {}
    for post in posts:
        publish_day = _nga_publish_day(post)
        if publish_day is None:
            continue
        buckets.setdefault(publish_day, []).append(post)
    return buckets


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


def _eastmoney_guba_url(code: str, page: int, page_size: int = 20) -> str:
    return (
        "https://gbapi.eastmoney.com/webarticlelist/api/Article/Articlelist"
        f"?code={code}&sorttype=1&page={page}&ps={page_size}"
        "&deviceid=web&version=300&product=Guba&plat=web"
    )


def _eastmoney_headers(code: str) -> dict[str, str]:
    return {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json,text/plain,*/*",
        "Referer": f"https://guba.eastmoney.com/list,{code}.html",
    }


def _fetch_eastmoney_guba_page(code: str, page: int, page_size: int = 20) -> dict[str, Any]:
    response = requests.get(
        _eastmoney_guba_url(code, page, page_size),
        headers=_eastmoney_headers(code),
        timeout=20,
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError("Eastmoney guba API returned non-object payload")
    return payload


def _fetch_eastmoney_hot_bars(limit: int = 30) -> list[dict[str, str]]:
    response = requests.get(
        "https://guba.eastmoney.com/remenba.aspx",
        headers={
            "User-Agent": "Mozilla/5.0",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Referer": "https://guba.eastmoney.com/",
        },
        timeout=20,
    )
    response.raise_for_status()
    response.encoding = response.encoding or "utf-8"
    bars: list[dict[str, str]] = []
    seen: set[str] = set()
    for match in re.finditer(
        r'href="(?:https?://guba\.eastmoney\.com/)?list,(?P<code>\d{6})\.html"[^>]*>\((?P=code)\)(?P<name>.*?)</a>',
        response.text,
        re.S,
    ):
        code = match.group("code")
        if code in seen:
            continue
        seen.add(code)
        bars.append({"code": code, "name": _nga_clean_text(match.group("name"))})
        if len(bars) >= limit:
            break
    return bars


def _wechat_sogou_query_values() -> list[str]:
    raw = _config_value("WECHAT_SOGOU_QUERIES", "wechat_sogou_queries", "")
    values = re.split(r"[,;，；\n]+", raw) if raw else list(DEFAULT_WECHAT_SOGOU_QUERIES)
    normalized: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if text and text not in normalized:
            normalized.append(text)
    return normalized


def _wechat_sogou_queries(symbol: str | None = None, stock: Stock | None = None) -> list[str]:
    configured = _wechat_sogou_query_values()
    if not symbol:
        return configured
    normalized = normalize_sentiment_symbol(symbol)
    code = normalized.split(".", 1)[0]
    stock_terms = [code]
    if stock is not None:
        stock_terms.extend(
            [
                str(stock.name or "").strip(),
                str(stock.company_name or "").strip(),
                str(_short_company_name(stock.company_name) or "").strip(),
            ]
        )
    queries: list[str] = []
    for term in stock_terms:
        if not term:
            continue
        for suffix in ("股票", "A股", "股价"):
            query = f"{term} {suffix}".strip()
            if query not in queries:
                queries.append(query)
    for query in configured:
        if query not in queries:
            queries.append(query)
    return queries


def _wechat_sogou_url(query: str, page: int = 1) -> str:
    params = {"type": "2", "query": query, "ie": "utf8"}
    if page > 1:
        params["page"] = str(page)
    return "https://weixin.sogou.com/weixin?" + urlencode(params)


def _wechat_sogou_headers(query: str) -> dict[str, str]:
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/125 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": "https://weixin.sogou.com/",
    }
    cookie = _config_value("WECHAT_SOGOU_COOKIE", "wechat_sogou_cookie")
    if cookie:
        headers["Cookie"] = cookie
    return headers


def _fetch_wechat_sogou_page(query: str, page: int = 1) -> str:
    response = requests.get(
        _wechat_sogou_url(query, page),
        headers=_wechat_sogou_headers(query),
        timeout=20,
    )
    response.raise_for_status()
    response.encoding = response.encoding or "utf-8"
    text = response.text
    if "请输入验证码" in text or "antispider" in response.url.lower():
        raise RuntimeError("Sogou WeChat returned a verification page; configure WECHAT_SOGOU_COOKIE or retry later")
    return text


def _wechat_clean_text(value: str | None) -> str:
    text = str(value or "")
    text = re.sub(r"<!--.*?-->", " ", text, flags=re.S)
    text = re.sub(r"<script\b.*?</script>", " ", text, flags=re.I | re.S)
    text = re.sub(r"<style\b.*?</style>", " ", text, flags=re.I | re.S)
    text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text)
    return re.sub(r"\s+", " ", text).strip()


def _wechat_article_id(link: str, title: str, account: str, published_at: datetime | None) -> str:
    source = "|".join([link, title, account, published_at.isoformat() if published_at else ""])
    return hashlib.sha1(source.encode("utf-8", errors="ignore")).hexdigest()[:24]


def _parse_wechat_sogou_articles(html_text: str, *, query: str, page: int = 1) -> list[dict[str, Any]]:
    articles: list[dict[str, Any]] = []
    for match in re.finditer(r"<li\b(?P<attrs>[^>]*)>(?P<body>.*?)</li>", html_text, flags=re.I | re.S):
        attrs = match.group("attrs") or ""
        body = match.group("body") or ""
        if "txt-box" not in body or "sogou_vr_11002601" not in attrs + body:
            continue
        title_match = re.search(r"<h3[^>]*>.*?<a\b(?P<attrs>[^>]*)>(?P<title>.*?)</a>", body, flags=re.I | re.S)
        if not title_match:
            continue
        href_match = re.search(r"href=[\"'](?P<href>.*?)[\"']", title_match.group("attrs") or "", flags=re.I | re.S)
        raw_link = html.unescape(href_match.group("href")) if href_match else ""
        if raw_link.startswith("//"):
            link = "https:" + raw_link
        elif raw_link:
            link = urljoin("https://weixin.sogou.com/", raw_link)
        else:
            link = ""
        title = _wechat_clean_text(title_match.group("title"))
        summary_match = re.search(
            r"<p\b[^>]*class=[\"'][^\"']*txt-info[^\"']*[\"'][^>]*>(?P<summary>.*?)</p>",
            body,
            flags=re.I | re.S,
        )
        summary = _wechat_clean_text(summary_match.group("summary")) if summary_match else ""
        account_match = re.search(
            r"<span\b[^>]*class=[\"'][^\"']*all-time-y2[^\"']*[\"'][^>]*>(?P<account>.*?)</span>",
            body,
            flags=re.I | re.S,
        )
        account = _wechat_clean_text(account_match.group("account")) if account_match else ""
        ts_match = re.search(r"timeConvert\(['\"]?(?P<ts>\d{10})['\"]?\)", body)
        published_at = _parse_datetime(int(ts_match.group("ts"))) if ts_match else None
        d_match = re.search(r"\bd=[\"'](?P<d>[^\"']+)[\"']", attrs)
        source_thread_id = str(d_match.group("d")).strip() if d_match else _wechat_article_id(link, title, account, published_at)
        if not title and not summary:
            continue
        articles.append(
            {
                "source_thread_id": source_thread_id,
                "title": title,
                "content": summary,
                "account": account,
                "author": account,
                "published_at": published_at.isoformat() if published_at else None,
                "url": link,
                "query": query,
                "page": page,
            }
        )
    return articles


def _jisilu_category_url(page: int) -> str:
    if page <= 1:
        return "https://www.jisilu.cn/category/8"
    return f"https://www.jisilu.cn/home/explore/sort_type-new__category-8__day-0__page-{page}"


def _jisilu_headers() -> dict[str, str]:
    return {
        "User-Agent": "Mozilla/5.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": "https://www.jisilu.cn/category/8",
    }


def _fetch_jisilu_html(url: str) -> str:
    response = requests.get(url, headers=_jisilu_headers(), timeout=20)
    response.raise_for_status()
    response.encoding = response.encoding or "utf-8"
    return response.text


def _parse_jisilu_list_posts(html_text: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for chunk in html_text.split('<div class="aw-item">')[1:]:
        if "aw-question-replay-count" not in chunk or "aw-questoin-content" not in chunk:
            continue
        reply_match = re.search(r'<span class="aw-question-replay-count[^"]*">\s*<em>(?P<count>\d+)</em>', chunk, re.S)
        title_match = re.search(
            r'<h4>\s*<a[^>]+href="(?P<url>https://www\.jisilu\.cn/question/(?P<qid>\d+))"[^>]*>(?P<title>.*?)</a>',
            chunk,
            re.S,
        )
        if title_match is None:
            continue
        author_match = re.search(
            r'<a[^>]+class="aw-user-name"[^>]*>(?P<author>.*?)</a>.*?(?P<active_time>\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}).*?(?P<view_count>\d+)\s*次浏览',
            chunk,
            re.S,
        )
        meta_text = _nga_clean_text(author_match.group(0)) if author_match else _nga_clean_text(chunk)
        rows.append(
            {
                "question_id": title_match.group("qid"),
                "title": _nga_clean_text(title_match.group("title")),
                "url": title_match.group("url"),
                "reply_count": _to_int(reply_match.group("count") if reply_match else 0),
                "author": _nga_clean_text(author_match.group("author")) if author_match else None,
                "active_time": author_match.group("active_time") if author_match else None,
                "view_count": _to_int(author_match.group("view_count") if author_match else 0),
                "meta_text": meta_text,
            }
        )
    return rows


def _parse_jisilu_detail(html_text: str) -> dict[str, Any]:
    title_match = re.search(r"<h1>(?P<title>.*?)</h1>", html_text, re.S)
    content_match = re.search(
        r'<div class="aw-question-detail-txt markitup-box">(?P<content>.*?)</div>',
        html_text,
        re.S,
    )
    published_match = re.search(r"发表时间\s*(?P<published>\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2})", html_text)
    comments: list[dict[str, Any]] = []
    for answer in re.finditer(
        r'<div class="markitup-box"\s*>(?P<content>.*?)</div>.*?'
        r'<span class="pull-left aw-text-color-999">(?P<publish_time>\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2})',
        html_text,
        re.S,
    ):
        content = _nga_clean_text(answer.group("content"))
        if not content:
            continue
        comments.append(
            {
                "content": content,
                "publish_time": answer.group("publish_time"),
            }
        )
        if len(comments) >= 20:
            break
    return {
        "detail_title": _nga_clean_text(title_match.group("title")) if title_match else None,
        "content": _nga_clean_text(content_match.group("content")) if content_match else None,
        "question_published_time": published_match.group("published") if published_match else None,
        "comments": comments,
    }


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

    async def upsert_threads(self, threads: list[SentimentThreadInput]) -> int:
        count = 0
        for thread in threads:
            thread.source = normalize_sentiment_source(thread.source)
            if not thread.source_thread_id:
                raise ValueError("source_thread_id is required")
            symbols = [
                normalize_sentiment_symbol(symbol)
                for symbol in thread.symbols
                if normalize_security_symbol(symbol)
            ]
            symbols = list(dict.fromkeys(symbols))
            label = _normalize_sentiment_label(thread.sentiment_label, thread.sentiment_score)
            storage_sources = _source_storage_values([thread.source]) or [thread.source]
            stmt = select(SentimentThread).where(
                SentimentThread.source.in_(storage_sources),
                SentimentThread.source_thread_id == str(thread.source_thread_id),
            )
            existing_rows = list((await self.session.execute(stmt)).scalars().all())
            existing = next((row for row in existing_rows if row.source == thread.source), None)
            existing = existing or (existing_rows[0] if existing_rows else None)
            for duplicate in existing_rows:
                if existing is not None and duplicate.id != existing.id:
                    await self.session.delete(duplicate)
            target = existing or SentimentThread(
                source=thread.source,
                source_thread_id=str(thread.source_thread_id),
            )
            target.source = thread.source
            target.title = thread.title
            target.content = thread.content
            target.author = thread.author
            target.published_at = thread.published_at
            target.last_reply_at = thread.last_reply_at
            target.url = thread.url
            target.reply_count = max(int(thread.reply_count or 0), 0)
            target.comment_count = max(int(thread.comment_count or 0), 0)
            target.sentiment_score = thread.sentiment_score
            target.sentiment_label = label
            target.symbols_json = _json_list(symbols)
            target.keywords_json = _json_list(thread.keywords)
            target.raw_json = json.dumps(thread.raw or {}, ensure_ascii=False)
            if existing is None:
                self.session.add(target)
            count += 1
        await self.session.flush()
        return count

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

    async def list_threads(
        self,
        start_date: date | None = None,
        end_date: date | None = None,
        sources: list[str] | None = None,
        symbol: str | None = None,
        limit: int = 50,
    ) -> list[SentimentThread]:
        start, end = _date_bounds(start_date, end_date)
        thread_time = func.coalesce(SentimentThread.last_reply_at, SentimentThread.published_at)
        stmt = select(SentimentThread)
        if start is not None:
            stmt = stmt.where(thread_time >= start)
        if end is not None:
            stmt = stmt.where(thread_time < end)
        storage_sources = _source_storage_values(sources)
        if storage_sources:
            stmt = stmt.where(SentimentThread.source.in_(storage_sources))
        if symbol:
            normalized_symbol = normalize_sentiment_symbol(symbol)
            code = normalized_symbol.split(".", 1)[0]
            stmt = stmt.where(
                or_(
                    SentimentThread.symbols_json.like(f'%"{normalized_symbol}"%'),
                    SentimentThread.symbols_json.like(f'%"{code}"%'),
                )
            )
        stmt = stmt.order_by(
            SentimentThread.reply_count.desc(),
            thread_time.desc().nullslast(),
        ).limit(max(1, min(limit, 200)))
        return list((await self.session.execute(stmt)).scalars().all())

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


def serialize_thread(thread: SentimentThread) -> dict[str, Any]:
    try:
        raw = json.loads(thread.raw_json or "{}")
    except json.JSONDecodeError:
        raw = {}
    raw_comments = raw.get("comments") if isinstance(raw, dict) else None
    comments = [
        {
            "content": str(comment.get("content") or ""),
            "publish_time": comment.get("publish_time"),
        }
        for comment in (raw_comments or [])
        if isinstance(comment, dict) and str(comment.get("content") or "").strip()
    ]
    full_text_parts = [str(thread.title or ""), str(thread.content or "")]
    full_text_parts.extend(str(comment["content"]) for comment in comments)
    return {
        "id": thread.id,
        "source": normalize_sentiment_source(thread.source),
        "source_thread_id": thread.source_thread_id,
        "title": thread.title,
        "content": thread.content,
        "author": thread.author,
        "published_at": thread.published_at.isoformat() if thread.published_at else None,
        "last_reply_at": thread.last_reply_at.isoformat() if thread.last_reply_at else None,
        "url": thread.url,
        "reply_count": thread.reply_count,
        "comment_count": thread.comment_count,
        "sentiment_score": thread.sentiment_score,
        "sentiment_label": thread.sentiment_label,
        "symbols": _loads_list(thread.symbols_json),
        "keywords": _loads_list(thread.keywords_json),
        "comments": comments,
        "full_text": "\n".join(part for part in full_text_parts if part.strip()),
    }


class SentimentIngestService:
    """Best-effort wrapper around local external crawler projects."""

    def __init__(
        self,
        session: AsyncSession,
        progress_callback: SentimentProgressCallback | None = None,
    ):
        self.session = session
        self.service = SentimentService(session)
        self.progress_callback = progress_callback

    def _emit_progress(self, stage: str, **payload: Any) -> None:
        if self.progress_callback is None:
            return
        self.progress_callback(
            {
                "stage": stage,
                "source": "flocktrader",
                "updated_at": datetime.now().isoformat(),
                **payload,
            }
        )

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
        elif source == "eastmoney_guba":
            if symbol:
                posts, eastmoney_stats = await asyncio.to_thread(
                    self._collect_eastmoney_guba,
                    symbol,
                    max_pages,
                    min_reply,
                    start_date,
                    end_date,
                )
                mode = "stock_bar"
                page_url = f"https://guba.eastmoney.com/list,{symbol.split('.', 1)[0]}.html"
            else:
                posts, eastmoney_stats = await asyncio.to_thread(
                    self._collect_eastmoney_guba_hot_bars,
                    max_pages,
                    min_reply,
                    start_date,
                    end_date,
                )
                mode = "hot_bars"
                page_url = "https://guba.eastmoney.com/remenba.aspx"
            stats = {
                "mode": mode,
                "collected": eastmoney_stats["collected"],
                "matched": len(posts),
                "page_url": page_url,
                **eastmoney_stats,
            }
        elif source == "jisilu":
            aliases: list[str] | None = None
            stock_aliases = await self._load_stock_aliases() if not symbol else None
            if symbol:
                stock = await self.session.get(Stock, symbol)
                aliases = _symbol_aliases_from_parts(symbol, stock)
            posts, jisilu_stats = await asyncio.to_thread(
                self._collect_jisilu,
                symbol,
                aliases,
                stock_aliases,
                max_pages,
                min_reply,
                start_date,
                end_date,
            )
            stats = {
                "mode": "topic_board",
                "collected": jisilu_stats["collected"],
                "matched": len(posts),
                "page_url": "https://www.jisilu.cn/category/8",
                **jisilu_stats,
            }
        elif source == "wechat_sogou":
            aliases = None
            stock = await self.session.get(Stock, symbol) if symbol else None
            stock_aliases = await self._load_stock_aliases() if not symbol else None
            if symbol:
                aliases = _symbol_aliases_from_parts(symbol, stock)
            queries = _wechat_sogou_queries(symbol, stock)
            posts, threads, wechat_stats = await asyncio.to_thread(
                self._collect_wechat_sogou,
                symbol,
                aliases,
                stock_aliases,
                queries,
                max_pages,
                start_date,
                end_date,
            )
            threads_upserted = await self.service.upsert_threads(threads)
            stats = {
                "mode": "public_account_search",
                "collected": wechat_stats["collected"],
                "matched": len(posts),
                "threads_upserted": threads_upserted,
                "page_url": "https://weixin.sogou.com/weixin?type=2",
                **wechat_stats,
            }
        elif source == "flocktrader":
            posts, threads, nga_stats = await self._collect_flocktrader_by_date(
                symbol,
                max_pages=max_pages,
                start_date=start_date,
                end_date=end_date,
                force_refresh=force_refresh,
            )
            nga_stats.thread_upserted = await self.service.upsert_threads(threads)
            stats = {
                "mode": "daily_cache",
                "collected": nga_stats.total_posts,
                "analyzed": nga_stats.analyzed_posts,
                "matched": nga_stats.matched_posts,
                "threads_upserted": nga_stats.thread_upserted,
                "loaded_dates": nga_stats.loaded_dates,
                "crawled_dates": nga_stats.crawled_dates,
                "date_files": nga_stats.date_files,
                "extra_date_files": nga_stats.extra_date_files,
                "empty_dates": nga_stats.empty_dates,
                "scan_time_basis": nga_stats.scan_time_basis,
                "cache_partition": nga_stats.cache_partition,
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

    def _collect_eastmoney_guba(
        self,
        symbol: str,
        max_pages: int,
        min_reply: int,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> tuple[list[SentimentPostInput], dict[str, Any]]:
        code = normalize_sentiment_symbol(symbol).split(".", 1)[0]
        normalized: list[SentimentPostInput] = []
        raw_count = 0
        page_rows: list[dict[str, Any]] = []
        for page in range(1, max_pages + 1):
            self._emit_progress(
                "eastmoney_guba.page_fetch",
                source="eastmoney_guba",
                current_step="eastmoney_guba_page",
                current_symbol=symbol,
                current_page=page,
                page_limit=max_pages,
            )
            payload = _fetch_eastmoney_guba_page(code, page)
            rows = payload.get("re") or []
            if not isinstance(rows, list) or not rows:
                page_rows.append({"page": page, "raw_count": 0, "matched": 0})
                break
            raw_count += len(rows)
            matched_before = len(normalized)
            for raw in rows:
                if not isinstance(raw, dict):
                    continue
                post = normalize_eastmoney_guba_post(raw, symbol=symbol)
                if post is None:
                    continue
                if post.comment_count < min_reply:
                    continue
                if not _in_date_window(post.published_at, start_date, end_date):
                    continue
                normalized.append(post)
            page_rows.append({"page": page, "raw_count": len(rows), "matched": len(normalized) - matched_before})
            self._emit_progress(
                "eastmoney_guba.page_parsed",
                source="eastmoney_guba",
                current_step="eastmoney_guba_page",
                current_symbol=symbol,
                current_page=page,
                page_limit=max_pages,
                rows_on_page=len(rows),
                posts_collected=len(normalized),
            )
        return [post for post in normalized if post.source_post_id], {
            "collected": raw_count,
            "pages": page_rows,
            "bar_code": code,
        }

    def _collect_eastmoney_guba_hot_bars(
        self,
        max_pages: int,
        min_reply: int,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> tuple[list[SentimentPostInput], dict[str, Any]]:
        bar_limit = max(max_pages, 1) * 10
        self._emit_progress(
            "eastmoney_guba.hot_bars_fetch",
            source="eastmoney_guba",
            current_step="eastmoney_hot_bars",
            bar_limit=bar_limit,
        )
        bars = _fetch_eastmoney_hot_bars(bar_limit)
        normalized: list[SentimentPostInput] = []
        raw_count = 0
        bar_rows: list[dict[str, Any]] = []
        seen_posts: set[tuple[str, str]] = set()
        for bar_index, bar in enumerate(bars, start=1):
            code = bar["code"]
            symbol = normalize_security_symbol(code, _infer_market_from_code(code))
            if not symbol:
                continue
            self._emit_progress(
                "eastmoney_guba.bar_fetch",
                source="eastmoney_guba",
                current_step="eastmoney_hot_bar",
                current_symbol=symbol,
                current_page=bar_index,
                page_limit=len(bars),
                current_title=bar.get("name"),
            )
            payload = _fetch_eastmoney_guba_page(code, 1)
            rows = payload.get("re") or []
            if not isinstance(rows, list):
                rows = []
            raw_count += len(rows)
            matched_before = len(normalized)
            for raw in rows:
                if not isinstance(raw, dict):
                    continue
                post = normalize_eastmoney_guba_post(raw, symbol=symbol)
                if post is None:
                    continue
                dedupe_key = (post.symbol, post.source_post_id)
                if dedupe_key in seen_posts:
                    continue
                seen_posts.add(dedupe_key)
                if post.comment_count < min_reply:
                    continue
                if not _in_date_window(post.published_at, start_date, end_date):
                    continue
                normalized.append(post)
            bar_rows.append(
                {
                    "symbol": symbol,
                    "code": code,
                    "name": bar.get("name"),
                    "raw_count": len(rows),
                    "matched": len(normalized) - matched_before,
                }
            )
            self._emit_progress(
                "eastmoney_guba.bar_parsed",
                source="eastmoney_guba",
                current_step="eastmoney_hot_bar",
                current_symbol=symbol,
                current_page=bar_index,
                page_limit=len(bars),
                current_title=bar.get("name"),
                rows_on_page=len(rows),
                posts_collected=len(normalized),
            )
        return [post for post in normalized if post.source_post_id], {
            "collected": raw_count,
            "bar_limit": bar_limit,
            "bars": bar_rows,
        }

    def _collect_jisilu(
        self,
        symbol: str | None,
        aliases: list[str] | None,
        stock_aliases: list[tuple[str, str]] | None,
        max_pages: int,
        min_reply: int,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> tuple[list[SentimentPostInput], dict[str, Any]]:
        normalized: list[SentimentPostInput] = []
        raw_count = 0
        page_rows: list[dict[str, Any]] = []
        seen_questions: set[str] = set()
        for page in range(1, max_pages + 1):
            url = _jisilu_category_url(page)
            self._emit_progress(
                "jisilu.page_fetch",
                source="jisilu",
                current_step="jisilu_page",
                current_page=page,
                page_limit=max_pages,
                page_url=url,
            )
            rows = _parse_jisilu_list_posts(_fetch_jisilu_html(url))
            if not rows:
                page_rows.append({"page": page, "raw_count": 0, "matched": 0})
                break
            raw_count += len(rows)
            matched_before = len(normalized)
            for topic_index, row in enumerate(rows, start=1):
                question_id = str(row.get("question_id") or "").strip()
                if not question_id or question_id in seen_questions:
                    continue
                seen_questions.add(question_id)
                if _to_int(row.get("reply_count")) < min_reply:
                    continue
                active_time = _parse_datetime(row.get("active_time"))
                if active_time is not None and not _in_date_window(active_time, start_date, end_date):
                    continue
                self._emit_progress(
                    "jisilu.thread.fetch",
                    source="jisilu",
                    current_step="thread_detail",
                    current_page=page,
                    page_limit=max_pages,
                    topic_index=topic_index,
                    topics_on_page=len(rows),
                    current_tid=question_id,
                    current_title=str(row.get("title") or "")[:120],
                    posts_collected=len(normalized),
                )
                detail = _parse_jisilu_detail(_fetch_jisilu_html(str(row.get("url"))))
                raw = {**row, **detail}
                raw["published_time"] = raw.get("question_published_time") or raw.get("active_time")
                text = _post_text(raw)
                stock_codes = _extract_forum_post_symbols(raw, stock_aliases=stock_aliases)
                raw["stock_codes"] = stock_codes
                raw["keywords"] = _nga_keywords(text, stock_codes)
                score, label = _nga_sentiment(text)
                raw["sentiment_score"] = score
                raw["sentiment_label"] = label
                if symbol:
                    if not _post_mentions_symbol(raw, symbol, aliases):
                        continue
                    normalized.extend(normalize_jisilu_data_posts(raw, symbol=symbol, aliases=aliases))
                else:
                    normalized.extend(normalize_jisilu_data_posts(raw, stock_aliases=stock_aliases))
                self._emit_progress(
                    "jisilu.thread.collected",
                    source="jisilu",
                    current_step="thread_detail",
                    current_page=page,
                    page_limit=max_pages,
                    topic_index=topic_index,
                    topics_on_page=len(rows),
                    current_tid=question_id,
                    current_title=str(raw.get("title") or "")[:120],
                    reply_count=_to_int(raw.get("reply_count")),
                    comments_count=len(raw.get("comments") or []),
                    posts_collected=len(normalized),
                )
            page_rows.append({"page": page, "raw_count": len(rows), "matched": len(normalized) - matched_before})
            self._emit_progress(
                "jisilu.page_parsed",
                source="jisilu",
                current_step="jisilu_page",
                current_page=page,
                page_limit=max_pages,
                rows_on_page=len(rows),
                posts_collected=len(normalized),
            )
        return [post for post in normalized if post.source_post_id], {
            "collected": raw_count,
            "pages": page_rows,
        }

    def _collect_wechat_sogou(
        self,
        symbol: str | None,
        aliases: list[str] | None,
        stock_aliases: list[tuple[str, str]] | None,
        queries: list[str],
        max_pages: int,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> tuple[list[SentimentPostInput], list[SentimentThreadInput], dict[str, Any]]:
        normalized: list[SentimentPostInput] = []
        threads: list[SentimentThreadInput] = []
        raw_count = 0
        query_rows: list[dict[str, Any]] = []
        seen_articles: set[str] = set()
        for query_index, query in enumerate(queries, start=1):
            query_raw_count = 0
            query_matched_before = len(normalized)
            for page in range(1, max_pages + 1):
                self._emit_progress(
                    "wechat_sogou.page_fetch",
                    source="wechat_sogou",
                    current_step="wechat_sogou_page",
                    current_page=page,
                    page_limit=max_pages,
                    query_index=query_index,
                    query_count=len(queries),
                    current_title=query,
                    page_url=_wechat_sogou_url(query, page),
                )
                rows = _parse_wechat_sogou_articles(_fetch_wechat_sogou_page(query, page), query=query, page=page)
                if not rows:
                    break
                raw_count += len(rows)
                query_raw_count += len(rows)
                for raw in rows:
                    article_id = str(raw.get("source_thread_id") or "").strip()
                    if not article_id or article_id in seen_articles:
                        continue
                    seen_articles.add(article_id)
                    if not _in_date_window(_parse_datetime(raw.get("published_at")), start_date, end_date):
                        continue
                    text = _post_text(raw)
                    article_symbols = _extract_forum_post_symbols(raw, stock_aliases=stock_aliases)
                    raw["stock_codes"] = article_symbols
                    raw["keywords"] = _nga_keywords(text, article_symbols)
                    score, label = _nga_sentiment(text)
                    raw["sentiment_score"] = score
                    raw["sentiment_label"] = label
                    thread = normalize_wechat_sogou_thread(raw, stock_aliases=stock_aliases)
                    if thread is not None:
                        threads.append(thread)
                    if symbol:
                        if not _post_mentions_symbol(raw, symbol, aliases):
                            continue
                        normalized.extend(normalize_wechat_sogou_data_posts(raw, symbol=symbol, aliases=aliases))
                    else:
                        normalized.extend(normalize_wechat_sogou_data_posts(raw, stock_aliases=stock_aliases))
                self._emit_progress(
                    "wechat_sogou.page_parsed",
                    source="wechat_sogou",
                    current_step="wechat_sogou_page",
                    current_page=page,
                    page_limit=max_pages,
                    query_index=query_index,
                    query_count=len(queries),
                    current_title=query,
                    rows_on_page=len(rows),
                    posts_collected=len(normalized),
                    threads_collected=len(threads),
                )
            query_rows.append(
                {
                    "query": query,
                    "raw_count": query_raw_count,
                    "matched": len(normalized) - query_matched_before,
                }
            )
        return [post for post in normalized if post.source_post_id], threads, {
            "collected": raw_count,
            "queries": query_rows,
        }

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
    ) -> tuple[list[SentimentPostInput], list[SentimentThreadInput], NgaIngestStats]:
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

        threads = [
            thread
            for raw in analyzed
            if (thread := normalize_nga_thread(raw, stock_aliases=stock_aliases)) is not None
        ]

        normalized: list[SentimentPostInput] = []
        for raw in analyzed:
            if symbol:
                if not _post_mentions_symbol(raw, symbol, aliases):
                    continue
                normalized.extend(normalize_nga_data_posts(raw, symbol=symbol, aliases=aliases))
                continue
            normalized.extend(normalize_nga_data_posts(raw, stock_aliases=stock_aliases))
        stats.matched_posts = len(normalized)
        return normalized, threads, stats

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
        crawled_posts: list[dict[str, Any]] = []
        requested_days = list(_date_range(start, end))
        requested_day_set = set(requested_days)
        missing_days: list[date] = []
        posts_by_day: dict[date, list[dict[str, Any]]] = {}
        self._emit_progress(
            "nga.prepare",
            current_step="prepare",
            start_date=start.isoformat(),
            end_date=end.isoformat(),
            requested_dates=[day.isoformat() for day in requested_days],
            force_refresh=force_refresh,
            cache_partition=stats.cache_partition,
            scan_time_basis=stats.scan_time_basis,
        )
        for day in requested_days:
            path = data_dir / _date_file_name(day)
            self._emit_progress(
                "nga.cache.check",
                current_step="cache_check",
                current_date=day.isoformat(),
                current_date_file=str(path),
            )
            if path.exists() and not force_refresh:
                posts = _read_json_list(path)
                posts_by_day[day] = posts
                stats.loaded_dates.append(day.isoformat())
                self._emit_progress(
                    "nga.cache.loaded",
                    current_step="cache_loaded",
                    current_date=day.isoformat(),
                    current_date_file=str(path),
                    cache_posts=len(posts),
                )
                continue
            missing_days.append(day)

        if missing_days:
            crawl_start = min(missing_days)
            crawl_end = max(missing_days)
            self._emit_progress(
                "nga.crawl.range_start",
                current_step="crawl_range",
                crawl_start=crawl_start.isoformat(),
                crawl_end=crawl_end.isoformat(),
                missing_dates=[day.isoformat() for day in missing_days],
            )
            crawled_posts = self._crawl_nga_range(crawl_start, crawl_end, max_pages=max_pages)
            crawled_by_day = _bucket_nga_posts_by_publish_day(crawled_posts)
            for day, posts in sorted(crawled_by_day.items()):
                path = data_dir / _date_file_name(day)
                if day in requested_day_set:
                    existing = posts_by_day.get(day, [])
                    posts_by_day[day] = _merge_nga_post_lists(existing, posts)
                    _write_json_list(path, posts_by_day[day])
                    self._emit_progress(
                        "nga.cache.write",
                        current_step="cache_write",
                        current_date=day.isoformat(),
                        current_date_file=str(path),
                        cache_posts=len(posts_by_day[day]),
                    )
                    continue
                if path.exists():
                    posts = _merge_nga_post_lists(_read_json_list(path), posts)
                _write_json_list(path, posts)
                stats.extra_date_files.append(str(path))
                self._emit_progress(
                    "nga.cache.write_extra",
                    current_step="cache_write_extra",
                    current_date=day.isoformat(),
                    current_date_file=str(path),
                    cache_posts=len(posts),
                )
            for day in missing_days:
                posts = posts_by_day.get(day, [])
                if day not in crawled_by_day:
                    path = data_dir / _date_file_name(day)
                    _write_json_list(path, posts)
                    self._emit_progress(
                        "nga.cache.write_empty",
                        current_step="cache_write",
                        current_date=day.isoformat(),
                        current_date_file=str(path),
                        cache_posts=0,
                    )
                stats.crawled_dates.append(day.isoformat())
                if not posts:
                    stats.empty_dates.append(day.isoformat())

        raw_posts: list[dict[str, Any]] = []
        for day in requested_days:
            path = data_dir / _date_file_name(day)
            stats.date_files.append(str(path))
            raw_posts.extend(posts_by_day.get(day, []))
        extra_posts = [
            post
            for post in crawled_posts
            if (publish_day := _nga_publish_day(post)) is not None and publish_day not in requested_day_set
        ]
        merged_posts = _merge_nga_post_lists(raw_posts, extra_posts)
        self._emit_progress(
            "nga.load.done",
            current_step="load_done",
            total_posts=len(merged_posts),
            loaded_dates=stats.loaded_dates,
            crawled_dates=stats.crawled_dates,
            empty_dates=stats.empty_dates,
            extra_date_files=stats.extra_date_files,
        )
        return merged_posts, stats

    def _crawl_nga_day(self, day: date, *, max_pages: int) -> list[Any]:
        return self._crawl_nga_range(day, day, max_pages=max_pages)

    def _crawl_nga_range(self, start: date, end: date, *, max_pages: int) -> list[Any]:
        cookie = _config_value("NGA_COOKIE", "nga_cookie")
        if not cookie:
            raise RuntimeError(
                "NGA_COOKIE is required to crawl missing NGAdata files; "
                "set it in the environment or pre-populate the shared sentiment data directory"
            )
        start_dt = datetime.combine(start, time.min)
        end_dt = datetime.combine(end, time.max.replace(microsecond=0))
        board_fid = int(getattr(settings, "nga_board_fid", 706) or 706)
        collected: list[dict[str, Any]] = []
        seen_tids: set[str] = set()
        page_limit = max_pages * max(1, (end - start).days + 1) + 1
        self._emit_progress(
            "nga.crawl.start",
            current_step="crawl",
            crawl_start=start.isoformat(),
            crawl_end=end.isoformat(),
            max_pages=max_pages,
            page_limit=page_limit,
            board_fid=board_fid,
        )
        for page in range(1, page_limit + 1):
            self._emit_progress(
                "nga.board.page_fetch",
                current_step="board_page",
                board_page=page,
                current_page=page,
                page_limit=page_limit,
                threads_collected=len(collected),
            )
            board_html = _fetch_nga_html("https://nga.178.com/thread.php", params={"fid": board_fid, "page": page})
            topics = _parse_nga_board_topics(board_html)
            if not topics:
                self._emit_progress(
                    "nga.board.no_topics",
                    current_step="board_page",
                    board_page=page,
                    current_page=page,
                    page_limit=page_limit,
                    threads_collected=len(collected),
                )
                break
            self._emit_progress(
                "nga.board.page_parsed",
                current_step="board_page",
                board_page=page,
                current_page=page,
                page_limit=page_limit,
                topics_on_page=len(topics),
                threads_collected=len(collected),
            )
            parsed_topic_times = [
                value
                for topic in topics
                if (value := _parse_datetime(topic.get("publish_time"))) is not None
            ]
            for topic_index, topic in enumerate(topics, start=1):
                tid = str(topic.get("tid") or "")
                if not tid or tid in seen_tids:
                    continue
                seen_tids.add(tid)
                topic_title = str(topic.get("title") or "")
                self._emit_progress(
                    "nga.thread.fetch",
                    current_step="thread_detail",
                    board_page=page,
                    current_page=page,
                    page_limit=page_limit,
                    topic_index=topic_index,
                    topics_on_page=len(topics),
                    current_tid=tid,
                    current_title=topic_title[:120],
                    detail_page="e",
                    threads_collected=len(collected),
                )
                detail_html = _fetch_nga_html("https://nga.178.com/read.php", params={"tid": tid, "page": "e"})
                thread_posts = _parse_nga_thread_posts(detail_html)
                if not thread_posts:
                    self._emit_progress(
                        "nga.thread.empty",
                        current_step="thread_detail",
                        board_page=page,
                        current_page=page,
                        page_limit=page_limit,
                        current_tid=tid,
                        current_title=topic_title[:120],
                        detail_page="e",
                        threads_collected=len(collected),
                    )
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
                self._emit_progress(
                    "nga.thread.collected",
                    current_step="thread_detail",
                    board_page=page,
                    current_page=page,
                    page_limit=page_limit,
                    topic_index=topic_index,
                    topics_on_page=len(topics),
                    current_tid=tid,
                    current_title=title[:120],
                    detail_page="e",
                    publish_time=publish_time.isoformat() if isinstance(publish_time, datetime) else str(publish_time),
                    last_reply_time=last_reply_time.isoformat() if isinstance(last_reply_time, datetime) else str(last_reply_time),
                    reply_count=topic.get("reply_count", len(comments)),
                    comments_count=len(comments),
                    threads_collected=len(collected),
                )
            if parsed_topic_times and all(item < start_dt for item in parsed_topic_times):
                self._emit_progress(
                    "nga.crawl.stop_before_start",
                    current_step="crawl_stop",
                    board_page=page,
                    current_page=page,
                    page_limit=page_limit,
                    threads_collected=len(collected),
                    oldest_page_time=min(parsed_topic_times).isoformat(),
                )
                break
        self._emit_progress(
            "nga.crawl.done",
            current_step="crawl_done",
            crawl_start=start.isoformat(),
            crawl_end=end.isoformat(),
            threads_collected=len(collected),
        )
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


def _extract_forum_post_symbols(
    raw: dict[str, Any],
    stock_aliases: list[tuple[str, str]] | None = None,
) -> list[str]:
    normalized = _extract_nga_post_symbols(raw, stock_aliases=stock_aliases)
    text = _post_text(raw)
    for code in re.findall(r"(?<!\d)(?:60|68|00|30)\d{4}(?!\d)", text):
        symbol = normalize_security_symbol(code, _infer_market_from_code(code))
        if symbol and "." in symbol and symbol not in normalized:
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

    if source == "eastmoney_guba":
        return {
            "label": "东方财富股吧",
            "project_dir": "https://guba.eastmoney.com/",
            "project_ready": True,
            "cookie_configured": False,
            "cache_dir": None,
            "cache_file_count": 0,
            "ready": True,
        }

    if source == "jisilu":
        return {
            "label": "集思录股票",
            "project_dir": "https://www.jisilu.cn/category/8",
            "project_ready": True,
            "cookie_configured": False,
            "cache_dir": None,
            "cache_file_count": 0,
            "ready": True,
        }

    if source == "wechat_sogou":
        return {
            "label": "搜狗微信",
            "project_dir": "https://weixin.sogou.com/weixin?type=2",
            "project_ready": True,
            "cookie_configured": bool(_config_value("WECHAT_SOGOU_COOKIE", "wechat_sogou_cookie").strip()),
            "cache_dir": None,
            "cache_file_count": 0,
            "ready": True,
        }

    if source == "flocktrader":
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


def normalize_eastmoney_guba_post(raw: dict[str, Any], *, symbol: str) -> SentimentPostInput | None:
    source_post_id = str(raw.get("post_id") or "").strip()
    if not source_post_id:
        return None
    normalized_symbol = normalize_sentiment_symbol(symbol)
    code = normalized_symbol.split(".", 1)[0]
    bar_code = str(raw.get("stockbar_code") or code).strip() or code
    if bar_code != code:
        return None
    title = _strip_html_fallback(raw.get("post_title") or "")
    published_at = _parse_datetime(raw.get("post_publish_time") or raw.get("post_display_time"))
    reply_count = _to_int(raw.get("post_comment_count"))
    text = title
    score, label = _nga_sentiment(text)
    keywords = _nga_keywords(text, [code])
    return SentimentPostInput(
        source="eastmoney_guba",
        source_post_id=source_post_id,
        symbol=normalized_symbol,
        title=title or None,
        content=None,
        author=str(raw.get("user_nickname") or "") or None,
        published_at=published_at,
        url=f"https://guba.eastmoney.com/news,{bar_code},{source_post_id}.html",
        reply_count=reply_count,
        like_count=0,
        comment_count=reply_count,
        sentiment_score=score,
        sentiment_label=_normalize_sentiment_label(label, score),
        keywords=keywords,
        raw=raw,
    )


def normalize_jisilu_data_post(
    raw: dict[str, Any],
    *,
    symbol: str,
    aliases: list[str] | None = None,
) -> SentimentPostInput | None:
    question_id = str(raw.get("question_id") or "").strip()
    if not question_id:
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
        source="jisilu",
        source_post_id=f"{question_id}:{symbol}",
        symbol=symbol,
        title=str(raw.get("title") or raw.get("detail_title") or "") or None,
        content=str(raw.get("content") or "") or None,
        author=str(raw.get("author") or "") or None,
        published_at=_parse_datetime(raw.get("published_time") or raw.get("active_time")),
        url=str(raw.get("url") or f"https://www.jisilu.cn/question/{question_id}"),
        reply_count=reply_count,
        like_count=0,
        comment_count=reply_count,
        sentiment_score=score,
        sentiment_label=_normalize_sentiment_label(str(raw.get("sentiment_label") or ""), score),
        keywords=keywords,
        raw=raw,
    )


def normalize_jisilu_data_posts(
    raw: dict[str, Any],
    *,
    symbol: str | None = None,
    aliases: list[str] | None = None,
    stock_aliases: list[tuple[str, str]] | None = None,
) -> list[SentimentPostInput]:
    target_symbols = [normalize_sentiment_symbol(symbol)] if symbol else _extract_forum_post_symbols(raw, stock_aliases=stock_aliases)
    normalized: list[SentimentPostInput] = []
    for target_symbol in target_symbols:
        post = normalize_jisilu_data_post(raw, symbol=target_symbol, aliases=aliases if symbol else None)
        if post is not None:
            normalized.append(post)
    return normalized


def normalize_wechat_sogou_data_post(
    raw: dict[str, Any],
    *,
    symbol: str,
    aliases: list[str] | None = None,
) -> SentimentPostInput | None:
    article_id = str(raw.get("source_thread_id") or "").strip()
    if not article_id:
        return None
    symbol = normalize_sentiment_symbol(symbol)
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
        source="wechat_sogou",
        source_post_id=f"{article_id}:{symbol}",
        symbol=symbol,
        title=str(raw.get("title") or "") or None,
        content=str(raw.get("content") or "") or None,
        author=str(raw.get("author") or raw.get("account") or "") or None,
        published_at=_parse_datetime(raw.get("published_at")),
        url=str(raw.get("url") or "") or None,
        reply_count=0,
        like_count=0,
        comment_count=0,
        sentiment_score=score,
        sentiment_label=_normalize_sentiment_label(str(raw.get("sentiment_label") or ""), score),
        keywords=keywords,
        raw=raw,
    )


def normalize_wechat_sogou_data_posts(
    raw: dict[str, Any],
    *,
    symbol: str | None = None,
    aliases: list[str] | None = None,
    stock_aliases: list[tuple[str, str]] | None = None,
) -> list[SentimentPostInput]:
    target_symbols = [normalize_sentiment_symbol(symbol)] if symbol else _extract_forum_post_symbols(raw, stock_aliases=stock_aliases)
    normalized: list[SentimentPostInput] = []
    for target_symbol in target_symbols:
        post = normalize_wechat_sogou_data_post(raw, symbol=target_symbol, aliases=aliases if symbol else None)
        if post is not None:
            normalized.append(post)
    return normalized


def normalize_wechat_sogou_thread(
    raw: dict[str, Any],
    *,
    stock_aliases: list[tuple[str, str]] | None = None,
) -> SentimentThreadInput | None:
    article_id = str(raw.get("source_thread_id") or "").strip()
    if not article_id:
        return None
    score = raw.get("sentiment_score")
    try:
        score = float(score) if score is not None else None
    except (TypeError, ValueError):
        score = None
    symbols = _extract_forum_post_symbols(raw, stock_aliases=stock_aliases)
    keywords = [str(item) for item in (raw.get("keywords") or []) if str(item).strip()]
    for symbol in symbols:
        if symbol not in keywords:
            keywords.append(symbol)
    return SentimentThreadInput(
        source="wechat_sogou",
        source_thread_id=article_id,
        title=str(raw.get("title") or "") or None,
        content=str(raw.get("content") or "") or None,
        author=str(raw.get("author") or raw.get("account") or "") or None,
        published_at=_parse_datetime(raw.get("published_at")),
        last_reply_at=_parse_datetime(raw.get("published_at")),
        url=str(raw.get("url") or "") or None,
        reply_count=0,
        comment_count=0,
        sentiment_score=score,
        sentiment_label=_normalize_sentiment_label(str(raw.get("sentiment_label") or ""), score),
        symbols=symbols,
        keywords=keywords,
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


def normalize_nga_thread(
    raw: dict[str, Any],
    *,
    stock_aliases: list[tuple[str, str]] | None = None,
) -> SentimentThreadInput | None:
    tid = str(raw.get("tid") or "").strip()
    if not tid:
        return None
    comments = raw.get("comments") or []
    reply_count = _to_int(raw.get("reply_count"), len(comments) if isinstance(comments, list) else 0)
    score = raw.get("sentiment_score")
    try:
        score = float(score) if score is not None else None
    except (TypeError, ValueError):
        score = None
    symbols = _extract_nga_post_symbols(raw, stock_aliases=stock_aliases)
    keywords = [str(item) for item in (raw.get("keywords") or []) if str(item).strip()]
    for symbol in symbols:
        if symbol not in keywords:
            keywords.append(symbol)
    return SentimentThreadInput(
        source="flocktrader",
        source_thread_id=tid,
        title=str(raw.get("title") or "") or None,
        content=str(raw.get("content") or "") or None,
        author=str(raw.get("author") or "") or None,
        published_at=_parse_datetime(raw.get("publish_time")),
        last_reply_at=_parse_datetime(raw.get("last_reply_time")),
        url=f"https://bbs.nga.cn/read.php?tid={tid}",
        reply_count=reply_count,
        comment_count=reply_count,
        sentiment_score=score,
        sentiment_label=_normalize_sentiment_label(str(raw.get("sentiment_label") or ""), score),
        symbols=symbols,
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
