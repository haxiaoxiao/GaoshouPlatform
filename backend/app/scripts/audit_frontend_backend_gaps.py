from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[3]
FRONTEND_ROOT = ROOT / "frontend" / "src"
DEFAULT_OUTPUT = ROOT / ".tmp" / "frontend-backend-gap-report.json"
DEFAULT_OPENAPI_URL = "http://127.0.0.1:18801/openapi.json"
DEFAULT_BASE_URL = "http://127.0.0.1:18801"

REQUEST_CALL_RE = re.compile(
    r"""request\.(?P<method>get|post|put|delete|patch)\s*(?:<[^>]+>\s*)?\(\s*(?P<quote>[`'"])(?P<path>.*?)(?P=quote)""",
    re.S,
)
FETCH_CALL_RE = re.compile(
    r"""fetch\(\s*(?P<quote>[`'"])(?P<path>.*?)(?P=quote)""",
    re.S,
)
AXIOS_CALL_RE = re.compile(
    r"""axios\.(?P<method>get|post|put|delete|patch)\s*\(\s*(?P<quote>[`'"])(?P<path>.*?)(?P=quote)""",
    re.S,
)
TEMPLATE_EXPR_RE = re.compile(r"\$\{([^}]+)\}")


@dataclass
class FrontendCall:
    file: str
    line: int
    method: str
    raw_path: str
    path: str
    api_path: str


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit frontend API calls against backend OpenAPI.")
    parser.add_argument("--root", default=str(ROOT), help="Project root directory")
    parser.add_argument("--openapi-url", default=DEFAULT_OPENAPI_URL, help="OpenAPI URL")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL, help="Runtime probe base URL")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT), help="Output JSON report")
    args = parser.parse_args()

    root = Path(args.root).resolve()
    frontend_root = root / "frontend" / "src"
    output = Path(args.output).resolve()

    frontend_calls = collect_frontend_calls(frontend_root)
    openapi = load_json_url(args.openapi_url)
    openapi_paths = collect_openapi_paths(openapi) if isinstance(openapi, dict) else {}
    comparisons = compare_calls(frontend_calls, openapi_paths)
    runtime_checks = run_runtime_checks(args.base_url)

    report = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "root": str(root),
        "openapi_url": args.openapi_url,
        "base_url": args.base_url,
        "frontend_call_count": len(frontend_calls),
        "openapi_path_count": len(openapi_paths),
        "frontend_calls": [call.__dict__ for call in frontend_calls],
        "missing_paths": comparisons["missing_paths"],
        "method_mismatches": comparisons["method_mismatches"],
        "runtime_checks": runtime_checks,
        "openapi_load_error": comparisons.get("openapi_load_error"),
    }

    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({
        "output": str(output),
        "frontend_call_count": len(frontend_calls),
        "missing_paths": len(comparisons["missing_paths"]),
        "method_mismatches": len(comparisons["method_mismatches"]),
        "runtime_checks": len(runtime_checks),
    }, ensure_ascii=False, indent=2))
    return 0


def collect_frontend_calls(frontend_root: Path) -> list[FrontendCall]:
    calls: list[FrontendCall] = []
    for path in frontend_root.rglob("*"):
        if path.suffix.lower() not in {".ts", ".tsx", ".vue", ".js", ".mjs"}:
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        calls.extend(parse_calls_from_text(path, text))
    return calls


def parse_calls_from_text(path: Path, text: str) -> list[FrontendCall]:
    calls: list[FrontendCall] = []
    for regex, method_override in (
        (REQUEST_CALL_RE, None),
        (FETCH_CALL_RE, "FETCH"),
        (AXIOS_CALL_RE, None),
    ):
        for match in regex.finditer(text):
            method = (
                infer_fetch_method(text, match.end())
                if method_override == "FETCH"
                else (method_override or match.groupdict().get("method") or "get").upper()
            )
            raw_path = match.group("path").strip()
            line = text.count("\n", 0, match.start()) + 1
            normalized = normalize_path_literal(raw_path)
            if normalized is None:
                continue
            calls.append(
                FrontendCall(
                    file=str(path.relative_to(ROOT)),
                    line=line,
                    method=method,
                    raw_path=raw_path,
                    path=normalized,
                    api_path=to_api_path(normalized),
                )
            )
    return dedupe_calls(calls)


def infer_fetch_method(text: str, start: int) -> str:
    trailer = text[start : start + 220]
    method_match = re.search(r"""method\s*:\s*['"](?P<method>GET|POST|PUT|DELETE|PATCH)['"]""", trailer, re.I)
    return method_match.group("method").upper() if method_match else "GET"


def dedupe_calls(calls: list[FrontendCall]) -> list[FrontendCall]:
    seen: set[tuple[str, str]] = set()
    unique: list[FrontendCall] = []
    for call in calls:
        key = (call.method, call.api_path)
        if key in seen:
            continue
        seen.add(key)
        unique.append(call)
    return unique


def normalize_path_literal(raw_path: str) -> str | None:
    if not raw_path:
        return None
    if raw_path.startswith("http://") or raw_path.startswith("https://"):
        return raw_path
    if raw_path.startswith("`") and raw_path.endswith("`"):
        raw_path = raw_path[1:-1]
    raw_path = raw_path.replace("\\/", "/")
    raw_path = TEMPLATE_EXPR_RE.sub(lambda match: "{" + _template_token(match.group(1)) + "}", raw_path)
    raw_path = raw_path.split("?", 1)[0]
    if not raw_path.startswith("/"):
        return None
    return raw_path


def _template_token(expr: str) -> str:
    token = re.sub(r"[^A-Za-z0-9_]+", "_", expr.strip())
    token = token.strip("_")
    return token or "param"


def to_api_path(path: str) -> str:
    if path.startswith("/api/"):
        return path
    if path == "/api":
        return path
    return f"/api{path}"


def load_json_url(url: str) -> dict[str, Any] | None:
    try:
        with urlopen(Request(url, headers={"Accept": "application/json"}), timeout=20) as response:
            return json.loads(response.read().decode("utf-8"))
    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as exc:
        return {"error": str(exc)}


def collect_openapi_paths(openapi: dict[str, Any]) -> dict[str, set[str]]:
    paths: dict[str, set[str]] = {}
    for path, spec in (openapi.get("paths") or {}).items():
        methods = set()
        if isinstance(spec, dict):
            for method in spec:
                if method.lower() in {"get", "post", "put", "delete", "patch"}:
                    methods.add(method.upper())
        paths[str(path)] = methods
    return paths


def compare_calls(frontend_calls: list[FrontendCall], openapi_paths: dict[str, set[str]]) -> dict[str, Any]:
    missing_paths = []
    method_mismatches = []
    if not openapi_paths:
        return {
            "missing_paths": missing_paths,
            "method_mismatches": method_mismatches,
            "openapi_load_error": "OpenAPI document unavailable",
        }

    openapi_method_map = {path: methods for path, methods in openapi_paths.items()}
    normalized_openapi = {path: methods for path, methods in openapi_method_map.items()}

    for call in frontend_calls:
        path_methods = normalized_openapi.get(call.api_path)
        if path_methods is None:
            shape_matches = [
                (path, methods)
                for path, methods in normalized_openapi.items()
                if _path_matches(path, call.api_path)
            ]
            if any(call.method in methods for _path, methods in shape_matches):
                continue
            if shape_matches:
                method_mismatches.append(
                    {
                        "file": call.file,
                        "line": call.line,
                        "method": call.method,
                        "path": call.api_path,
                        "available_methods": sorted({method for _path, methods in shape_matches for method in methods}),
                        "matched_by_shape": [path for path, _methods in shape_matches[:5]],
                    }
                )
                continue
            candidates = [path for path in normalized_openapi if _path_matches(path, call.api_path)]
            missing_paths.append(
                {
                    "file": call.file,
                    "line": call.line,
                    "method": call.method,
                    "path": call.api_path,
                    "reason": "path_missing",
                    "candidates": candidates[:5],
                }
            )
            continue
        if call.method not in path_methods:
            method_mismatches.append(
                {
                    "file": call.file,
                    "line": call.line,
                    "method": call.method,
                    "path": call.api_path,
                    "available_methods": sorted(path_methods),
                }
            )

    return {
        "missing_paths": missing_paths,
        "method_mismatches": method_mismatches,
    }


def _path_shape(path: str) -> str:
    return re.sub(r"\{[^}]+\}", "{}", path)


def _path_matches(openapi_path: str, actual_path: str) -> bool:
    openapi_parts = [part for part in openapi_path.strip("/").split("/") if part]
    actual_parts = [part for part in actual_path.strip("/").split("/") if part]
    if len(openapi_parts) != len(actual_parts):
        return False
    for expected, actual in zip(openapi_parts, actual_parts):
        if expected.startswith("{") and expected.endswith("}"):
            continue
        if expected != actual:
            return False
    return True


def run_runtime_checks(base_url: str) -> list[dict[str, Any]]:
    probes = [
        ("GET", "/api/system/data-summary", None),
        ("GET", "/api/data/stocks/600519.SH", None),
        ("GET", "/api/data/sync/status", None),
        ("GET", "/api/grid-trading/status", None),
        ("POST", "/api/grid-trading/signals", {"params": {}, "manual_account": None}),
    ]
    results: list[dict[str, Any]] = []
    for method, path, payload in probes:
        url = f"{base_url.rstrip('/')}{path}"
        result: dict[str, Any] = {"method": method, "path": path, "ok": False}
        try:
            body = None
            headers = {"Accept": "application/json"}
            if payload is not None:
                body = json.dumps(payload).encode("utf-8")
                headers["Content-Type"] = "application/json"
            req = Request(url, data=body, headers=headers, method=method)
            with urlopen(req, timeout=20) as response:
                raw = response.read().decode("utf-8")
                parsed = json.loads(raw) if raw else None
                result.update(
                    {
                        "ok": True,
                        "status": response.status,
                        "keys": sorted(list(parsed.keys()))[:20] if isinstance(parsed, dict) else None,
                    }
                )
        except Exception as exc:
            result["error"] = str(exc)
        results.append(result)
    return results


if __name__ == "__main__":
    raise SystemExit(main())
