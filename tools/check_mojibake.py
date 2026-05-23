"""Scan source files for common mojibake patterns.

This is intentionally conservative: it targets sequences that repeatedly show
up when UTF-8 Chinese text is decoded with the wrong code page, plus a few
unexpected Unicode ranges that should not appear in this codebase UI text.
"""

from __future__ import annotations

from pathlib import Path


ROOTS = [Path("backend/app"), Path("frontend/src")]
SKIP_PARTS = {"__pycache__", "node_modules", ".venv", "dist", "build"}
SOURCE_SUFFIXES = {
    ".py",
    ".vue",
    ".ts",
    ".tsx",
    ".js",
    ".jsx",
    ".css",
    ".scss",
    ".json",
    ".md",
}

MOJIBAKE_PATTERNS = [
    "\ufffd",
    "Ã",
    "Â",
    "鏃",
    "琛",
    "瑕",
    "鎹",
    "姣",
    "璐",
    "绾",
    "鍥",
    "瀹",
    "搴",
    "杈",
    "鍙",
    "鏁",
    "寮",
    "瑙",
    "闇",
    "鏈",
    "涓",
    "婊",
    "褰",
    "鍚",
    "淇",
    "骞",
    "缂",
    "濂",
    "宸",
    "浠",
    "妯",
    "绛",
    "纭",
    "蹇",
    "闈",
    "熶",
    "€",
    "\ufeff",
]

UNEXPECTED_RANGES = [
    (0x0590, 0x05FF),  # Hebrew
    (0x0600, 0x06FF),  # Arabic
    (0x0400, 0x04FF),  # Cyrillic
]


def has_unexpected_char(line: str) -> bool:
    return any(start <= ord(char) <= end for char in line for start, end in UNEXPECTED_RANGES)


def should_scan(path: Path) -> bool:
    if not path.is_file():
        return False
    if path.suffix.lower() not in SOURCE_SUFFIXES:
        return False
    return not any(part in SKIP_PARTS for part in path.parts)


def main() -> int:
    findings: list[tuple[Path, int, str]] = []
    for root in ROOTS:
        for path in root.rglob("*"):
            if not should_scan(path):
                continue
            try:
                text = path.read_text(encoding="utf-8")
            except UnicodeDecodeError as exc:
                findings.append((path, 0, f"UTF-8 decode failed: {exc}"))
                continue
            for line_number, line in enumerate(text.splitlines(), 1):
                if any(pattern in line for pattern in MOJIBAKE_PATTERNS) or has_unexpected_char(line):
                    findings.append((path, line_number, line.strip()))

    if findings:
        for path, line_number, line in findings:
            location = f"{path}:{line_number}" if line_number else str(path)
            print(f"{location}: {line[:180]}")
        return 1

    print("No mojibake patterns found in backend/app or frontend/src")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
