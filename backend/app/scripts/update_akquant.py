#!/usr/bin/env python
"""Check, upgrade, and validate the platform AKQuant dependency.

Default usage is intentionally safe:

    python -m app.scripts.update_akquant

That only checks PyPI and the local backend virtualenv. To update the local
environment and platform dependency declarations, run:

    python -m app.scripts.update_akquant --upgrade
"""
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

PACKAGE_NAME = "akquant"
PYPI_JSON_URL = f"https://pypi.org/pypi/{PACKAGE_NAME}/json"
DEFAULT_TESTS = (
    "tests/backtest/test_akquant_integration.py",
    "tests/backtest/test_akquant_parquet_provider.py",
)
VERSION_RE = re.compile(r"^\d+(?:\.\d+)*(?:[A-Za-z0-9_.!+-]*)?$")


@dataclass(frozen=True)
class VersionState:
    installed: str | None
    latest: str
    target: str


def find_repo_root(start: Path | None = None) -> Path:
    """Find the repository root by locating backend/pyproject.toml."""
    current = (start or Path(__file__)).resolve()
    if current.is_file():
        current = current.parent

    for candidate in (current, *current.parents):
        if (candidate / "backend" / "pyproject.toml").exists():
            return candidate

    raise RuntimeError("Could not locate repository root with backend/pyproject.toml")


def default_backend_python(repo_root: Path) -> Path:
    """Prefer the backend virtualenv so updates affect the running platform."""
    if os.name == "nt":
        candidate = repo_root / "backend" / ".venv" / "Scripts" / "python.exe"
    else:
        candidate = repo_root / "backend" / ".venv" / "bin" / "python"

    if candidate.exists():
        return candidate
    return Path(sys.executable)


def parse_release_tuple(version: str) -> tuple[int, ...]:
    """Parse stable numeric releases used by AKQuant, e.g. 0.2.40."""
    match = re.match(r"^(\d+(?:\.\d+)*)", version)
    if not match:
        raise ValueError(f"Unsupported version format: {version!r}")
    return tuple(int(part) for part in match.group(1).split("."))


def compare_versions(left: str, right: str) -> int:
    """Return -1, 0, or 1 for the numeric release portion of two versions."""
    left_parts = list(parse_release_tuple(left))
    right_parts = list(parse_release_tuple(right))
    width = max(len(left_parts), len(right_parts))
    left_parts.extend([0] * (width - len(left_parts)))
    right_parts.extend([0] * (width - len(right_parts)))

    if left_parts < right_parts:
        return -1
    if left_parts > right_parts:
        return 1
    return 0


def validate_version(version: str) -> None:
    if not VERSION_RE.match(version):
        raise ValueError(f"Refusing suspicious version string: {version!r}")


def fetch_latest_version(timeout: float = 15.0, retries: int = 3) -> str:
    """Fetch the latest AKQuant release version from PyPI JSON metadata."""
    request = urllib.request.Request(
        PYPI_JSON_URL,
        headers={"Accept": "application/json", "User-Agent": "GaoshouPlatform-AKQuant-Updater"},
    )

    last_error: BaseException | None = None
    for attempt in range(1, retries + 1):
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                payload = json.loads(response.read().decode("utf-8"))
            break
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            last_error = exc
            if attempt < retries:
                time.sleep(min(attempt, 3))
    else:
        raise RuntimeError(
            f"Failed to fetch latest {PACKAGE_NAME} version from PyPI after {retries} attempts: "
            f"{last_error}"
        ) from last_error

    version = payload.get("info", {}).get("version")
    if not isinstance(version, str):
        raise RuntimeError(f"PyPI response did not contain info.version for {PACKAGE_NAME}")

    validate_version(version)
    return version


def get_installed_version(python_exe: Path) -> str | None:
    """Read the AKQuant version installed in the selected Python environment."""
    command = [
        str(python_exe),
        "-c",
        (
            "import importlib.metadata as m\n"
            f"print(m.version({PACKAGE_NAME!r}))\n"
        ),
    ]
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode == 0:
        version = result.stdout.strip()
        validate_version(version)
        return version

    stderr = result.stderr.lower()
    if "packagenotfounderror" in stderr or "no package metadata" in stderr:
        return None

    raise RuntimeError(
        "Failed to inspect installed AKQuant version:\n"
        f"command: {' '.join(command)}\n"
        f"stderr: {result.stderr.strip()}"
    )


def _dependency_pattern(package: str) -> re.Pattern[str]:
    return re.compile(rf"(?P<prefix>[\"']?{re.escape(package)}\s*>=)(?P<version>[^\"'\s,]+)(?P<suffix>[\"']?)")


def update_pyproject_dependency(text: str, package: str, version: str) -> tuple[str, bool]:
    """Update an existing pyproject dependency string for AKQuant."""
    pattern = _dependency_pattern(package)

    def replace(match: re.Match[str]) -> str:
        return f"{match.group('prefix')}{version}{match.group('suffix')}"

    new_text, count = pattern.subn(replace, text, count=1)
    if count:
        return new_text, new_text != text

    marker = '    "tushare>=1.4.0",'
    if marker in text:
        inserted = f'    "{package}>={version}",\n{marker}'
        return text.replace(marker, inserted, 1), True

    raise RuntimeError(f"Could not find a safe insertion point for {package} in pyproject.toml")


def update_requirements_dependency(text: str, package: str, version: str) -> tuple[str, bool]:
    """Update or add AKQuant in requirements.txt."""
    lines = text.splitlines()
    requirement_re = re.compile(rf"^\s*{re.escape(package)}\s*(?:[<>=!~]=.*)?(?:\s*#.*)?$")
    replacement = f"{package}>={version}"

    for index, line in enumerate(lines):
        if requirement_re.match(line):
            if line == replacement:
                return text, False
            lines[index] = replacement
            return "\n".join(lines) + ("\n" if text.endswith("\n") else ""), True

    insert_after = None
    for index, line in enumerate(lines):
        if line.strip() == "loguru>=0.7.0":
            insert_after = index + 1
            break

    if insert_after is None:
        for index, line in enumerate(lines):
            if line.startswith("# Optional local integrations"):
                insert_after = index
                break

    if insert_after is None:
        insert_after = len(lines)

    lines.insert(insert_after, replacement)
    return "\n".join(lines) + "\n", True


def sync_dependency_files(repo_root: Path, version: str) -> list[Path]:
    """Sync platform dependency declarations to the validated AKQuant version."""
    validate_version(version)
    updates: list[tuple[Path, str]] = []

    pyproject = repo_root / "backend" / "pyproject.toml"
    requirements = repo_root / "backend" / "requirements.txt"

    pyproject_text = pyproject.read_text(encoding="utf-8")
    new_pyproject, changed = update_pyproject_dependency(pyproject_text, PACKAGE_NAME, version)
    if changed:
        updates.append((pyproject, new_pyproject))

    requirements_text = requirements.read_text(encoding="utf-8")
    new_requirements, changed = update_requirements_dependency(requirements_text, PACKAGE_NAME, version)
    if changed:
        updates.append((requirements, new_requirements))

    for path, content in updates:
        path.write_text(content, encoding="utf-8")

    return [path for path, _ in updates]


def run_command(command: Iterable[str], cwd: Path, *, dry_run: bool = False) -> None:
    command_list = [str(part) for part in command]
    print(f"$ {' '.join(command_list)}")
    if dry_run:
        return

    subprocess.run(command_list, cwd=str(cwd), check=True)


def install_target_version(python_exe: Path, version: str, backend_dir: Path, *, dry_run: bool) -> None:
    run_command(
        [str(python_exe), "-m", "pip", "install", "--upgrade", f"{PACKAGE_NAME}=={version}"],
        backend_dir,
        dry_run=dry_run,
    )


def run_regression_tests(
    python_exe: Path,
    backend_dir: Path,
    tests: tuple[str, ...],
    *,
    dry_run: bool,
) -> None:
    if not tests:
        return
    run_command([str(python_exe), "-m", "pytest", *tests, "-q"], backend_dir, dry_run=dry_run)


def print_state(python_exe: Path, state: VersionState) -> None:
    installed = state.installed or "not installed"
    status = "up-to-date"
    if state.installed is None or compare_versions(state.installed, state.target) < 0:
        status = "update available"
    elif compare_versions(state.installed, state.target) > 0:
        status = "installed newer than target"

    print("AKQuant update check")
    print(f"  python:    {python_exe}")
    print(f"  installed: {installed}")
    print(f"  latest:    {state.latest}")
    print(f"  target:    {state.target}")
    print(f"  status:    {status}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Check, upgrade, and validate GaoshouPlatform's AKQuant dependency.",
    )
    parser.add_argument(
        "--upgrade",
        action="store_true",
        help="Install the target AKQuant version, sync dependency files, and run regression tests.",
    )
    parser.add_argument(
        "--version",
        dest="target_version",
        help="Install/check a specific AKQuant version instead of the latest PyPI release.",
    )
    parser.add_argument(
        "--python",
        dest="python_exe",
        type=Path,
        help="Python executable to update. Defaults to backend/.venv when available.",
    )
    parser.add_argument(
        "--test",
        action="append",
        dest="tests",
        help="Regression test path to run after upgrade. Repeat to provide multiple tests.",
    )
    parser.add_argument(
        "--skip-tests",
        action="store_true",
        help="Upgrade and sync dependency files without running pytest.",
    )
    parser.add_argument(
        "--no-update-files",
        action="store_true",
        help="Upgrade the environment but do not rewrite pyproject.toml or requirements.txt.",
    )
    parser.add_argument(
        "--allow-downgrade",
        action="store_true",
        help="Allow --version to install a version older than the current environment.",
    )
    parser.add_argument(
        "--fail-if-outdated",
        action="store_true",
        help="Exit with code 2 when check-only mode finds an available update.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print planned install/test commands without running them or writing files.",
    )
    parser.add_argument(
        "--pypi-timeout",
        type=float,
        default=15.0,
        help="Seconds to wait for each PyPI metadata request.",
    )
    parser.add_argument(
        "--pypi-retries",
        type=int,
        default=3,
        help="Number of attempts for PyPI metadata requests.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    repo_root = find_repo_root()
    backend_dir = repo_root / "backend"
    python_exe = (args.python_exe or default_backend_python(repo_root)).resolve()

    if not python_exe.exists():
        raise SystemExit(f"Python executable does not exist: {python_exe}")

    latest = args.target_version or fetch_latest_version(
        timeout=args.pypi_timeout,
        retries=max(1, args.pypi_retries),
    )
    target = args.target_version or latest
    validate_version(target)

    installed = get_installed_version(python_exe)
    state = VersionState(installed=installed, latest=latest, target=target)
    print_state(python_exe, state)

    if installed is not None and compare_versions(target, installed) < 0 and not args.allow_downgrade:
        raise SystemExit(
            f"Target {target} is older than installed {installed}. "
            "Use --allow-downgrade if this rollback is intentional."
        )

    if not args.upgrade:
        if args.fail_if_outdated and (installed is None or compare_versions(installed, target) < 0):
            return 2
        print("Check-only mode. Use --upgrade to install and validate the target version.")
        return 0

    if installed != target:
        install_target_version(python_exe, target, backend_dir, dry_run=args.dry_run)
        if not args.dry_run:
            installed_after = get_installed_version(python_exe)
            if installed_after != target:
                raise RuntimeError(
                    f"Expected {PACKAGE_NAME} {target}, but environment reports {installed_after}"
                )
    else:
        print(f"{PACKAGE_NAME} {target} is already installed.")

    if not args.no_update_files:
        if args.dry_run:
            print("Would sync backend/pyproject.toml and backend/requirements.txt.")
        else:
            updated = sync_dependency_files(repo_root, target)
            if updated:
                print("Updated dependency files:")
                for path in updated:
                    print(f"  {path.relative_to(repo_root)}")
            else:
                print("Dependency files already match the target version.")

    tests = tuple(args.tests) if args.tests else DEFAULT_TESTS
    if args.skip_tests:
        print("Skipping regression tests by request.")
    else:
        run_regression_tests(python_exe, backend_dir, tests, dry_run=args.dry_run)

    print("AKQuant update workflow completed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
