from __future__ import annotations

import importlib
import importlib.util
import os
import signal
import subprocess
import sys
import time
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
import pytest
from fastapi import FastAPI

ROOT = Path(__file__).resolve().parents[3]


def test_ci_blocks_lint_and_disables_live_order_submit():
    workflow = (ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")

    assert 'LIVE_TRADING_ENABLE_ORDER_SUBMIT: "false"' in workflow
    assert 'LIVE_TRADING_AUTO_EXECUTE_ENABLED: "false"' in workflow
    assert "continue-on-error: true" not in workflow
    assert "npm test" in workflow
    assert "npm run test:e2e" in workflow


def test_prod_frontend_uses_preview_with_dynamic_port_fallback():
    start_script = (ROOT / "tools" / "start-gaoshouplatform.bat").read_text(encoding="utf-8")
    deploy_script = (ROOT / "tools" / "deploy-windows.ps1").read_text(encoding="utf-8")
    vite_config = (ROOT / "frontend" / "vite.config.ts").read_text(encoding="utf-8")

    assert 'FRONTEND_PORT=3500' in start_script
    assert ':resolve_frontend_port' in start_script
    assert 'frontend-port.txt' in start_script
    assert 'FrontendPort = "3500"' in deploy_script
    assert "port: 3500" in vite_config
    assert "npm run preview" in start_script
    assert "npm run dev" not in start_script


def test_startup_runs_alembic_before_backend_services_and_masks_account():
    start_script = (ROOT / "tools" / "start-gaoshouplatform.bat").read_text(encoding="utf-8")

    migration = start_script.index("upgrade head")
    sync_start = start_script.index("app.service_runner','app.sync_main:app")
    backend_start = start_script.index("app.service_runner','app.main:app")

    assert migration < sync_start < backend_start
    assert "QMT_ACCOUNT_MASK" in start_script
    assert "account %QMT_ACCOUNT_ID%" not in start_script


def _load_service_shutdown_module():
    spec = importlib.util.find_spec("app.core.service_shutdown")
    assert spec is not None, "the local graceful-shutdown endpoint is missing"
    return importlib.import_module("app.core.service_shutdown")


@pytest.mark.asyncio
async def test_local_shutdown_endpoint_requires_loopback_and_matching_process_id():
    shutdown = _load_service_shutdown_module()
    calls: list[str] = []
    app = FastAPI()
    shutdown.install_shutdown_endpoint(
        app,
        request_shutdown=lambda: calls.append("shutdown"),
        process_id=4321,
    )

    local_transport = httpx.ASGITransport(app=app, client=("127.0.0.1", 12345))
    async with httpx.AsyncClient(
        transport=local_transport,
        base_url="http://127.0.0.1",
    ) as client:
        wrong_pid = await client.post(
            "/internal/shutdown",
            headers={"X-Gaoshou-Process-ID": "9999"},
        )
        accepted = await client.post(
            "/internal/shutdown",
            headers={"X-Gaoshou-Process-ID": "4321"},
        )

    remote_transport = httpx.ASGITransport(app=app, client=("192.0.2.10", 12345))
    async with httpx.AsyncClient(
        transport=remote_transport,
        base_url="http://service.test",
    ) as client:
        remote = await client.post(
            "/internal/shutdown",
            headers={"X-Gaoshou-Process-ID": "4321"},
        )

    assert wrong_pid.status_code == 403
    assert accepted.status_code == 202
    assert remote.status_code == 403
    assert calls == ["shutdown"]


@pytest.mark.filterwarnings("ignore:websockets.legacy is deprecated:DeprecationWarning")
@pytest.mark.filterwarnings(
    "ignore:websockets.server.WebSocketServerProtocol is deprecated:DeprecationWarning"
)
def test_service_runner_returns_nonzero_when_lifespan_startup_fails(monkeypatch):
    runner = importlib.import_module("app.service_runner")

    async def fail_startup() -> None:
        raise RuntimeError("probe startup failed")

    @asynccontextmanager
    async def failing_lifespan(_app):
        await fail_startup()
        yield

    application = FastAPI(lifespan=failing_lifespan)
    monkeypatch.setattr(runner, "_load_app", lambda _import_path: application)

    assert runner.main(["probe:app", "--host", "127.0.0.1", "--port", "0"]) != 0


def test_service_runner_atomically_owns_and_removes_pid_file(monkeypatch, tmp_path):
    runner = importlib.import_module("app.service_runner")
    pid_file = tmp_path / "runtime" / "backend-api.pid"
    replacements: list[tuple[Path, Path]] = []
    observed: dict[str, str] = {}
    real_replace = os.replace

    class StartedServer:
        started = True

        def run(self):
            observed["pid"] = pid_file.read_text(encoding="ascii")

    def capture_replace(source, destination):
        replacements.append((Path(source), Path(destination)))
        real_replace(source, destination)

    monkeypatch.setattr(runner, "create_server", lambda *_args, **_kwargs: StartedServer())
    monkeypatch.setattr(os, "replace", capture_replace)

    try:
        exit_code = runner.main(
            ["probe:app", "--port", "8800", "--pid-file", str(pid_file)]
        )
    except SystemExit as exc:
        pytest.fail(f"service runner rejected --pid-file: {exc}")

    assert exit_code == 0
    assert observed == {"pid": str(os.getpid())}
    assert replacements and replacements[0][0] != replacements[0][1] == pid_file
    assert not pid_file.exists()


def test_service_runner_keeps_pid_file_replaced_by_new_owner(monkeypatch, tmp_path):
    runner = importlib.import_module("app.service_runner")
    pid_file = tmp_path / "backend-api.pid"

    class ReplacedOwnerServer:
        started = True

        def run(self):
            assert pid_file.read_text(encoding="ascii") == str(os.getpid())
            replacement = pid_file.with_suffix(".replacement")
            replacement.write_text("999999", encoding="ascii")
            os.replace(replacement, pid_file)

    monkeypatch.setattr(runner, "create_server", lambda *_args, **_kwargs: ReplacedOwnerServer())

    exit_code = runner.main(["probe:app", "--port", "8800", "--pid-file", str(pid_file)])

    assert exit_code == 0
    assert pid_file.read_text(encoding="ascii") == "999999"


def test_service_runner_exclusively_owns_pid_file_across_processes(tmp_path):
    probe_script = tmp_path / "runner_lock_probe.py"
    pid_file = tmp_path / "backend-api.pid"
    ready_file = tmp_path / "first-ready"
    release_file = tmp_path / "release-first"
    second_ran_file = tmp_path / "second-ran"
    probe_script.write_text(
        """
import sys
import time
from pathlib import Path

import app.service_runner as runner

mode, pid_name, ready_name, release_name, ran_name = sys.argv[1:]
pid_file = Path(pid_name)
ready_file = Path(ready_name)
release_file = Path(release_name)
ran_file = Path(ran_name)

class ProbeServer:
    started = True

    def run(self):
        if mode == "hold":
            ready_file.write_text("ready", encoding="ascii")
            while not release_file.exists():
                time.sleep(0.02)
        else:
            ran_file.write_text("ran", encoding="ascii")

runner.create_server = lambda *_args, **_kwargs: ProbeServer()
raise SystemExit(
    runner.main(["probe:app", "--port", "1", "--pid-file", str(pid_file)])
)
""",
        encoding="ascii",
    )
    command = [
        sys.executable,
        str(probe_script),
        "hold",
        str(pid_file),
        str(ready_file),
        str(release_file),
        str(second_ran_file),
    ]
    child_environment = os.environ.copy()
    child_environment["PYTHONPATH"] = os.pathsep.join(
        filter(None, (str(ROOT / "backend"), child_environment.get("PYTHONPATH")))
    )
    first = subprocess.Popen(command, cwd=ROOT / "backend", env=child_environment)
    try:
        deadline = time.monotonic() + 10
        while not ready_file.exists() and time.monotonic() < deadline:
            assert first.poll() is None, "first service runner exited before acquiring ownership"
            time.sleep(0.02)
        assert ready_file.exists(), "first service runner did not acquire ownership"
        first_pid = pid_file.read_text(encoding="ascii")
        assert first_pid.isdigit()
        first_service_pid = int(first_pid)

        contender = subprocess.run(
            [*command[:2], "once", *command[3:]],
            cwd=ROOT / "backend",
            env=child_environment,
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )

        assert contender.returncode != 0
        assert first.poll() is None
        assert pid_file.read_text(encoding="ascii") == first_pid
        assert not second_ran_file.exists()

        os.kill(first_service_pid, signal.SIGTERM)
        first.wait(timeout=10)
        replacement = subprocess.run(
            [*command[:2], "once", *command[3:]],
            cwd=ROOT / "backend",
            env=child_environment,
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )

        assert replacement.returncode == 0, replacement.stdout + replacement.stderr
        assert second_ran_file.read_text(encoding="ascii") == "ran"
        assert not pid_file.exists()
    finally:
        if first.poll() is None:
            release_file.write_text("release", encoding="ascii")
            first.wait(timeout=10)


def _ps_quote(value: str | Path) -> str:
    return str(value).replace("'", "''")


def test_shutdown_identity_rejects_synthetic_other_project_commands():
    helper = ROOT / "tools" / "stop-gaoshouplatform-services.ps1"
    backend_pid_file = ROOT / ".runtime" / "backend-api.pid"
    valid_backend = (
        f'"{ROOT / "backend" / ".venv" / "Scripts" / "python.exe"}" '
        f'-m app.service_runner app.main:app --port 8800 --pid-file "{backend_pid_file}"'
    )
    other_backend = valid_backend.replace(str(ROOT), r"E:\OtherProject")
    valid_frontend = (
        f'"node.exe" "{ROOT / "frontend" / "node_modules" / "vite" / "bin" / "vite.js"}" '
        "preview --port 3511"
    )
    other_frontend = valid_frontend.replace(str(ROOT), r"E:\OtherProject")
    script = f"""
. '{_ps_quote(helper)}' -ProjectRoot '{_ps_quote(ROOT)}' -BackendPort 1 -SyncPort 2 -FrontendPort 3
if (-not (Test-ServiceCommandIdentity -CommandLine '{_ps_quote(valid_backend)}' -Application 'app.main:app' -PidFile '{_ps_quote(backend_pid_file)}')) {{ exit 11 }}
if (Test-ServiceCommandIdentity -CommandLine '{_ps_quote(other_backend)}' -Application 'app.main:app' -PidFile '{_ps_quote(backend_pid_file)}') {{ exit 12 }}
if (-not (Test-FrontendCommandIdentity -CommandLine '{_ps_quote(valid_frontend)}' -ProjectRoot '{_ps_quote(ROOT)}')) {{ exit 13 }}
if (Test-FrontendCommandIdentity -CommandLine '{_ps_quote(other_frontend)}' -ProjectRoot '{_ps_quote(ROOT)}') {{ exit 14 }}
"""

    result = subprocess.run(
        [
            "powershell",
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-Command",
            script,
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=20,
        check=False,
    )

    assert result.returncode == 0, result.stdout + result.stderr


def test_windows_scripts_request_bounded_graceful_shutdown_before_force_fallback():
    start_script = (ROOT / "tools" / "start-gaoshouplatform.bat").read_text(encoding="utf-8")
    stop_script = (ROOT / "tools" / "stop-gaoshouplatform.bat").read_text(encoding="utf-8")
    stop_helper = (ROOT / "tools" / "stop-gaoshouplatform-services.ps1").read_text(
        encoding="utf-8"
    )
    shutdown_scripts = stop_script + stop_helper

    assert "'-m','app.service_runner','app.sync_main:app'" in start_script
    assert "'-m','app.service_runner','app.main:app'" in start_script
    assert "'--pid-file','%ROOT%\\.runtime\\sync-service.pid'" in start_script
    assert "'--pid-file','%ROOT%\\.runtime\\backend-api.pid'" in start_script
    assert '-ProjectRoot "%ROOT%"' in start_script
    assert '-ProjectRoot "%ROOT%"' in stop_script
    assert "uvicorn app\\.(main|sync_main):app" not in start_script

    graceful_request = shutdown_scripts.index("/internal/shutdown")
    bounded_wait = shutdown_scripts.index("Stopwatch")
    force_fallback = shutdown_scripts.index("Stop-Process -Id $processId -Force")
    assert graceful_request < bounded_wait < force_fallback
    assert "X-Gaoshou-Process-ID" in shutdown_scripts
    assert "app\\.service_runner" in shutdown_scripts
    assert "Get-Content -LiteralPath $PidFile" in stop_helper
    assert "$revalidated = Get-ManagedServiceProcess" in stop_helper
    assert "frontend processes may be stopped immediately" in shutdown_scripts
    assert (
        "Backend/sync policy: graceful request, bounded wait, forced termination only as fallback."
        in stop_script
    )
    assert "Market radar realtime feed stopped with the backend API lifecycle" not in stop_script
    assert "miniQMT client is external and was left running" in stop_script


def test_market_radar_docs_match_current_reduced_emotion_and_route_contracts():
    docs = (ROOT / "docs" / "market-radar.md").read_text(encoding="utf-8")

    assert "`closed` | 市场休市或实时服务已明确停止" in docs
    assert "`market-radar-emotion-reduced-v1`" in docs
    assert "当前实现" in docs
    assert "`partial`" in docs
    assert "`label = null`" in docs
    assert "目标完整公式" in docs
    assert "GET/POST/PATCH/DELETE" not in docs
    assert "`/api/market-radar/rules/{rule_id}`" in docs
    assert "`/api/market-radar/alerts/{event_id}`" in docs
    assert "`/api/market-radar/refresh/{task_id}`" in docs


def test_alembic_config_uses_platform_path_separator():
    config = (ROOT / "backend" / "alembic.ini").read_text(encoding="utf-8")

    assert "path_separator = os" in config
