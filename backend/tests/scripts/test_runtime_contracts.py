from __future__ import annotations

from pathlib import Path

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
    sync_start = start_script.index("uvicorn','app.sync_main:app")
    backend_start = start_script.index("uvicorn','app.main:app")

    assert migration < sync_start < backend_start
    assert "QMT_ACCOUNT_MASK" in start_script
    assert "account %QMT_ACCOUNT_ID%" not in start_script


def test_alembic_config_uses_platform_path_separator():
    config = (ROOT / "backend" / "alembic.ini").read_text(encoding="utf-8")

    assert "path_separator = os" in config
