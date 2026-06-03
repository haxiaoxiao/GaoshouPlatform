from pathlib import Path

from app.scripts import update_akquant


def test_compare_versions_uses_numeric_release_parts():
    assert update_akquant.compare_versions("0.2.40", "0.2.39") == 1
    assert update_akquant.compare_versions("0.2.40", "0.2.40") == 0
    assert update_akquant.compare_versions("0.2.9", "0.2.10") == -1


def test_update_pyproject_dependency_rewrites_existing_requirement():
    text = 'dependencies = [\n    "akquant>=0.2.26",\n]\n'

    new_text, changed = update_akquant.update_pyproject_dependency(text, "akquant", "0.2.40")

    assert changed is True
    assert '"akquant>=0.2.40"' in new_text
    assert "0.2.26" not in new_text


def test_update_requirements_dependency_adds_missing_requirement():
    text = "duckdb>=1.0.0\npandas>=2.0.0\npyarrow>=15.0.0\nloguru>=0.7.0\n"

    new_text, changed = update_akquant.update_requirements_dependency(text, "akquant", "0.2.40")

    assert changed is True
    assert "loguru>=0.7.0\nakquant>=0.2.40\n" in new_text


def test_sync_dependency_files_updates_both_backend_files(tmp_path: Path):
    backend = tmp_path / "backend"
    backend.mkdir()
    (backend / "pyproject.toml").write_text(
        '[project]\ndependencies = [\n    "akquant>=0.2.26",\n    "tushare>=1.4.0",\n]\n',
        encoding="utf-8",
    )
    (backend / "requirements.txt").write_text(
        "duckdb>=1.0.0\nloguru>=0.7.0\n",
        encoding="utf-8",
    )

    updated = update_akquant.sync_dependency_files(tmp_path, "0.2.40")

    assert updated == [backend / "pyproject.toml", backend / "requirements.txt"]
    assert '"akquant>=0.2.40"' in (backend / "pyproject.toml").read_text(encoding="utf-8")
    assert "akquant>=0.2.40" in (backend / "requirements.txt").read_text(encoding="utf-8")
