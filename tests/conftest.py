from __future__ import annotations

from pathlib import Path

import pytest


@pytest.fixture(autouse=True)
def isolate_test_environment(monkeypatch, tmp_path: Path):
    monkeypatch.setenv("DB_PATH", str(tmp_path / "test_pr_analysis.db"))
    monkeypatch.delenv("GITLAB_PROJECT_ID", raising=False)
    monkeypatch.delenv("GITLAB_PROJECT_IDS", raising=False)
    monkeypatch.delenv("GITLAB_GROUP_ID", raising=False)
    monkeypatch.delenv("GITLAB_GROUP_IDS", raising=False)
    monkeypatch.delenv("GITLAB_BASE_URL", raising=False)
    monkeypatch.delenv("GITLAB_TOKEN", raising=False)
    monkeypatch.setenv("PRTOOL_ENV_FILE", str(tmp_path / ".nonexistent"))
