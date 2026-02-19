from __future__ import annotations

import os

from prtool.config import resolve_project_ids


def test_resolve_project_ids_from_cli_overrides(monkeypatch) -> None:
    monkeypatch.setenv("GITLAB_PROJECT_IDS", "10,11")
    assert resolve_project_ids([3, 2, 3]) == [2, 3]


def test_resolve_project_ids_from_many_env(monkeypatch) -> None:
    monkeypatch.delenv("GITLAB_PROJECT_ID", raising=False)
    monkeypatch.setenv("GITLAB_PROJECT_IDS", "10, 11,10")
    assert resolve_project_ids() == [10, 11]


def test_resolve_project_ids_from_single_env(monkeypatch) -> None:
    monkeypatch.delenv("GITLAB_PROJECT_IDS", raising=False)
    monkeypatch.setenv("GITLAB_PROJECT_ID", "42")
    assert resolve_project_ids() == [42]


def test_resolve_project_ids_raises_when_missing(monkeypatch) -> None:
    monkeypatch.delenv("GITLAB_PROJECT_IDS", raising=False)
    monkeypatch.delenv("GITLAB_PROJECT_ID", raising=False)
    try:
        resolve_project_ids()
        assert False, "Expected ValueError"
    except ValueError as exc:
        assert "Provide --project-id" in str(exc)
