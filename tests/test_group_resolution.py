from __future__ import annotations

from prtool.config import resolve_group_ids


def test_resolve_group_ids_from_cli() -> None:
    assert resolve_group_ids(["abc/team", "123", "abc/team"]) == ["123", "abc/team"]


def test_resolve_group_ids_from_env_many(monkeypatch) -> None:
    monkeypatch.setenv("GITLAB_GROUP_IDS", "team/a,123,team/a")
    monkeypatch.delenv("GITLAB_GROUP_ID", raising=False)
    assert resolve_group_ids() == ["123", "team/a"]


def test_resolve_group_ids_from_env_one(monkeypatch) -> None:
    monkeypatch.delenv("GITLAB_GROUP_IDS", raising=False)
    monkeypatch.setenv("GITLAB_GROUP_ID", "team/a")
    assert resolve_group_ids() == ["team/a"]


def test_resolve_group_ids_empty(monkeypatch) -> None:
    monkeypatch.delenv("GITLAB_GROUP_IDS", raising=False)
    monkeypatch.delenv("GITLAB_GROUP_ID", raising=False)
    assert resolve_group_ids() == []
