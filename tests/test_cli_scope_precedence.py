from __future__ import annotations

from argparse import Namespace

from prtool import cli


def test_project_scope_prefers_explicit_project_ids(monkeypatch) -> None:
    monkeypatch.setattr(cli, "resolve_group_ids", lambda _: ["env/group"])
    monkeypatch.setattr(cli, "resolve_project_ids", lambda pids=None: sorted(set(pids or [])))

    called = {"discovery": False}

    def _fail_discovery(_args, _settings):
        called["discovery"] = True
        return []

    monkeypatch.setattr(cli, "_resolve_discovery_projects", _fail_discovery)

    args = Namespace(
        project_id=[30570685],
        group_id=None,
        all_projects=False,
        project_start_index=1,
        project_count=None,
    )
    ids = cli._resolve_project_scope_ids(args)
    assert ids == [30570685]
    assert called["discovery"] is False


def test_sync_scope_prefers_explicit_project_ids(monkeypatch) -> None:
    monkeypatch.setattr(cli, "resolve_project_ids", lambda pids=None: sorted(set(pids or [])))

    called = {"discovery": False}

    def _fail_discovery(_args, _settings):
        called["discovery"] = True
        return []

    monkeypatch.setattr(cli, "_resolve_discovery_projects", _fail_discovery)

    args = Namespace(
        project_id=[1, 2],
        project_start_index=1,
        project_count=None,
    )
    ids = cli._resolve_sync_project_ids(args, settings=object())
    assert ids == [1, 2]
    assert called["discovery"] is False

