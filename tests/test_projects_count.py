from __future__ import annotations

import json

from prtool import cli


def test_projects_count_all_projects_text(monkeypatch, capsys):
    monkeypatch.setattr(cli, "load_settings", lambda: object())
    monkeypatch.setattr(
        cli,
        "_resolve_count_scope",
        lambda args, settings: ("all-projects", [1, 2, 3], []),
    )

    rc = cli.main(["projects", "count", "--all-projects"])
    out = capsys.readouterr().out

    assert rc == 0
    assert "total_projects: 3" in out
    assert "scope: all-projects" in out


def test_projects_count_groups_json_include_ids(monkeypatch, capsys):
    monkeypatch.setattr(cli, "load_settings", lambda: object())
    monkeypatch.setattr(
        cli,
        "_resolve_count_scope",
        lambda args, settings: ("groups", [101, 102, 103], ["org/a", "org/b"]),
    )

    rc = cli.main(["projects", "count", "--group-id", "org/a", "--format", "json", "--include-ids"])
    out = capsys.readouterr().out.strip()

    assert rc == 0
    payload = json.loads(out)
    assert payload["total_projects"] == 3
    assert payload["scope"] == "groups"
    assert payload["groups"] == ["org/a", "org/b"]
    assert payload["project_ids"] == [101, 102, 103]


def test_projects_count_configured_scope(monkeypatch, capsys):
    monkeypatch.setattr(cli, "load_settings", lambda: object())
    monkeypatch.setattr(
        cli,
        "_resolve_count_scope",
        lambda args, settings: ("configured-project-ids", [5, 6], []),
    )

    rc = cli.main(["projects", "count"])
    out = capsys.readouterr().out

    assert rc == 0
    assert "total_projects: 2" in out
    assert "scope: configured-project-ids" in out
