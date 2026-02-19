from __future__ import annotations

import json

from prtool import cli


class _DummyClient:
    def __init__(self, _settings):
        pass

    def get_project_mr_count_all_states(self, project_id: int) -> int:
        return {10: 5, 20: 12, 30: 12}[project_id]


def test_projects_list_ranked_json(monkeypatch, capsys):
    monkeypatch.setattr(cli, "load_settings", lambda: object())
    monkeypatch.setattr(
        cli,
        "_resolve_discovery_projects",
        lambda args, settings: [
            {"id": 10, "path_with_namespace": "a/p10", "name": "p10"},
            {"id": 20, "path_with_namespace": "a/p20", "name": "p20"},
            {"id": 30, "path_with_namespace": "a/p30", "name": "p30"},
        ],
    )
    monkeypatch.setattr(cli, "GitLabSourceClient", _DummyClient)

    rc = cli.main(["projects", "list", "--all-projects", "--format", "json"])
    out = capsys.readouterr().out.strip()

    assert rc == 0
    payload = json.loads(out)
    assert [row["id"] for row in payload] == [20, 30, 10]
    assert [row["rank"] for row in payload] == [1, 2, 3]
    assert [row["mr_count_all_states"] for row in payload] == [12, 12, 5]
