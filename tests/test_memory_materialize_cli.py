from __future__ import annotations

from prtool import cli


def test_memory_materialize_cli(monkeypatch, capsys) -> None:
    monkeypatch.setattr(cli, "_resolve_project_scope_ids", lambda args: [111, 222])

    calls: list[tuple[int, object]] = []

    def _fake_materialize(db, project_id: int, opts):
        calls.append((project_id, opts))
        return {
            "project_id": project_id,
            "baseline_written": 1,
            "runtime_eligible": 3,
            "runtime_written": 2,
            "runtime_skipped": 1,
        }

    monkeypatch.setattr(cli, "materialize_project_markdown_from_db", _fake_materialize)

    rc = cli.main(["memory", "materialize", "--project-id", "111", "--mr-limit", "3"])
    out = capsys.readouterr().out

    assert rc == 0
    assert "Selected projects (2): [111, 222]" in out
    assert "Materialize total:" in out
    assert [c[0] for c in calls] == [111, 222]
