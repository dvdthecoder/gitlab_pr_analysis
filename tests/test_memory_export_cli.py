from __future__ import annotations

from pathlib import Path

from prtool import cli


def test_memory_export_cli_project_scope(monkeypatch, capsys, tmp_path) -> None:
    calls: dict[str, object] = {}

    monkeypatch.setattr(cli, "_resolve_project_scope_ids", lambda args: [101, 202])

    def _fake_csv(db, out_dir="./exports", project_ids=None, filename_stem="mr_memory"):
        calls["csv_project_ids"] = project_ids
        calls["csv_stem"] = filename_stem
        p = Path(tmp_path / f"{filename_stem}.csv")
        p.write_text("ok", encoding="utf-8")
        return p

    def _fake_jsonl(db, out_dir="./exports", project_ids=None, filename_stem="mr_memory"):
        calls["jsonl_project_ids"] = project_ids
        calls["jsonl_stem"] = filename_stem
        p = Path(tmp_path / f"{filename_stem}.jsonl")
        p.write_text("ok", encoding="utf-8")
        return p

    monkeypatch.setattr(cli, "export_memory_csv", _fake_csv)
    monkeypatch.setattr(cli, "export_memory_jsonl", _fake_jsonl)

    rc = cli.main(["memory", "export", "--format", "both", "--project-id", "101"])
    out = capsys.readouterr().out

    assert rc == 0
    assert "Memory exported:" in out
    assert calls["csv_project_ids"] == [101, 202]
    assert calls["jsonl_project_ids"] == [101, 202]
    assert calls["csv_stem"] == "mr_memory_project_101"
    assert calls["jsonl_stem"] == "mr_memory_project_101"
