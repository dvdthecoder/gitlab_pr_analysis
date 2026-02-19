from __future__ import annotations

from pathlib import Path

from prtool import cli


def test_export_cli_project_scope(monkeypatch, capsys, tmp_path) -> None:
    calls: dict[str, list[int] | None] = {"csv": None, "jsonl": None}

    monkeypatch.setattr(cli, "_resolve_project_scope_ids", lambda args: [101, 202])

    def _fake_csv(db, out_dir="./exports", project_ids=None):
        calls["csv"] = project_ids
        p = Path(tmp_path / "mr_classification.csv")
        p.write_text("ok", encoding="utf-8")
        return p

    def _fake_jsonl(db, out_dir="./exports", project_ids=None):
        calls["jsonl"] = project_ids
        p = Path(tmp_path / "mr_classification.jsonl")
        p.write_text("ok", encoding="utf-8")
        return p

    monkeypatch.setattr(cli, "export_csv", _fake_csv)
    monkeypatch.setattr(cli, "export_jsonl", _fake_jsonl)

    rc = cli.main(["export", "--format", "both", "--project-id", "101"])
    out = capsys.readouterr().out

    assert rc == 0
    assert "Exported:" in out
    assert calls["csv"] == [101, 202]
    assert calls["jsonl"] == [101, 202]
