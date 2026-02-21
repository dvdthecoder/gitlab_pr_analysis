from __future__ import annotations

from prtool import cli


def test_reclassify_only_stale_default(monkeypatch, capsys) -> None:
    monkeypatch.setattr(cli, "load_partial_settings", lambda: type("P", (), {"db_path": ":memory:"})())

    class _DB:
        def __init__(self, _):
            pass

        def init_schema(self):
            return None

        def connect(self):
            raise AssertionError("connect should not be used in this test")

    monkeypatch.setattr(cli, "Database", _DB)
    monkeypatch.setattr(cli, "_resolve_classify_project_ids", lambda args, db: [1])

    calls = []

    def _fake_classify(db, partial, project_id, **kwargs):
        calls.append((project_id, kwargs))
        return 3

    monkeypatch.setattr(cli, "classify_project", _fake_classify)

    rc = cli.main(["reclassify", "--project-id", "1"])
    out = capsys.readouterr().out

    assert rc == 0
    assert "mode=only-stale" in out
    assert calls[0][1]["only_stale"] is True


def test_reclassify_force_overrides_only_stale(monkeypatch, capsys) -> None:
    monkeypatch.setattr(cli, "load_partial_settings", lambda: type("P", (), {"db_path": ":memory:"})())

    class _DB:
        def __init__(self, _):
            pass

        def init_schema(self):
            return None

        def connect(self):
            raise AssertionError("connect should not be used in this test")

    monkeypatch.setattr(cli, "Database", _DB)
    monkeypatch.setattr(cli, "_resolve_classify_project_ids", lambda args, db: [2])

    calls = []

    def _fake_classify(db, partial, project_id, **kwargs):
        calls.append((project_id, kwargs))
        return 5

    monkeypatch.setattr(cli, "classify_project", _fake_classify)

    rc = cli.main(["reclassify", "--project-id", "2", "--force"])
    out = capsys.readouterr().out

    assert rc == 0
    assert "mode=force-all" in out
    assert calls[0][1]["only_stale"] is False


def test_reclassify_qodo_inline_runs_before_classify(monkeypatch, capsys) -> None:
    monkeypatch.setattr(cli, "load_partial_settings", lambda: type("P", (), {"db_path": ":memory:"})())

    class _DB:
        def __init__(self, _):
            pass

        def init_schema(self):
            return None

    monkeypatch.setattr(cli, "Database", _DB)
    monkeypatch.setattr(cli, "_resolve_classify_project_ids", lambda args, db: [7])
    monkeypatch.setattr(
        cli,
        "_select_qodo_threshold_candidates",
        lambda db, **kwargs: [
            {
                "id": 701,
                "project_id": 7,
                "iid": 11,
                "web_url": "https://gitlab.example/mr/11",
            }
        ],
    )

    enrich_calls = []
    monkeypatch.setattr(
        cli,
        "enrich_qodo_project",
        lambda db, project_id, opts, candidates=None: enrich_calls.append((project_id, len(candidates or []))) or {"eligible": 1, "success": 1, "failed": 0, "skipped": 0},
    )
    monkeypatch.setattr(cli, "compact_project_qodo", lambda db, project_id, opts: {"compact_markdown_path": "/tmp/compact.md"})

    class_calls = []

    def _fake_classify(db, partial, project_id, **kwargs):
        class_calls.append((project_id, kwargs))
        return 2

    monkeypatch.setattr(cli, "classify_project", _fake_classify)

    rc = cli.main(["reclassify", "--project-id", "7", "--force", "--qodo-inline"])
    out = capsys.readouterr().out

    assert rc == 0
    assert "qodo_inline=True" in out
    assert "[qodo-inline] selected=1" in out
    assert enrich_calls == [(7, 1)]
    assert class_calls[0][1]["only_stale"] is False
