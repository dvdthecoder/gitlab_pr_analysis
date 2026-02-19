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
