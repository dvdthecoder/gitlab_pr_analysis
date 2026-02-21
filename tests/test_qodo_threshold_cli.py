from __future__ import annotations

from prtool import cli


class _DB:
    def __init__(self, _):
        self.initialized = False

    def init_schema(self):
        self.initialized = True

    def connect(self):
        raise AssertionError("connect should not be used in this test")


def test_qodo_threshold_dry_run_only_selects(monkeypatch, capsys) -> None:
    monkeypatch.setattr(cli, "load_partial_settings", lambda: type("P", (), {"db_path": ":memory:"})())
    monkeypatch.setattr(cli, "Database", _DB)
    monkeypatch.setattr(cli, "_resolve_project_scope_ids", lambda args: [101])
    monkeypatch.setattr(
        cli,
        "_needs_review_stats",
        lambda db, project_ids, data_source: {"total": 10, "needs_review": 5, "needs_review_pct": 50.0},
    )
    monkeypatch.setattr(
        cli,
        "_select_qodo_threshold_candidates",
        lambda db, **kwargs: [
            {
                "id": 10001,
                "project_id": 101,
                "iid": 77,
                "web_url": "https://gitlab.example/mr/77",
                "updated_at": "2026-02-20T00:00:00+00:00",
                "final_type": "feature",
                "classification_confidence": 0.74,
                "needs_review": 1,
                "classifier_version": "v2.8",
                "has_empty_description": 1,
            }
        ],
    )

    enrich_calls = []
    class_calls = []
    monkeypatch.setattr(cli, "enrich_qodo_project", lambda *a, **k: enrich_calls.append((a, k)))
    monkeypatch.setattr(cli, "classify_project", lambda *a, **k: class_calls.append((a, k)))

    rc = cli.main(["enrich", "qodo-threshold", "--project-id", "101", "--dry-run"])
    out = capsys.readouterr().out

    assert rc == 0
    assert "selected=1" in out
    assert "Dry-run only" in out
    assert not enrich_calls
    assert not class_calls


def test_qodo_threshold_runs_enrich_then_targeted_reclassify(monkeypatch, capsys) -> None:
    monkeypatch.setattr(cli, "load_partial_settings", lambda: type("P", (), {"db_path": ":memory:"})())
    monkeypatch.setattr(cli, "Database", _DB)
    monkeypatch.setattr(cli, "_resolve_project_scope_ids", lambda args: [101, 102])

    stats_calls = []

    def _fake_stats(db, project_ids, data_source):
        stats_calls.append((tuple(project_ids), data_source))
        if len(stats_calls) == 1:
            return {"total": 20, "needs_review": 10, "needs_review_pct": 50.0}
        return {"total": 20, "needs_review": 8, "needs_review_pct": 40.0}

    monkeypatch.setattr(cli, "_needs_review_stats", _fake_stats)
    monkeypatch.setattr(
        cli,
        "_select_qodo_threshold_candidates",
        lambda db, **kwargs: [
            {
                "id": 20001,
                "project_id": 101,
                "iid": 11,
                "web_url": "https://gitlab.example/mr/11",
                "updated_at": "2026-02-20T00:00:00+00:00",
                "final_type": "bugfix",
                "classification_confidence": 0.72,
                "needs_review": 1,
                "classifier_version": "v2.8",
                "has_empty_description": 1,
            },
            {
                "id": 20002,
                "project_id": 101,
                "iid": 12,
                "web_url": "https://gitlab.example/mr/12",
                "updated_at": "2026-02-20T00:00:00+00:00",
                "final_type": "feature",
                "classification_confidence": 0.71,
                "needs_review": 1,
                "classifier_version": "v2.8",
                "has_empty_description": 0,
            },
        ],
    )

    monkeypatch.setattr(cli, "enrich_qodo_project", lambda db, project_id, opts, candidates=None: {"eligible": len(candidates or []), "success": len(candidates or []), "failed": 0, "skipped": 0})
    monkeypatch.setattr(cli, "compact_project_qodo", lambda db, project_id, opts: {"compact_markdown_path": f"/tmp/{project_id}.md"})

    class_calls = []

    def _fake_classify(db, partial, project_id, **kwargs):
        class_calls.append((project_id, kwargs))
        return len(kwargs.get("mr_ids") or [])

    monkeypatch.setattr(cli, "classify_project", _fake_classify)

    class _Conn:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, sql, params=()):
            class _Cur:
                def fetchall(self_nonlocal):
                    return [
                        {"mr_id": 20001, "classification_confidence": 0.78, "needs_review": 0},
                        {"mr_id": 20002, "classification_confidence": 0.74, "needs_review": 1},
                    ]

            return _Cur()

    monkeypatch.setattr(_DB, "connect", lambda self: _Conn())

    rc = cli.main(["enrich", "qodo-threshold", "--project-id", "101"])
    out = capsys.readouterr().out

    assert rc == 0
    assert "Threshold enrich total" in out
    assert "delta=-10.00pp" in out
    assert len(class_calls) == 1
    assert class_calls[0][0] == 101
    assert class_calls[0][1]["mr_ids"] == [20001, 20002]
