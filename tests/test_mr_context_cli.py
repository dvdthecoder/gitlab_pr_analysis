from __future__ import annotations

from pathlib import Path

from prtool import cli
from prtool.db import Database


def _seed_minimal_mr(db_path: Path) -> None:
    db = Database(str(db_path))
    db.init_schema()
    with db.connect() as conn:
        conn.execute(
            """
            INSERT INTO merge_requests (
              id, project_id, iid, title, description, state, author_username, labels_json, web_url,
              created_at, updated_at, merged_at, closed_at, source_branch, target_branch, data_source
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                1010,
                55,
                77,
                "Improve classifier confidence",
                "Adds better rationale and confidence calibration.",
                "merged",
                "dev1",
                '["infra","risk"]',
                "https://gitlab.example/org/repo/-/merge_requests/77",
                "2026-02-01T00:00:00+00:00",
                "2026-02-02T00:00:00+00:00",
                "2026-02-03T00:00:00+00:00",
                None,
                "feature/classifier-v2",
                "main",
                "production",
            ),
        )
        conn.execute(
            """
            INSERT INTO mr_features (
              mr_id, files_changed, additions, deletions, churn, commit_count,
              review_comment_count, review_thread_count, unresolved_thread_count,
              pipeline_failed_count, infra_ticket_match_count, infra_keyword_score,
              infra_label_match_count, infra_signal_score, infra_signal_level, feature_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (1010, 3, 120, 35, 155, 2, 4, 2, 1, 1, 0, 0.2, 1, 0.75, "high", "{}"),
        )
        conn.execute(
            """
            INSERT INTO mr_classifications (
              mr_id, base_type, final_type, is_infra_related, infra_override_applied,
              complexity_level, complexity_score, capability_tags_json, risk_tags_json,
              classification_confidence, confidence_band, needs_review, classifier_version,
              classification_rationale_json, classified_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                1010,
                "feature",
                "infra-change",
                1,
                1,
                "high",
                0.82,
                '["infra","ci"]',
                '["deploy"]',
                0.74,
                "low",
                1,
                "v2.7",
                '{"why_needs_review":["low_top2_margin"]}',
                "2026-02-02T00:00:00+00:00",
            ),
        )
        conn.execute(
            "INSERT INTO mr_discussions (mr_id, thread_count, note_count, unresolved_count) VALUES (?, ?, ?, ?)",
            (1010, 2, 4, 1),
        )
        conn.execute(
            "INSERT INTO mr_pipelines (mr_id, pipeline_count, failed_count, success_count, retry_count) VALUES (?, ?, ?, ?, ?)",
            (1010, 2, 1, 1, 1),
        )
        conn.execute(
            "INSERT INTO mr_files (mr_id, path, additions, deletions) VALUES (?, ?, ?, ?)",
            (1010, "prtool/classifier.py", 40, 10),
        )
        conn.execute(
            "INSERT INTO mr_commits (mr_id, commit_sha, title, authored_date) VALUES (?, ?, ?, ?)",
            (1010, "abcdef123456", "classifier: tune confidence", "2026-02-02T00:00:00+00:00"),
        )


def test_mr_context_writes_markdown_without_qodo(monkeypatch, tmp_path, capsys) -> None:
    db_path = tmp_path / "mr_context.db"
    monkeypatch.setattr(cli, "load_partial_settings", lambda: type("P", (), {"db_path": str(db_path)})())
    _seed_minimal_mr(db_path)

    out_path = tmp_path / "context.md"
    rc = cli.main(
        [
            "mr-context",
            "--project-id",
            "55",
            "--mr-iid",
            "77",
            "--out-path",
            str(out_path),
            "--no-qodo-inline",
            "--no-reclassify",
        ]
    )
    out = capsys.readouterr().out

    assert rc == 0
    assert "MR context written" in out
    text = out_path.read_text(encoding="utf-8")
    assert "MR Context: !77 Improve classifier confidence" in text
    assert "final_type: infra-change" in text
    assert "why_needs_review: low_top2_margin" in text


def test_mr_context_runs_qodo_and_reclassify(monkeypatch, tmp_path, capsys) -> None:
    db_path = tmp_path / "mr_context_actions.db"
    monkeypatch.setattr(cli, "load_partial_settings", lambda: type("P", (), {"db_path": str(db_path)})())
    _seed_minimal_mr(db_path)

    qodo_calls: list[tuple[int, list[dict[str, int]] | None]] = []
    reclassify_calls: list[tuple[int, list[int] | None]] = []

    def _fake_enrich(_db, project_id, _opts, candidates=None):
        qodo_calls.append((project_id, candidates))
        return {"eligible": 1, "success": 1, "failed": 0, "skipped": 0}

    def _fake_classify(_db, _partial, project_id, **kwargs):
        reclassify_calls.append((project_id, kwargs.get("mr_ids")))
        return 1

    monkeypatch.setattr(cli, "enrich_qodo_project", _fake_enrich)
    monkeypatch.setattr(cli, "compact_project_qodo", lambda *a, **k: {"compact_markdown_path": "/tmp/compact.md"})
    monkeypatch.setattr(cli, "classify_project", _fake_classify)

    rc = cli.main(["mr-context", "--project-id", "55", "--mr-iid", "77", "--qodo-inline", "--reclassify"])
    out = capsys.readouterr().out

    assert rc == 0
    assert "[mr-context] qodo tools=describe eligible=1 success=1 failed=0 skipped=0" in out
    assert "[mr-context] reclassified=1" in out
    assert qodo_calls and qodo_calls[0][0] == 55
    assert qodo_calls[0][1] and qodo_calls[0][1][0]["id"] == 1010
    assert reclassify_calls == [(55, [1010])]


def test_mr_context_suppresses_low_quality_qodo_and_hides_paths(monkeypatch, tmp_path, capsys) -> None:
    db_path = tmp_path / "mr_context_qodo_quality.db"
    monkeypatch.setattr(cli, "load_partial_settings", lambda: type("P", (), {"db_path": str(db_path)})())
    _seed_minimal_mr(db_path)

    db = Database(str(db_path))
    with db.connect() as conn:
        conn.execute(
            """
            INSERT INTO mr_qodo_artifacts (
              mr_id, project_id, mr_iid, tool, markdown_path, raw_output_path, content_sha256,
              qodo_title, qodo_type, qodo_summary, qodo_sections_json, qodo_labels_json,
              parser_version, quality_status, prompt_leak_count, prompt_leak_markers_json,
              structured_payload_json, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                1010,
                55,
                77,
                "describe",
                "/tmp/describe.md",
                "/tmp/raw.txt",
                "abc",
                "",
                "bugfix",
                "@@ -1,2 +1,2 @@\n-foo\n+bar",
                '{"summary":"@@ -10,2 +10,2 @@\\n-x\\n+y"}',
                '["@@ diff"]',
                "qodo-v2",
                "partial",
                2,
                '["system_prompt"]',
                '{"summary":"@@ -20,1 +20,1 @@\\n-a\\n+b"}',
                "2026-02-22T00:00:00+00:00",
            ),
        )
        conn.execute(
            """
            INSERT INTO mr_memory_runtime (
              mr_id, project_id, mr_iid, mr_outcome, mr_achieved_outcome, mr_achieved_outcome_bullets_json,
              outcome_source, outcome_mode, outcome_quality_score, topic_labels_json, similarity_strategy,
              regression_probability, review_depth_required, assessment_json, similar_mrs_json,
              addendum_markdown_path, context_markdown_path, memory_score_version, content_sha256, generated_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                1010,
                55,
                77,
                "at_baseline",
                "Outcome text",
                '["a"]',
                "heuristic",
                "template",
                0.9,
                '["chore"]',
                "lexical",
                0.3,
                "standard",
                "{}",
                "[]",
                "outputs/memory/projects/55/mrs/77/addendum.md",
                "outputs/memory/projects/55/mrs/77/context.md",
                "memory-v1",
                "def",
                "2026-02-22T00:00:00+00:00",
                "2026-02-22T00:00:00+00:00",
            ),
        )

    out_path = tmp_path / "context_quality.md"
    rc = cli.main(
        [
            "mr-context",
            "--project-id",
            "55",
            "--mr-iid",
            "77",
            "--out-path",
            str(out_path),
            "--no-qodo-inline",
            "--no-reclassify",
        ]
    )
    _ = capsys.readouterr().out
    assert rc == 0

    text = out_path.read_text(encoding="utf-8")
    assert "summary: (suppressed due to low-quality/parsing artifacts)" in text
    assert "labels: (suppressed due to low-quality/parsing artifacts)" in text
    assert "markdown_path:" not in text
    assert "addendum_markdown_path:" not in text
    assert "context_markdown_path:" not in text
    assert "qodo_low_quality_or_prompt_leak" in text
    assert "classifier_memory_type_mismatch" in text
