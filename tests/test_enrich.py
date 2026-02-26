from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from prtool import cli
from prtool.db import Database
from prtool.enrich import (
    CandidateOptions,
    EnrichOptions,
    _extract_ai_response,
    _extract_ai_response_blocks,
    _parse_yaml_payload,
    _post_process_for_tool,
    _redact_secrets,
    compact_project_qodo,
    enrich_qodo_project,
    select_enrich_candidates,
)


def _mr(project_id: int, iid: int, mr_id: int) -> dict:
    now = datetime.now(timezone.utc).isoformat()
    return {
        "id": mr_id,
        "project_id": project_id,
        "iid": iid,
        "title": f"MR {iid}",
        "description": "",
        "state": "opened",
        "author_username": "u",
        "labels": [],
        "web_url": f"https://gitlab.com/org/repo/-/merge_requests/{iid}",
        "created_at": now,
        "updated_at": now,
        "merged_at": None,
        "closed_at": None,
        "source_branch": "a",
        "target_branch": "main",
        "data_source": "production",
    }


def _class(mr_id: int, final_type: str, complexity_score: float) -> dict:
    now = datetime.now(timezone.utc).isoformat()
    return {
        "base_type": final_type,
        "final_type": final_type,
        "is_infra_related": False,
        "infra_override_applied": False,
        "complexity_level": "high" if complexity_score >= 7 else "medium",
        "complexity_score": complexity_score,
        "rationale": {"test": True},
        "classified_at": now,
    }


def test_enrich_and_compact(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "enrich.db"
    db = Database(str(db_path))
    db.init_schema()

    with db.connect() as conn:
        db.upsert_merge_request(conn, _mr(42, 1, 42001))
        db.upsert_merge_request(conn, _mr(42, 2, 42002))

    monkeypatch.setenv("QODO_DESCRIBE_CMD", "echo '# Qodo Title for {mr_url}\n\n## Summary\nA bug fix.'")

    opts = EnrichOptions(output_root=str(tmp_path / "outputs"), concurrency=2, data_source="production")
    res = enrich_qodo_project(db, 42, opts)
    assert res["eligible"] == 2
    assert res["success"] == 2

    comp = compact_project_qodo(db, 42, opts)
    assert comp["source_mr_count"] == 2
    assert Path(comp["compact_markdown_path"]).exists()
    assert Path(comp["overview_mermaid_path"]).exists()

    with db.connect() as conn:
        qcount = conn.execute("SELECT COUNT(*) FROM mr_qodo_describe WHERE project_id = 42").fetchone()[0]
        rcount = conn.execute("SELECT COUNT(*) FROM mr_qodo_runs").fetchone()[0]

    assert qcount == 2
    assert rcount == 2


def test_enrich_strips_log_noise_from_markdown(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "enrich_noise.db"
    db = Database(str(db_path))
    db.init_schema()

    with db.connect() as conn:
        db.upsert_merge_request(conn, _mr(42, 3, 42003))

    noisy = (
        "printf '2026-02-18 11:43:26.682 | WARNING  | x:y:1 - log noise\\n"
        "# Clean Title\\n\\n## Summary\\nUseful content for {mr_url}.\\n'"
    )
    monkeypatch.setenv("QODO_DESCRIBE_CMD", noisy)

    opts = EnrichOptions(output_root=str(tmp_path / "outputs"), concurrency=1, data_source="production")
    res = enrich_qodo_project(db, 42, opts)
    assert res["success"] == 1

    md_path = Path(opts.output_root) / "42" / "3" / "describe.md"
    text = md_path.read_text(encoding="utf-8")
    assert "WARNING" not in text
    assert text.startswith("# Clean Title")


def test_extract_ai_response_from_log_block() -> None:
    raw = (
        "2026-02-18 12:00:00.001 | INFO     | x:y:1 - start\n"
        "AI response:\n"
        "# Title\n\n## Summary\nBody.\n"
        "2026-02-18 12:00:01.001 | INFO     | x:y:2 - done\n"
    )
    out = _extract_ai_response(raw)
    assert out.startswith("# Title")
    assert "done" not in out


def test_enrich_persists_structured_quality_fields(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "enrich_structured.db"
    db = Database(str(db_path))
    db.init_schema()

    with db.connect() as conn:
        db.upsert_merge_request(conn, _mr(7, 11, 7011))

    monkeypatch.setenv(
        "QODO_DESCRIBE_CMD",
        "echo '# Example for {mr_url}\\n\\n## Summary\\nSystem prompt leaked line\\n\\n## Changes\\n- real change'",
    )

    opts = EnrichOptions(output_root=str(tmp_path / "outputs"), concurrency=1, data_source="production")
    res = enrich_qodo_project(db, 7, opts)
    assert res["success"] == 1

    with db.connect() as conn:
        row = conn.execute(
            """
            SELECT raw_output_path, parser_version, quality_status, prompt_leak_count, structured_payload_json
            FROM mr_qodo_describe WHERE project_id = 7 AND mr_iid = 11
            """
        ).fetchone()
    assert row is not None
    assert Path(str(row["raw_output_path"])).exists()
    assert row["parser_version"] == "qodo-v2"
    assert row["quality_status"] in {"ok", "partial"}
    assert int(row["prompt_leak_count"] or 0) >= 0
    assert "sections" in str(row["structured_payload_json"])


def test_parse_yaml_payload_from_ai_response_review() -> None:
    raw = """
2026-02-18 12:00:00.001 | INFO | x:y:1 - start
AI response:
review:
  estimated_effort_to_review_[1-5]: |
    1
  relevant_tests: |
    No
  key_issues_to_review: []
  security_concerns: |
    No
2026-02-18 12:00:01.001 | INFO | x:y:2 - done
"""
    parsed = _parse_yaml_payload(raw)
    assert parsed is not None
    assert parsed["title"] == "PR Review Summary"
    assert "findings" in parsed["sections"]


def test_parse_yaml_payload_from_ai_response_improve() -> None:
    raw = """
2026-02-18 12:00:00.001 | INFO | x:y:1 - start
AI response:
code_suggestions: []
2026-02-18 12:00:01.001 | INFO | x:y:2 - done
"""
    parsed = _parse_yaml_payload(raw)
    assert parsed is not None
    assert parsed["title"] == "Code Improvement Suggestions"
    assert "suggestions" in parsed["sections"]


def test_extract_ai_response_uses_last_block() -> None:
    raw = """
2026-02-18 12:00:00.001 | INFO | x:y:1 - start
AI response:
review:
  relevant_tests: |
    No
AI response:
code_suggestions:
  - suggestion_summary: |
      Use `some` instead of `find`.
    why: |
      Boolean intent is clearer.
2026-02-18 12:00:01.001 | INFO | x:y:2 - done
"""
    blocks = _extract_ai_response_blocks(raw)
    assert len(blocks) == 2
    assert "review:" in blocks[0]
    assert "code_suggestions:" in blocks[1]
    out = _extract_ai_response(raw)
    assert "code_suggestions:" in out
    assert "review:" not in out


def test_parse_yaml_payload_improve_suggestion_summary_shape() -> None:
    raw = """
AI response:
code_suggestions:
  - suggestion_summary: |
      Use `.some` instead of `.find` for existence checks.
    relevant_file: "src/a.ts"
    suggestion_score: 9
    why: |
      `.some` directly returns boolean and is clearer.
"""
    parsed = _parse_yaml_payload(raw)
    assert parsed is not None
    suggestions = str(parsed["sections"].get("suggestions") or "")
    assert "Use `.some` instead of `.find`" in suggestions
    assert parsed["summary"] is not None


def test_post_process_does_not_override_existing_tool_section() -> None:
    parsed = {
        "title": "PR Review Summary",
        "summary": "Review completed.",
        "sections": {"findings": "- Real finding from AI response."},
        "labels": ["review"],
    }
    noisy = "- In this format, we separated each hunk..."
    out = _post_process_for_tool(parsed, noisy, "review")
    assert "Real finding from AI response." in str(out["sections"].get("findings"))
    assert "In this format" not in str(out["sections"].get("findings"))


def test_select_enrich_candidates_stratified_soft_global(tmp_path) -> None:
    db = Database(str(tmp_path / "stratified.db"))
    db.init_schema()
    mr_types = [
        ("feature", 10.0),
        ("bugfix", 9.5),
        ("refactor", 9.2),
        ("chore", 8.9),
        ("feature", 8.5),
        ("feature", 8.1),
        ("bugfix", 7.9),
        ("perf-security", 7.7),
        ("feature", 7.5),
        ("feature", 7.4),
        ("feature", 7.2),
        ("docs-only", 7.0),
    ]
    with db.connect() as conn:
        for idx, (final_type, score) in enumerate(mr_types, start=1):
            mr_id = 90000 + idx
            db.upsert_merge_request(conn, _mr(101 if idx % 2 else 202, idx, mr_id))
            db.upsert_classification(conn, mr_id, _class(mr_id, final_type, score))

    opts = EnrichOptions(output_root=str(tmp_path / "out"), tools=("describe", "review"))
    c_opts = CandidateOptions(mode="stratified", count=10, scope="global", type_balance="soft", data_source="production")
    selected = select_enrich_candidates(db, [101, 202], opts, c_opts)
    assert len(selected) == 10
    assert len({int(row["mr_id"]) for row in selected}) == 10

    type_counts: dict[str, int] = {}
    for row in selected:
        final_type = str(row["final_type"])
        type_counts[final_type] = type_counts.get(final_type, 0) + 1
    assert max(type_counts.values()) <= 4

    seeded_types = {str(row["final_type"]) for row in selected[:4]}
    assert len(seeded_types) == 4


def test_select_enrich_candidates_respects_data_source_and_only_missing(tmp_path) -> None:
    db = Database(str(tmp_path / "stratified_filters.db"))
    db.init_schema()
    with db.connect() as conn:
        db.upsert_merge_request(conn, _mr(303, 1, 93001))
        db.upsert_merge_request(
            conn,
            {
                **_mr(303, 2, 93002),
                "data_source": "test",
            },
        )
        db.upsert_classification(conn, 93001, _class(93001, "feature", 9.9))
        db.upsert_classification(conn, 93002, _class(93002, "bugfix", 9.8))
        db.upsert_qodo_artifact(
            conn,
            {
                "mr_id": 93001,
                "project_id": 303,
                "mr_iid": 1,
                "tool": "describe",
                "markdown_path": str(tmp_path / "already.md"),
                "raw_output_path": None,
                "content_sha256": "x",
                "qodo_title": None,
                "qodo_type": None,
                "qodo_summary": None,
                "qodo_sections": {},
                "qodo_labels": [],
                "parser_version": "qodo-v2",
                "quality_status": "ok",
                "prompt_leak_count": 0,
                "prompt_leak_markers": [],
                "structured_payload": {},
                "updated_at": datetime.now(timezone.utc).isoformat(),
            },
        )

    opts = EnrichOptions(output_root=str(tmp_path / "out"), only_missing=True, force=False, tools=("describe",))
    c_opts = CandidateOptions(mode="stratified", count=10, scope="global", type_balance="soft", data_source="production")
    selected = select_enrich_candidates(db, [303], opts, c_opts)
    assert selected == []

    opts_force = EnrichOptions(output_root=str(tmp_path / "out"), only_missing=True, force=True, tools=("describe",))
    selected_force = select_enrich_candidates(db, [303], opts_force, c_opts)
    assert len(selected_force) == 1
    assert int(selected_force[0]["mr_id"]) == 93001


def test_cli_candidate_preview_does_not_execute(monkeypatch, capsys, tmp_path) -> None:
    db_path = tmp_path / "preview.db"
    db = Database(str(db_path))
    db.init_schema()
    with db.connect() as conn:
        for iid, score, t in [(1, 9.0, "feature"), (2, 8.0, "bugfix")]:
            mr_id = 97000 + iid
            db.upsert_merge_request(conn, _mr(505, iid, mr_id))
            db.upsert_classification(conn, mr_id, _class(mr_id, t, score))

    monkeypatch.setenv("DB_PATH", str(db_path))
    monkeypatch.setenv("GITLAB_PROJECT_ID", "505")
    monkeypatch.setenv("QODO_DESCRIBE_CMD", "python -c 'import sys; sys.exit(7)'")

    rc = cli.main(
        [
            "enrich",
            "qodo",
            "--project-id",
            "505",
            "--candidate-mode",
            "stratified",
            "--candidate-count",
            "2",
            "--candidate-preview",
        ]
    )
    out = capsys.readouterr().out
    assert rc == 0
    assert "Candidate selection: selected=2/2" in out
    assert "project_id\tmr_iid\tmr_id\tfinal_type\tcomplexity_score\tupdated_at\tweb_url" in out

    with db.connect() as conn:
        run_count = conn.execute("SELECT COUNT(*) FROM mr_qodo_runs").fetchone()[0]
    assert int(run_count) == 0


def test_redact_secrets_masks_pat_and_settings_line(monkeypatch) -> None:
    monkeypatch.setenv("GITLAB_TOKEN", "glpat-very-secret-token")
    raw = (
        'Updated setting GITLAB.PERSONAL_ACCESS_TOKEN to: "glpat-very-secret-token"\n'
        "--gitlab.personal_access_token=glpat-very-secret-token\n"
    )
    out = _redact_secrets(raw)
    assert "glpat-very-secret-token" not in out
    assert "[REDACTED]" in out


def test_redact_secrets_masks_openai_style_tokens(monkeypatch) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "sk-live-super-secret-key")
    raw = (
        "OPENAI_API_KEY=sk-live-super-secret-key\n"
        "Authorization: Bearer sk-live-super-secret-key\n"
        "payload key sk-proj-abcdef1234567890zzzz\n"
    )
    out = _redact_secrets(raw)
    assert "sk-live-super-secret-key" not in out
    assert "sk-proj-abcdef1234567890zzzz" not in out
    assert out.count("[REDACTED]") >= 2


def test_enrich_raw_log_is_redacted(monkeypatch, tmp_path) -> None:
    db = Database(str(tmp_path / "redact.db"))
    db.init_schema()
    with db.connect() as conn:
        db.upsert_merge_request(conn, _mr(808, 1, 808001))
    monkeypatch.setenv("GITLAB_TOKEN", "glpat-super-secret")
    monkeypatch.setenv(
        "QODO_DESCRIBE_CMD",
        "printf 'Updated setting GITLAB.PERSONAL_ACCESS_TOKEN to: \"glpat-super-secret\"\\nfor {mr_url}\\n# T\\n\\n## Summary\\nOk\\n'",
    )
    opts = EnrichOptions(output_root=str(tmp_path / "out"), concurrency=1, data_source="production")
    res = enrich_qodo_project(db, 808, opts)
    assert res["success"] == 1
    raw_path = Path(opts.output_root) / "808" / "1" / "describe.raw.log"
    content = raw_path.read_text(encoding="utf-8")
    assert "glpat-super-secret" not in content
    assert "[REDACTED]" in content


def test_enrich_raw_log_redacts_openai_key(monkeypatch, tmp_path) -> None:
    db = Database(str(tmp_path / "redact_openai.db"))
    db.init_schema()
    with db.connect() as conn:
        db.upsert_merge_request(conn, _mr(809, 1, 809001))
    monkeypatch.setenv("OPENAI_API_KEY", "sk-sensitive-openai-key")
    monkeypatch.setenv(
        "QODO_DESCRIBE_CMD",
        "printf 'OPENAI_API_KEY=sk-sensitive-openai-key\\nfor {mr_url}\\n# T\\n\\n## Summary\\nOk\\n'",
    )
    opts = EnrichOptions(output_root=str(tmp_path / "out"), concurrency=1, data_source="production")
    res = enrich_qodo_project(db, 809, opts)
    assert res["success"] == 1
    raw_path = Path(opts.output_root) / "809" / "1" / "describe.raw.log"
    content = raw_path.read_text(encoding="utf-8")
    assert "sk-sensitive-openai-key" not in content
    assert "[REDACTED]" in content
