from __future__ import annotations

from prtool.config import PartialSettings
from prtool.db import Database
from prtool.seed_data import seed_demo_data
from prtool.viewer import (
    get_enrichment_rows,
    get_overview,
    get_project_compactions,
    get_project_ids,
    get_recent_rows,
    get_type_counts,
)


def _settings(db_path: str) -> PartialSettings:
    return PartialSettings(
        db_path=db_path,
        infra_ticket_regex=[r"INFRA-\d+", r"OPS-\d+"],
        infra_label_allowlist=["infra", "platform", "devops", "sre"],
        infra_keyword_list=["terraform", "k8s", "deployment", "infra", "docker"],
        infra_strong_threshold=4.0,
        infra_weak_threshold=1.5,
    )


def test_viewer_queries(tmp_path) -> None:
    db_path = str(tmp_path / "viewer.db")
    db = Database(db_path)
    seed_demo_data(db, project_id=9001, settings=_settings(db_path), run_classify=True)

    assert get_project_ids(db_path) == []
    projects = get_project_ids(db_path, data_source="test")
    assert projects == [9001]

    overview = get_overview(db_path, project_id=9001, data_source="test")
    assert overview["total_mrs"] == 4
    assert "avg_regression_probability" in overview

    type_counts = dict(get_type_counts(db_path, project_id=9001, data_source="test"))
    assert sum(type_counts.values()) == 4

    rows = get_recent_rows(db_path, project_id=9001, data_source="test", limit=10)
    assert len(rows) == 4
    assert {r["project_id"] for r in rows} == {9001}
    assert get_recent_rows(db_path, project_ids=[9001], data_source="test", limit=10)
    assert get_recent_rows(db_path, project_ids=[123456], data_source="test", limit=10) == []

    with db.connect() as conn:
        mr_id = conn.execute(
            "SELECT id FROM merge_requests WHERE project_id = ? ORDER BY iid LIMIT 1",
            (9001,),
        ).fetchone()["id"]
        db.upsert_qodo_describe(
            conn,
            {
                "mr_id": int(mr_id),
                "project_id": 9001,
                "mr_iid": 1,
                "markdown_path": "outputs/qodo/9001/1/describe.md",
                "content_sha256": "abc",
                "qodo_title": "Title",
                "qodo_type": "feature",
                "qodo_summary": "Summary",
                "qodo_sections": {"headings": ["Summary"]},
                "qodo_labels": ["feature"],
                "updated_at": "2026-02-18T00:00:00Z",
            },
        )
        db.upsert_project_qodo_compaction(
            conn,
            {
                "project_id": 9001,
                "compact_markdown_path": "outputs/qodo/9001/compact.md",
                "overview_mermaid_path": "outputs/qodo/9001/overview.mmd",
                "source_mr_count": 1,
                "content_sha256": "def",
                "updated_at": "2026-02-18T00:00:00Z",
            },
        )

        db.upsert_mr_memory_runtime(
            conn,
            {
                "mr_id": int(mr_id),
                "project_id": 9001,
                "mr_iid": 1,
                "mr_outcome": "above_baseline",
                "regression_probability": 0.7,
                "review_depth_required": "deep",
                "assessment_json": {"ok": True},
                "similar_mrs_json": [],
                "addendum_markdown_path": "outputs/memory/projects/9001/mrs/1/addendum.md",
                "context_markdown_path": "outputs/memory/projects/9001/mrs/1/context.md",
                "memory_score_version": "memory-v1",
                "content_sha256": "abc",
                "generated_at": "2026-02-18T00:00:00Z",
                "updated_at": "2026-02-18T00:00:00Z",
            },
        )

    filtered_rows = get_recent_rows(
        db_path,
        project_id=9001,
        data_source="test",
        mr_outcome="above_baseline",
        review_depth_required="deep",
        limit=10,
    )
    assert len(filtered_rows) == 1

    enrich_rows = get_enrichment_rows(db_path, project_id=9001, data_source="test", limit=10)
    assert len(enrich_rows) == 1
    assert enrich_rows[0]["qodo_title"] == "Title"

    compactions = get_project_compactions(db_path, project_id=9001, data_source="test", limit=10)
    assert len(compactions) == 1
    assert compactions[0]["source_mr_count"] == 1
