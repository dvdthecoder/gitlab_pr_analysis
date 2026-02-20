from __future__ import annotations

from datetime import datetime, timezone

from prtool.db import Database
from prtool.memory import (
    BaselineBuildOptions,
    MRBuildOptions,
    MaterializeOptions,
    build_project_baseline,
    build_runtime_for_project,
    get_memory_status,
    materialize_project_markdown_from_db,
)


def _mr(project_id: int, iid: int, mr_id: int) -> dict:
    now = datetime.now(timezone.utc).isoformat()
    return {
        "id": mr_id,
        "project_id": project_id,
        "iid": iid,
        "title": f"Enable redis cache for flow {iid}",
        "description": "update auth/session code paths",
        "state": "opened",
        "author_username": "u",
        "labels": [],
        "web_url": f"https://gitlab.com/Cimpress-Technology/boxup/repo/-/merge_requests/{iid}",
        "created_at": now,
        "updated_at": now,
        "merged_at": None,
        "closed_at": None,
        "source_branch": "a",
        "target_branch": "main",
        "data_source": "production",
    }


def _class() -> dict:
    return {
        "base_type": "feature",
        "final_type": "infra",
        "is_infra_related": True,
        "infra_override_applied": True,
        "complexity_level": "High",
        "complexity_score": 7.2,
        "capability_tags": ["infra", "redis"],
        "risk_tags": ["operational"],
        "classification_confidence": 0.8,
        "classifier_version": "v2.3",
        "rationale": {"ok": True},
        "classified_at": datetime.now(timezone.utc).isoformat(),
    }


def _feature() -> dict:
    return {
        "files_changed": 10,
        "additions": 200,
        "deletions": 45,
        "churn": 245,
        "commit_count": 3,
        "review_comment_count": 2,
        "review_thread_count": 1,
        "unresolved_thread_count": 1,
        "pipeline_failed_count": 1,
        "infra_ticket_match_count": 1,
        "infra_keyword_score": 2.0,
        "infra_label_match_count": 1,
        "infra_signal_score": 4.5,
        "infra_signal_level": "strong",
    }


def test_memory_runtime_and_baseline(tmp_path) -> None:
    db = Database(str(tmp_path / "memory.db"))
    db.init_schema()
    with db.connect() as conn:
        for iid in (1, 2, 3):
            mr_id = 1000 + iid
            db.upsert_merge_request(conn, _mr(1234, iid, mr_id))
            db.upsert_classification(conn, mr_id, _class())
            db.upsert_feature_row(conn, mr_id, _feature())

    baseline = build_project_baseline(
        db,
        1234,
        BaselineBuildOptions(output_root=str(tmp_path / "outputs"), data_source="production", history_window_months=12),
    )
    assert baseline["sample_size"] >= 3

    result = build_runtime_for_project(
        db,
        1234,
        MRBuildOptions(
            output_root=str(tmp_path / "outputs"),
            data_source="production",
            include_similar_limit=2,
            compose=True,
            only_missing=True,
            force=False,
        ),
    )
    assert result["eligible"] == 3
    assert result["success"] == 3

    status = get_memory_status(db, [1234], data_source="production")
    assert len(status) == 1
    assert int(status[0]["scored"]) == 3


def test_memory_db_only_mode(tmp_path) -> None:
    db = Database(str(tmp_path / "memory_db_only.db"))
    db.init_schema()
    with db.connect() as conn:
        for iid in (1, 2):
            mr_id = 2000 + iid
            db.upsert_merge_request(conn, _mr(5678, iid, mr_id))
            db.upsert_classification(conn, mr_id, _class())
            db.upsert_feature_row(conn, mr_id, _feature())

    out_root = tmp_path / "outputs"
    baseline = build_project_baseline(
        db,
        5678,
        BaselineBuildOptions(
            output_root=str(out_root),
            data_source="production",
            history_window_months=12,
            db_only=True,
        ),
    )
    assert baseline["sample_size"] >= 2
    assert not (out_root / "projects" / "5678" / "project_memory_5678.md").exists()

    result = build_runtime_for_project(
        db,
        5678,
        MRBuildOptions(
            output_root=str(out_root),
            data_source="production",
            include_similar_limit=2,
            compose=True,
            only_missing=True,
            force=False,
            db_only=True,
        ),
    )
    assert result["success"] == 2
    assert not (out_root / "projects" / "5678" / "mrs" / "1" / "addendum.md").exists()
    assert not (out_root / "projects" / "5678" / "mrs" / "1" / "context.md").exists()

    with db.connect() as conn:
        scored = conn.execute(
            "SELECT COUNT(*) as cnt FROM mr_memory_runtime WHERE project_id = ?",
            (5678,),
        ).fetchone()["cnt"]
    assert int(scored) == 2


def test_memory_materialize_from_db(tmp_path) -> None:
    db = Database(str(tmp_path / "materialize.db"))
    db.init_schema()
    with db.connect() as conn:
        for iid in (1, 2):
            mr_id = 3000 + iid
            db.upsert_merge_request(conn, _mr(9100, iid, mr_id))
            db.upsert_classification(conn, mr_id, _class())
            db.upsert_feature_row(conn, mr_id, _feature())

    out_root = tmp_path / "outputs"
    build_project_baseline(
        db,
        9100,
        BaselineBuildOptions(output_root=str(out_root), data_source="production", history_window_months=12, db_only=True),
    )
    build_runtime_for_project(
        db,
        9100,
        MRBuildOptions(
            output_root=str(out_root),
            data_source="production",
            include_similar_limit=2,
            compose=True,
            only_missing=True,
            force=False,
            db_only=True,
        ),
    )

    baseline_path = out_root / "projects" / "9100" / "project_memory_9100.md"
    addendum_path = out_root / "projects" / "9100" / "mrs" / "1" / "addendum.md"
    context_path = out_root / "projects" / "9100" / "mrs" / "1" / "context.md"
    assert not baseline_path.exists()
    assert not addendum_path.exists()
    assert not context_path.exists()

    result = materialize_project_markdown_from_db(
        db,
        9100,
        MaterializeOptions(
            output_root=str(out_root),
            data_source="production",
            compose=True,
            only_missing=True,
            force=False,
            mr_limit=None,
        ),
    )

    assert result["baseline_written"] == 1
    assert result["runtime_written"] == 2
    assert baseline_path.exists()
    assert addendum_path.exists()
    assert context_path.exists()

    rerun = materialize_project_markdown_from_db(
        db,
        9100,
        MaterializeOptions(
            output_root=str(out_root),
            data_source="production",
            compose=True,
            only_missing=True,
            force=False,
            mr_limit=None,
        ),
    )
    assert rerun["runtime_written"] == 0
    assert rerun["runtime_skipped"] >= 2
