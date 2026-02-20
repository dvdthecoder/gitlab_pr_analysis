from __future__ import annotations

from prtool.db import Database


def test_schema_contains_infra_columns(tmp_path) -> None:
    db = Database(str(tmp_path / "t.db"))
    db.init_schema()

    with db.connect() as conn:
        feature_cols = {r[1] for r in conn.execute("PRAGMA table_info(mr_features)").fetchall()}
        class_cols = {r[1] for r in conn.execute("PRAGMA table_info(mr_classifications)").fetchall()}
        mr_cols = {r[1] for r in conn.execute("PRAGMA table_info(merge_requests)").fetchall()}
        qodo_cols = {r[1] for r in conn.execute("PRAGMA table_info(mr_qodo_describe)").fetchall()}
        comp_cols = {r[1] for r in conn.execute("PRAGMA table_info(project_qodo_compaction)").fetchall()}
        mem_base_cols = {r[1] for r in conn.execute("PRAGMA table_info(project_memory_baseline)").fetchall()}
        mem_runtime_cols = {r[1] for r in conn.execute("PRAGMA table_info(mr_memory_runtime)").fetchall()}

    assert "infra_ticket_match_count" in feature_cols
    assert "infra_keyword_score" in feature_cols
    assert "infra_label_match_count" in feature_cols
    assert "infra_signal_score" in feature_cols
    assert "infra_signal_level" in feature_cols

    assert "base_type" in class_cols
    assert "final_type" in class_cols
    assert "is_infra_related" in class_cols
    assert "infra_override_applied" in class_cols
    assert "classification_rationale_json" in class_cols
    assert "capability_tags_json" in class_cols
    assert "risk_tags_json" in class_cols
    assert "classification_confidence" in class_cols
    assert "classifier_version" in class_cols
    assert "data_source" in mr_cols
    assert "markdown_path" in qodo_cols
    assert "qodo_type" in qodo_cols
    assert "compact_markdown_path" in comp_cols
    assert "baseline_json" in mem_base_cols
    assert "markdown_path" in mem_base_cols
    assert "mr_outcome" in mem_runtime_cols
    assert "regression_probability" in mem_runtime_cols
    assert "review_depth_required" in mem_runtime_cols
