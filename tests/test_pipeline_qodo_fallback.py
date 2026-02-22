from __future__ import annotations

import json
from datetime import datetime, timezone

from prtool.config import PartialSettings
from prtool.db import Database
from prtool.pipeline import classify_project


def _settings(db_path: str) -> PartialSettings:
    return PartialSettings(
        db_path=db_path,
        infra_ticket_regex=[r"INFRA-\\d+", r"OPS-\\d+"],
        infra_label_allowlist=["infra", "platform", "devops", "sre"],
        infra_keyword_list=["terraform", "k8s", "deployment", "infra", "docker"],
        infra_strong_threshold=4.0,
        infra_weak_threshold=1.5,
    )


def test_classify_uses_qodo_summary_when_description_missing(tmp_path) -> None:
    db_path = str(tmp_path / "qodo_fallback.db")
    db = Database(db_path)
    db.init_schema()

    now = datetime.now(timezone.utc).isoformat()
    project_id = 9911
    mr_id = 80001

    with db.connect() as conn:
        db.upsert_merge_request(
            conn,
            {
                "id": mr_id,
                "project_id": project_id,
                "iid": 12,
                "title": "Parser reliability tweaks",
                "description": "",
                "state": "opened",
                "author_username": "u",
                "labels": [],
                "web_url": "https://gitlab.example/mr/12",
                "created_at": now,
                "updated_at": now,
                "merged_at": None,
                "closed_at": None,
                "source_branch": "x",
                "target_branch": "main",
                "data_source": "test",
            },
        )
        db.replace_mr_files(
            conn,
            mr_id,
            [
                {
                    "new_path": "src/parser.py",
                    "additions": 11,
                    "deletions": 3,
                }
            ],
        )
        db.upsert_qodo_artifact(
            conn,
            {
                "mr_id": mr_id,
                "project_id": project_id,
                "mr_iid": 12,
                "tool": "describe",
                "markdown_path": "outputs/qodo/9911/12/describe.md",
                "content_sha256": "abc",
                "qodo_summary": "Fix null pointer bug in parser when header is missing.",
                "updated_at": now,
            },
        )

    processed = classify_project(db, _settings(db_path), project_id, only_stale=False)
    assert processed == 1

    with db.connect() as conn:
        class_row = conn.execute(
            """
            SELECT c.final_type, c.classifier_version
            FROM mr_classifications c
            WHERE c.mr_id = ?
            """,
            (mr_id,),
        ).fetchone()
        feat_row = conn.execute(
            "SELECT feature_json FROM mr_features WHERE mr_id = ?",
            (mr_id,),
        ).fetchone()

    assert class_row is not None
    assert class_row["final_type"] == "bugfix"
    assert class_row["classifier_version"] == "v2.8"

    assert feat_row is not None
    feat = json.loads(feat_row["feature_json"])
    assert feat["has_description"] is True


def test_classify_prefers_clean_reviewer_summary_over_raw_qodo_summary(tmp_path) -> None:
    db_path = str(tmp_path / "qodo_reviewer_summary.db")
    db = Database(db_path)
    db.init_schema()

    now = datetime.now(timezone.utc).isoformat()
    project_id = 9912
    mr_id = 80002

    with db.connect() as conn:
        db.upsert_merge_request(
            conn,
            {
                "id": mr_id,
                "project_id": project_id,
                "iid": 13,
                "title": "Address validation edge cases",
                "description": "",
                "state": "opened",
                "author_username": "u",
                "labels": [],
                "web_url": "https://gitlab.example/mr/13",
                "created_at": now,
                "updated_at": now,
                "merged_at": None,
                "closed_at": None,
                "source_branch": "x",
                "target_branch": "main",
                "data_source": "test",
            },
        )
        db.replace_mr_files(
            conn,
            mr_id,
            [
                {
                    "new_path": "src/address.ts",
                    "additions": 12,
                    "deletions": 4,
                }
            ],
        )
        db.upsert_qodo_artifact(
            conn,
            {
                "mr_id": mr_id,
                "project_id": project_id,
                "mr_iid": 13,
                "tool": "describe",
                "markdown_path": "outputs/qodo/9912/13/describe.md",
                "content_sha256": "abc",
                "qodo_summary": "@@ -1,2 +1,2 @@\n-old\n+new",
                "reviewer_summary": "Fixes address validation fallback to avoid invalid state/country selection.",
                "reviewer_summary_status": "clean",
                "updated_at": now,
            },
        )

    processed = classify_project(db, _settings(db_path), project_id, only_stale=False)
    assert processed == 1

    with db.connect() as conn:
        feat_row = conn.execute(
            "SELECT feature_json FROM mr_features WHERE mr_id = ?",
            (mr_id,),
        ).fetchone()

    assert feat_row is not None
    feat = json.loads(feat_row["feature_json"])
    assert feat["has_description"] is True
