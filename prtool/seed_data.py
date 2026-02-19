from __future__ import annotations

from datetime import datetime, timezone

from prtool.config import PartialSettings
from prtool.db import Database
from prtool.pipeline import classify_project


def seed_demo_data(db: Database, project_id: int, settings: PartialSettings, run_classify: bool = True) -> int:
    now = datetime.now(timezone.utc).isoformat()
    sample_rows = [
        {
            "id": project_id * 100000 + 1,
            "project_id": project_id,
            "iid": 1,
            "title": "INFRA-123 Add cluster deployment automation",
            "description": "terraform and k8s updates for deployment",
            "state": "merged",
            "author_username": "alice",
            "labels": ["infra", "automation"],
            "web_url": "https://example.local/mr/1",
            "created_at": now,
            "updated_at": now,
            "merged_at": now,
            "closed_at": None,
            "source_branch": "feature/infra-123-automation",
            "target_branch": "main",
            "data_source": "test",
            "commits": [
                {"id": "a1b2c3", "title": "add terraform module", "authored_date": now},
                {"id": "a1b2c4", "title": "wire deployment", "authored_date": now},
            ],
            "files": [
                {"new_path": "infra/main.tf", "additions": 120, "deletions": 5},
                {"new_path": "infra/helm/values.yaml", "additions": 40, "deletions": 2},
            ],
            "discussions": {"thread_count": 2, "note_count": 6, "unresolved_count": 0},
            "approvals": {"approvals_required": 1, "approvals_given": 1},
            "pipelines": {"pipeline_count": 2, "failed_count": 0, "success_count": 2, "retry_count": 0},
        },
        {
            "id": project_id * 100000 + 2,
            "project_id": project_id,
            "iid": 2,
            "title": "Fix null pointer in parser",
            "description": "touch deployment script for startup order",
            "state": "merged",
            "author_username": "bob",
            "labels": ["bug"],
            "web_url": "https://example.local/mr/2",
            "created_at": now,
            "updated_at": now,
            "merged_at": now,
            "closed_at": None,
            "source_branch": "bugfix/parser-null",
            "target_branch": "main",
            "data_source": "test",
            "commits": [{"id": "b1b2c3", "title": "fix parser bug", "authored_date": now}],
            "files": [{"new_path": "src/parser.py", "additions": 5, "deletions": 2}],
            "discussions": {"thread_count": 1, "note_count": 2, "unresolved_count": 0},
            "approvals": {"approvals_required": 1, "approvals_given": 1},
            "pipelines": {"pipeline_count": 1, "failed_count": 0, "success_count": 1, "retry_count": 0},
        },
        {
            "id": project_id * 100000 + 3,
            "project_id": project_id,
            "iid": 3,
            "title": "OPS-45 Update deployment runbook",
            "description": "infra docker cluster deployment docs",
            "state": "opened",
            "author_username": "charlie",
            "labels": ["platform"],
            "web_url": "https://example.local/mr/3",
            "created_at": now,
            "updated_at": now,
            "merged_at": None,
            "closed_at": None,
            "source_branch": "docs/runbook-update",
            "target_branch": "main",
            "data_source": "test",
            "commits": [{"id": "c1b2c3", "title": "update runbook", "authored_date": now}],
            "files": [{"new_path": "docs/runbook.md", "additions": 60, "deletions": 8}],
            "discussions": {"thread_count": 1, "note_count": 3, "unresolved_count": 1},
            "approvals": {"approvals_required": 2, "approvals_given": 1},
            "pipelines": {"pipeline_count": 1, "failed_count": 0, "success_count": 1, "retry_count": 0},
        },
        {
            "id": project_id * 100000 + 4,
            "project_id": project_id,
            "iid": 4,
            "title": "Add payment webhook feature",
            "description": "new endpoint and handlers",
            "state": "merged",
            "author_username": "dana",
            "labels": ["feature"],
            "web_url": "https://example.local/mr/4",
            "created_at": now,
            "updated_at": now,
            "merged_at": now,
            "closed_at": None,
            "source_branch": "feature/payment-webhook",
            "target_branch": "main",
            "data_source": "test",
            "commits": [
                {"id": "d1b2c3", "title": "add endpoint", "authored_date": now},
                {"id": "d1b2c4", "title": "add tests", "authored_date": now},
            ],
            "files": [
                {"new_path": "src/payments/webhook.py", "additions": 90, "deletions": 3},
                {"new_path": "tests/test_webhook.py", "additions": 55, "deletions": 0},
            ],
            "discussions": {"thread_count": 2, "note_count": 4, "unresolved_count": 0},
            "approvals": {"approvals_required": 1, "approvals_given": 1},
            "pipelines": {"pipeline_count": 2, "failed_count": 1, "success_count": 1, "retry_count": 0},
        },
    ]

    db.init_schema()
    with db.connect() as conn:
        for row in sample_rows:
            mr_id = db.upsert_merge_request(conn, row)
            db.replace_mr_commits(conn, mr_id, row["commits"])
            db.replace_mr_files(conn, mr_id, row["files"])
            db.upsert_discussions(conn, mr_id, row["discussions"])
            db.upsert_approvals(conn, mr_id, row["approvals"])
            db.upsert_pipelines(conn, mr_id, row["pipelines"])
            db.upsert_raw_snapshot(conn, project_id, "merge_request", str(row["iid"]), row, now)

    if run_classify:
        classify_project(db, settings, project_id)

    return len(sample_rows)
