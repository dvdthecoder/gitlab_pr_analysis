from __future__ import annotations

import csv
import json
from datetime import datetime, timezone

from prtool.db import Database
from prtool.export import export_csv, export_jsonl


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


def _class() -> dict:
    return {
        "base_type": "feature",
        "final_type": "feature",
        "is_infra_related": False,
        "infra_override_applied": False,
        "complexity_level": "High",
        "complexity_score": 6.5,
        "rationale": {"ok": True},
        "classified_at": datetime.now(timezone.utc).isoformat(),
    }


def test_export_csv_project_scope(tmp_path) -> None:
    db = Database(str(tmp_path / "export.db"))
    db.init_schema()
    with db.connect() as conn:
        for project_id, iid in [(10, 1), (20, 2)]:
            mr_id = project_id * 100 + iid
            db.upsert_merge_request(conn, _mr(project_id, iid, mr_id))
            db.upsert_classification(conn, mr_id, _class())

    out = export_csv(db, out_dir=str(tmp_path / "exports"), project_ids=[10])
    with out.open("r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 1
    assert rows[0]["project_id"] == "10"


def test_export_jsonl_project_scope(tmp_path) -> None:
    db = Database(str(tmp_path / "export2.db"))
    db.init_schema()
    with db.connect() as conn:
        for project_id, iid in [(10, 1), (20, 2)]:
            mr_id = project_id * 100 + iid
            db.upsert_merge_request(conn, _mr(project_id, iid, mr_id))
            db.upsert_classification(conn, mr_id, _class())

    out = export_jsonl(db, out_dir=str(tmp_path / "exports"), project_ids=[20])
    with out.open("r", encoding="utf-8") as f:
        rows = [json.loads(line) for line in f if line.strip()]
    assert len(rows) == 1
    assert int(rows[0]["project_id"]) == 20
