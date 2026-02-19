from __future__ import annotations

from prtool.db import Database


def test_list_ingested_project_ids(tmp_path) -> None:
    db = Database(str(tmp_path / "t.db"))
    db.init_schema()

    with db.connect() as conn:
        db.upsert_merge_request(
            conn,
            {
                "id": 1,
                "project_id": 10,
                "iid": 1,
                "title": "A",
                "description": "",
                "state": "opened",
                "author_username": "u",
                "labels": [],
                "web_url": "",
                "created_at": "",
                "updated_at": "",
                "merged_at": None,
                "closed_at": None,
                "source_branch": "a",
                "target_branch": "main",
            },
        )
        db.upsert_merge_request(
            conn,
            {
                "id": 2,
                "project_id": 20,
                "iid": 1,
                "title": "B",
                "description": "",
                "state": "opened",
                "author_username": "u",
                "labels": [],
                "web_url": "",
                "created_at": "",
                "updated_at": "",
                "merged_at": None,
                "closed_at": None,
                "source_branch": "a",
                "target_branch": "main",
            },
        )
        ids = db.list_ingested_project_ids(conn)

    assert ids == [10, 20]
