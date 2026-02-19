from __future__ import annotations

from prtool.config import PartialSettings
from prtool.db import Database
from prtool.seed_data import seed_demo_data


def _settings(db_path: str) -> PartialSettings:
    return PartialSettings(
        db_path=db_path,
        infra_ticket_regex=[r"INFRA-\d+", r"OPS-\d+"],
        infra_label_allowlist=["infra", "platform", "devops", "sre"],
        infra_keyword_list=["terraform", "k8s", "deployment", "infra", "docker"],
        infra_strong_threshold=4.0,
        infra_weak_threshold=1.5,
    )


def test_delete_merge_requests_by_source(tmp_path) -> None:
    db_path = str(tmp_path / "cleanup.db")
    db = Database(db_path)
    seed_demo_data(db, project_id=7001, settings=_settings(db_path), run_classify=False)

    with db.connect() as conn:
        db.upsert_merge_request(
            conn,
            {
                "id": 999001,
                "project_id": 7001,
                "iid": 999,
                "title": "real mr",
                "description": "",
                "state": "opened",
                "author_username": "x",
                "labels": [],
                "web_url": "https://gitlab.com/x/y/-/merge_requests/999",
                "created_at": "",
                "updated_at": "",
                "merged_at": None,
                "closed_at": None,
                "source_branch": "a",
                "target_branch": "main",
                "data_source": "production",
            },
        )
        deleted = db.delete_merge_requests_by_source(conn, data_source="test")
        assert deleted == 4

        remaining = conn.execute("SELECT data_source, COUNT(*) c FROM merge_requests GROUP BY data_source").fetchall()

    assert len(remaining) == 1
    assert remaining[0][0] == "production"
    assert remaining[0][1] == 1
