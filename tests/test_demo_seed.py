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


def test_seed_demo_data_inserts_and_classifies(tmp_path) -> None:
    db = Database(str(tmp_path / "demo.db"))
    settings = _settings(str(tmp_path / "demo.db"))

    inserted = seed_demo_data(db, project_id=999, settings=settings, run_classify=True)
    assert inserted == 4

    with db.connect() as conn:
        mr_count = conn.execute("SELECT COUNT(*) FROM merge_requests WHERE project_id = 999").fetchone()[0]
        class_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM mr_classifications c
            JOIN merge_requests m ON m.id = c.mr_id
            WHERE m.project_id = 999
            """
        ).fetchone()[0]

    assert mr_count == 4
    assert class_count == 4
