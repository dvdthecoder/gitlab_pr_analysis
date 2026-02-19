from __future__ import annotations

import csv
from pathlib import Path

from prtool.db import Database


def create_audit_sample(db: Database, size: int, out_dir: str = "./reports") -> Path:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    target = out / "audit_sample.csv"

    with db.connect() as conn, target.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["project_id", "mr_iid", "title", "predicted_type", "predicted_complexity", "human_type", "human_complexity", "notes"])
        rows = conn.execute(
            """
            SELECT m.project_id, m.iid, m.title, c.final_type, c.complexity_level
            FROM merge_requests m
            JOIN mr_classifications c ON c.mr_id = m.id
            ORDER BY RANDOM()
            LIMIT ?
            """,
            (size,),
        ).fetchall()
        for r in rows:
            writer.writerow([r[0], r[1], r[2], r[3], r[4], "", "", ""])

    return target
