from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

from prtool.db import Database


def _scope_where(project_ids: list[int] | None) -> tuple[str, tuple[Any, ...]]:
    if not project_ids:
        return "", ()
    placeholders = ",".join(["?"] * len(project_ids))
    return f"WHERE m.project_id IN ({placeholders})", tuple(project_ids)


def export_csv(db: Database, out_dir: str = "./exports", project_ids: list[int] | None = None) -> Path:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    target = out / "mr_classification.csv"
    where_sql, params = _scope_where(project_ids)

    with db.connect() as conn, target.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "project_id",
                "mr_iid",
                "title",
                "base_type",
                "final_type",
                "is_infra_related",
                "infra_override_applied",
                "complexity_level",
                "complexity_score",
            ]
        )
        rows = conn.execute(
            f"""
            SELECT m.project_id, m.iid, m.title, c.base_type, c.final_type,
                   c.is_infra_related, c.infra_override_applied,
                   c.complexity_level, c.complexity_score
            FROM merge_requests m
            JOIN mr_classifications c ON c.mr_id = m.id
            {where_sql}
            ORDER BY m.updated_at ASC
            """,
            params,
        ).fetchall()
        for r in rows:
            writer.writerow(list(r))

    return target


def export_jsonl(db: Database, out_dir: str = "./exports", project_ids: list[int] | None = None) -> Path:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    target = out / "mr_classification.jsonl"
    where_sql, params = _scope_where(project_ids)

    with db.connect() as conn, target.open("w", encoding="utf-8") as f:
        rows = conn.execute(
            f"""
            SELECT m.project_id, m.iid, m.title, c.base_type, c.final_type,
                   c.is_infra_related, c.infra_override_applied,
                   c.complexity_level, c.complexity_score, c.classification_rationale_json
            FROM merge_requests m
            JOIN mr_classifications c ON c.mr_id = m.id
            {where_sql}
            ORDER BY m.updated_at ASC
            """,
            params,
        ).fetchall()
        for r in rows:
            row = dict(r)
            row["classification_rationale"] = json.loads(row.pop("classification_rationale_json"))
            f.write(json.dumps(row) + "\n")

    return target
