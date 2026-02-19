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


def export_csv(db: Database, out_dir: str = "./exports", project_ids: list[int] | None = None, filename_stem: str = "mr_classification") -> Path:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    target = out / f"{filename_stem}.csv"
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
                "capability_tags",
                "risk_tags",
                "classification_confidence",
                "classifier_version",
            ]
        )
        rows = conn.execute(
            f"""
            SELECT m.project_id, m.iid, m.title, c.base_type, c.final_type,
                   c.is_infra_related, c.infra_override_applied,
                   c.complexity_level, c.complexity_score,
                   c.capability_tags_json, c.risk_tags_json,
                   c.classification_confidence, c.classifier_version
            FROM merge_requests m
            JOIN mr_classifications c ON c.mr_id = m.id
            {where_sql}
            ORDER BY m.updated_at ASC
            """,
            params,
        ).fetchall()
        for r in rows:
            writer.writerow([
                r["project_id"], r["iid"], r["title"], r["base_type"], r["final_type"],
                r["is_infra_related"], r["infra_override_applied"],
                r["complexity_level"], r["complexity_score"],
                r["capability_tags_json"], r["risk_tags_json"],
                r["classification_confidence"], r["classifier_version"],
            ])

    return target


def export_jsonl(db: Database, out_dir: str = "./exports", project_ids: list[int] | None = None, filename_stem: str = "mr_classification") -> Path:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    target = out / f"{filename_stem}.jsonl"
    where_sql, params = _scope_where(project_ids)

    with db.connect() as conn, target.open("w", encoding="utf-8") as f:
        rows = conn.execute(
            f"""
            SELECT m.project_id, m.iid, m.title, c.base_type, c.final_type,
                   c.is_infra_related, c.infra_override_applied,
                   c.complexity_level, c.complexity_score,
                   c.capability_tags_json, c.risk_tags_json,
                   c.classification_confidence, c.classifier_version,
                   c.classification_rationale_json
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
            row["capability_tags"] = json.loads(row.pop("capability_tags_json") or "[]")
            row["risk_tags"] = json.loads(row.pop("risk_tags_json") or "[]")
            f.write(json.dumps(row) + "\n")

    return target
