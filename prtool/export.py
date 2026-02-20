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
                "mr_outcome",
                "regression_probability",
                "review_depth_required",
                "memory_score_version",
                "memory_updated_at",
            ]
        )
        rows = conn.execute(
            f"""
            SELECT m.project_id, m.iid, m.title, c.base_type, c.final_type,
                   c.is_infra_related, c.infra_override_applied,
                   c.complexity_level, c.complexity_score,
                   c.capability_tags_json, c.risk_tags_json,
                   c.classification_confidence, c.classifier_version,
                   r.mr_outcome, r.regression_probability, r.review_depth_required,
                   r.memory_score_version, r.updated_at as memory_updated_at
            FROM merge_requests m
            JOIN mr_classifications c ON c.mr_id = m.id
            LEFT JOIN mr_memory_runtime r ON r.mr_id = m.id
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
                r["mr_outcome"], r["regression_probability"], r["review_depth_required"],
                r["memory_score_version"], r["memory_updated_at"],
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
                   c.classification_rationale_json,
                   r.mr_outcome, r.regression_probability, r.review_depth_required,
                   r.memory_score_version, r.updated_at as memory_updated_at
            FROM merge_requests m
            JOIN mr_classifications c ON c.mr_id = m.id
            LEFT JOIN mr_memory_runtime r ON r.mr_id = m.id
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


def export_memory_csv(
    db: Database,
    out_dir: str = "./exports",
    project_ids: list[int] | None = None,
    filename_stem: str = "mr_memory",
) -> Path:
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
                "final_type",
                "complexity_score",
                "mr_outcome",
                "regression_probability",
                "review_depth_required",
                "memory_score_version",
                "memory_updated_at",
                "addendum_markdown_path",
                "context_markdown_path",
            ]
        )
        rows = conn.execute(
            f"""
            SELECT
              m.project_id,
              m.iid,
              m.title,
              c.final_type,
              c.complexity_score,
              r.mr_outcome,
              r.regression_probability,
              r.review_depth_required,
              r.memory_score_version,
              r.updated_at as memory_updated_at,
              r.addendum_markdown_path,
              r.context_markdown_path
            FROM mr_memory_runtime r
            JOIN merge_requests m ON m.id = r.mr_id
            LEFT JOIN mr_classifications c ON c.mr_id = r.mr_id
            {where_sql}
            ORDER BY r.updated_at DESC, m.project_id ASC, m.iid ASC
            """,
            params,
        ).fetchall()

        for r in rows:
            writer.writerow(
                [
                    r["project_id"],
                    r["iid"],
                    r["title"],
                    r["final_type"],
                    r["complexity_score"],
                    r["mr_outcome"],
                    r["regression_probability"],
                    r["review_depth_required"],
                    r["memory_score_version"],
                    r["memory_updated_at"],
                    r["addendum_markdown_path"],
                    r["context_markdown_path"],
                ]
            )

    return target


def export_memory_jsonl(
    db: Database,
    out_dir: str = "./exports",
    project_ids: list[int] | None = None,
    filename_stem: str = "mr_memory",
) -> Path:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    target = out / f"{filename_stem}.jsonl"
    where_sql, params = _scope_where(project_ids)

    with db.connect() as conn, target.open("w", encoding="utf-8") as f:
        rows = conn.execute(
            f"""
            SELECT
              m.project_id,
              m.iid,
              m.title,
              c.final_type,
              c.complexity_score,
              r.mr_outcome,
              r.regression_probability,
              r.review_depth_required,
              r.memory_score_version,
              r.updated_at as memory_updated_at,
              r.addendum_markdown_path,
              r.context_markdown_path,
              r.assessment_json,
              r.similar_mrs_json
            FROM mr_memory_runtime r
            JOIN merge_requests m ON m.id = r.mr_id
            LEFT JOIN mr_classifications c ON c.mr_id = r.mr_id
            {where_sql}
            ORDER BY r.updated_at DESC, m.project_id ASC, m.iid ASC
            """,
            params,
        ).fetchall()
        for row in rows:
            item = dict(row)
            item["assessment"] = json.loads(item.pop("assessment_json") or "{}")
            item["similar_mrs"] = json.loads(item.pop("similar_mrs_json") or "[]")
            f.write(json.dumps(item) + "\n")

    return target
