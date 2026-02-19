from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any

from prtool.classifier import CLASSIFIER_VERSION, ClassificationConfig, classify
from prtool.config import PartialSettings, Settings
from prtool.db import Database
from prtool.feature_extractor import FeatureExtractor
from prtool.gitlab_client import GitLabSourceClient


def _to_mr_record(project_id: int, raw: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": int(raw["id"]),
        "project_id": project_id,
        "iid": int(raw["iid"]),
        "title": raw.get("title", ""),
        "description": raw.get("description"),
        "state": raw.get("state"),
        "author_username": (raw.get("author") or {}).get("username"),
        "labels": raw.get("labels", []),
        "web_url": raw.get("web_url"),
        "created_at": raw.get("created_at"),
        "updated_at": raw.get("updated_at"),
        "merged_at": raw.get("merged_at"),
        "closed_at": raw.get("closed_at"),
        "source_branch": raw.get("source_branch"),
        "target_branch": raw.get("target_branch"),
        "data_source": "production",
    }


def _summarize_discussions(discussions: list[dict[str, Any]]) -> dict[str, int]:
    note_count = 0
    unresolved_count = 0
    for d in discussions:
        notes = d.get("notes", [])
        note_count += len(notes)
        if d.get("resolvable") and not d.get("resolved"):
            unresolved_count += 1
    return {
        "thread_count": len(discussions),
        "note_count": note_count,
        "unresolved_count": unresolved_count,
    }


def _summarize_approvals(a: dict[str, Any]) -> dict[str, int]:
    approved_by = a.get("approved_by", []) or []
    return {
        "approvals_required": int(a.get("approvals_required", 0) or 0),
        "approvals_given": len(approved_by),
    }


def _summarize_pipelines(pipelines: list[dict[str, Any]]) -> dict[str, int]:
    failed = sum(1 for p in pipelines if p.get("status") == "failed")
    success = sum(1 for p in pipelines if p.get("status") == "success")
    retry = sum(1 for p in pipelines if p.get("status") == "canceled")
    return {
        "pipeline_count": len(pipelines),
        "failed_count": failed,
        "success_count": success,
        "retry_count": retry,
    }


def _fetch_mr_details(
    client: GitLabSourceClient,
    project_id: int,
    mr_iid: int,
    light_mode: bool,
) -> dict[str, Any]:
    commits = client.get_mr_commits(project_id, mr_iid)
    files = client.get_mr_changes(project_id, mr_iid)
    if light_mode:
        return {
            "commits": commits,
            "files": files,
            "discussions": [],
            "approvals": {},
            "pipelines": [],
        }
    return {
        "commits": commits,
        "files": files,
        "discussions": client.get_mr_discussions(project_id, mr_iid),
        "approvals": client.get_mr_approvals(project_id, mr_iid),
        "pipelines": client.get_mr_pipelines(project_id, mr_iid),
    }


def sync_backfill(
    db: Database,
    settings: Settings,
    project_id: int,
    since: str,
    *,
    concurrency: int = 5,
    light_mode: bool = False,
) -> int:
    client = GitLabSourceClient(settings)
    fetched = client.list_merge_requests(project_id, created_after=since)
    return _ingest_mrs(
        db,
        settings,
        project_id,
        fetched,
        source="backfill",
        concurrency=concurrency,
        light_mode=light_mode,
    )


def sync_refresh(
    db: Database,
    settings: Settings,
    project_id: int,
    *,
    concurrency: int = 5,
    light_mode: bool = False,
) -> int:
    client = GitLabSourceClient(settings)
    with db.connect() as conn:
        cp = db.load_checkpoint(conn, project_id, "refresh")
    updated_after = cp["watermark_updated_at"] if cp else None
    fetched = client.list_merge_requests(project_id, updated_after=updated_after)
    return _ingest_mrs(
        db,
        settings,
        project_id,
        fetched,
        source="refresh",
        concurrency=concurrency,
        light_mode=light_mode,
    )


def _ingest_mrs(
    db: Database,
    settings: Settings,
    project_id: int,
    mrs: list[dict[str, Any]],
    source: str,
    *,
    concurrency: int = 5,
    light_mode: bool = False,
) -> int:
    client = GitLabSourceClient(settings)
    now = datetime.now(timezone.utc).isoformat()
    max_updated_at: str | None = None
    last_iid: int | None = None
    concurrency = max(1, concurrency)

    with db.connect() as conn:
        iids = [int(mr["iid"]) for mr in mrs]
        existing_updated_map = db.get_mr_updated_at_map(conn, project_id, iids)

        to_process: list[dict[str, Any]] = []
        skipped = 0
        for mr in mrs:
            mr_iid = int(mr["iid"])
            updated_at = mr.get("updated_at")
            if source == "refresh" and updated_at and existing_updated_map.get(mr_iid) == updated_at:
                skipped += 1
            else:
                to_process.append(mr)
            if updated_at and (max_updated_at is None or updated_at > max_updated_at):
                max_updated_at = updated_at
                last_iid = mr_iid

        total = len(mrs)
        print(f"[project {project_id}] fetched={total}, skipped_unchanged={skipped}, processing={len(to_process)}")

        processed = 0
        future_map: dict[Any, dict[str, Any]] = {}
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            for mr in to_process:
                mr_iid = int(mr["iid"])
                future = executor.submit(_fetch_mr_details, client, project_id, mr_iid, light_mode)
                future_map[future] = mr

            for future in as_completed(future_map):
                mr = future_map[future]
                mr_iid = int(mr["iid"])
                details = future.result()
                mr_record = _to_mr_record(project_id, mr)
                mr_id = db.upsert_merge_request(conn, mr_record)

                commits = details["commits"]
                files = details["files"]
                discussions = details["discussions"]
                approvals = details["approvals"]
                pipelines = details["pipelines"]

                db.replace_mr_commits(conn, mr_id, commits)
                db.replace_mr_files(conn, mr_id, files)
                db.upsert_discussions(conn, mr_id, _summarize_discussions(discussions))
                db.upsert_approvals(conn, mr_id, _summarize_approvals(approvals))
                db.upsert_pipelines(conn, mr_id, _summarize_pipelines(pipelines))
                db.upsert_raw_snapshot(conn, project_id, "merge_request", str(mr_iid), mr, now)

                processed += 1
                if processed == 1 or processed % 10 == 0 or processed == len(to_process):
                    print(f"[project {project_id}] progress {processed}/{len(to_process)} processed")

        db.upsert_checkpoint(conn, project_id, source, max_updated_at, last_iid, now)

    return len(to_process)


def classify_project(
    db: Database,
    partial_settings: PartialSettings,
    project_id: int,
    *,
    only_stale: bool = False,
    target_classifier_version: str | None = None,
) -> int:
    extractor = FeatureExtractor(partial_settings)
    c_cfg = ClassificationConfig(
        infra_strong_threshold=partial_settings.infra_strong_threshold,
        infra_weak_threshold=partial_settings.infra_weak_threshold,
    )

    with db.connect() as conn:
        if only_stale:
            expected_version = target_classifier_version or CLASSIFIER_VERSION
            rows = conn.execute(
                """
                SELECT m.*
                FROM merge_requests m
                LEFT JOIN mr_classifications c ON c.mr_id = m.id
                WHERE m.project_id = ?
                  AND (c.mr_id IS NULL OR c.classifier_version IS NULL OR c.classifier_version != ?)
                ORDER BY m.updated_at ASC
                """,
                (project_id, expected_version),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT * FROM merge_requests WHERE project_id = ?
                ORDER BY updated_at ASC
                """,
                (project_id,),
            ).fetchall()

        total = len(rows)
        for idx, row in enumerate(rows, start=1):
            mr_id = int(row["id"])
            commits = conn.execute("SELECT * FROM mr_commits WHERE mr_id = ?", (mr_id,)).fetchall()
            files = conn.execute("SELECT * FROM mr_files WHERE mr_id = ?", (mr_id,)).fetchall()
            discussion = conn.execute("SELECT * FROM mr_discussions WHERE mr_id = ?", (mr_id,)).fetchone()
            pipelines = conn.execute("SELECT * FROM mr_pipelines WHERE mr_id = ?", (mr_id,)).fetchone()

            mr = dict(row)
            mr["labels"] = []
            labels_json = row["labels_json"]
            if labels_json:
                import json

                mr["labels"] = json.loads(labels_json)

            discussion_map = {
                "thread_count": int(discussion["thread_count"]) if discussion else 0,
                "note_count": int(discussion["note_count"]) if discussion else 0,
                "unresolved_count": int(discussion["unresolved_count"]) if discussion else 0,
            }
            pipeline_map = {
                "pipeline_count": int(pipelines["pipeline_count"]) if pipelines else 0,
                "failed_count": int(pipelines["failed_count"]) if pipelines else 0,
                "success_count": int(pipelines["success_count"]) if pipelines else 0,
                "retry_count": int(pipelines["retry_count"]) if pipelines else 0,
            }

            feature_row = extractor.extract(
                mr=mr,
                commits=[dict(c) for c in commits],
                files=[dict(f) for f in files],
                discussions=discussion_map,
                pipelines=pipeline_map,
            )
            db.upsert_feature_row(conn, mr_id, feature_row)
            classification = classify(mr, [dict(f) for f in files], feature_row, c_cfg)
            db.upsert_classification(conn, mr_id, classification)
            if idx == 1 or idx % 25 == 0 or idx == total:
                print(f"[project {project_id}] classify progress {idx}/{total}")

    return len(rows)
