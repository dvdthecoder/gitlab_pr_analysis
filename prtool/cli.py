from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any

from prtool.audit import create_audit_sample
from prtool.config import (
    Settings,
    load_dotenv,
    load_partial_settings,
    load_settings,
    resolve_group_ids,
    resolve_project_ids,
)
from prtool.db import Database
from prtool.enrich import (
    QODO_TOOLS,
    CandidateOptions,
    EnrichOptions,
    compact_project_qodo,
    enrich_qodo_project,
    get_enrich_status,
    select_enrich_candidates,
)
from prtool.export import export_csv, export_jsonl
from prtool.gitlab_client import GitLabSourceClient
from prtool.pipeline import classify_project, sync_backfill, sync_refresh
from prtool.seed_data import seed_demo_data
from prtool.viewer import run_viewer


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="prtool")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("init-db")

    sync = sub.add_parser("sync")
    sync_sub = sync.add_subparsers(dest="sync_command", required=True)

    backfill = sync_sub.add_parser("backfill")
    backfill.add_argument("--project-id", type=int, action="append")
    backfill.add_argument("--group-id", action="append")
    backfill.add_argument("--all-projects", action="store_true")
    backfill.add_argument("--project-start-index", type=int, default=1)
    backfill.add_argument("--project-count", type=int)
    backfill.add_argument("--since", required=True)
    backfill.add_argument("--concurrency", type=int, default=5)
    backfill.add_argument("--light-mode", action="store_true")

    refresh = sync_sub.add_parser("refresh")
    refresh.add_argument("--project-id", type=int, action="append")
    refresh.add_argument("--group-id", action="append")
    refresh.add_argument("--all-projects", action="store_true")
    refresh.add_argument("--project-start-index", type=int, default=1)
    refresh.add_argument("--project-count", type=int)
    refresh.add_argument("--concurrency", type=int, default=5)
    refresh.add_argument("--light-mode", action="store_true")

    classify_cmd = sub.add_parser("classify")
    classify_cmd.add_argument("--project-id", type=int, action="append")
    classify_cmd.add_argument("--group-id", action="append")
    classify_cmd.add_argument("--all-projects", action="store_true")
    classify_cmd.add_argument("--project-start-index", type=int, default=1)
    classify_cmd.add_argument("--project-count", type=int)

    export_cmd = sub.add_parser("export")
    export_cmd.add_argument("--format", choices=["csv", "jsonl", "both"], default="both")

    audit_cmd = sub.add_parser("audit")
    audit_sub = audit_cmd.add_subparsers(dest="audit_command", required=True)
    sample = audit_sub.add_parser("sample")
    sample.add_argument("--size", type=int, default=50)

    demo_cmd = sub.add_parser("demo")
    demo_sub = demo_cmd.add_subparsers(dest="demo_command", required=True)
    seed = demo_sub.add_parser("seed")
    seed.add_argument("--project-id", type=int, default=999)
    seed.add_argument("--no-classify", action="store_true")

    seed_cmd = sub.add_parser("seed")
    seed_cmd.add_argument("--project-id", type=int, default=999)
    seed_cmd.add_argument("--no-classify", action="store_true")

    batch_cmd = sub.add_parser("batch")
    batch_sub = batch_cmd.add_subparsers(dest="batch_command", required=True)
    run = batch_sub.add_parser("run")
    run.add_argument("--project-id", type=int, action="append")
    run.add_argument("--group-id", action="append")
    run.add_argument("--all-projects", action="store_true")
    run.add_argument("--project-start-index", type=int, default=1)
    run.add_argument("--project-count", type=int)
    run.add_argument("--since")
    run.add_argument("--format", choices=["csv", "jsonl", "both"], default="both")
    run.add_argument("--concurrency", type=int, default=5)
    run.add_argument("--light-mode", action="store_true")

    projects_cmd = sub.add_parser("projects")
    projects_sub = projects_cmd.add_subparsers(dest="projects_command", required=True)
    list_cmd = projects_sub.add_parser("list")
    list_cmd.add_argument("--project-id", type=int, action="append")
    list_cmd.add_argument("--all-projects", action="store_true")
    list_cmd.add_argument("--group-id", action="append")
    list_cmd.add_argument("--project-start-index", type=int, default=1)
    list_cmd.add_argument("--project-count", type=int)
    list_cmd.add_argument("--with-mr-count", action="store_true", default=True)
    list_cmd.add_argument("--format", choices=["text", "json"], default="text")
    count_cmd = projects_sub.add_parser("count")
    count_cmd.add_argument("--all-projects", action="store_true")
    count_cmd.add_argument("--group-id", action="append")
    count_cmd.add_argument("--format", choices=["text", "json"], default="text")
    count_cmd.add_argument("--include-ids", action="store_true")

    enrich_cmd = sub.add_parser("enrich")
    enrich_sub = enrich_cmd.add_subparsers(dest="enrich_command", required=True)
    qodo_cmd = enrich_sub.add_parser("qodo")
    qodo_cmd.add_argument("--project-id", type=int, action="append")
    qodo_cmd.add_argument("--group-id", action="append")
    qodo_cmd.add_argument("--all-projects", action="store_true")
    qodo_cmd.add_argument("--project-start-index", type=int, default=1)
    qodo_cmd.add_argument("--project-count", type=int)
    qodo_cmd.add_argument("--mr-limit", type=int)
    qodo_cmd.add_argument("--concurrency", type=int, default=5)
    qodo_cmd.add_argument("--only-missing", action="store_true", default=True)
    qodo_cmd.add_argument("--force", action="store_true")
    qodo_cmd.add_argument("--data-source", choices=["production", "test", "all"], default="production")
    qodo_cmd.add_argument("--output-root", default="outputs/qodo")
    qodo_cmd.add_argument("--compact-max-tokens", type=int, default=3000)
    qodo_cmd.add_argument("--include-mermaid", action="store_true", default=True)
    qodo_cmd.add_argument("--timeout-sec", type=int, default=180)
    qodo_cmd.add_argument("--tools", default="describe", help="Comma-separated tools: describe,review,improve")
    qodo_cmd.add_argument("--no-progress", action="store_true")
    qodo_cmd.add_argument("--candidate-mode", choices=["none", "stratified"], default="none")
    qodo_cmd.add_argument("--candidate-count", type=int, default=10)
    qodo_cmd.add_argument("--candidate-scope", choices=["global", "per-project", "hybrid"], default="global")
    qodo_cmd.add_argument("--candidate-type-balance", choices=["soft", "hard", "none"], default="soft")
    qodo_cmd.add_argument("--candidate-data-source", choices=["production", "test", "all"], default="production")
    qodo_cmd.add_argument("--candidate-preview", action="store_true")

    status_cmd = enrich_sub.add_parser("status")
    status_cmd.add_argument("--project-id", type=int, action="append")
    status_cmd.add_argument("--group-id", action="append")
    status_cmd.add_argument("--all-projects", action="store_true")
    status_cmd.add_argument("--project-start-index", type=int, default=1)
    status_cmd.add_argument("--project-count", type=int)
    status_cmd.add_argument("--data-source", choices=["production", "test", "all"], default="production")
    status_cmd.add_argument("--format", choices=["text", "json"], default="text")

    list_projects_cmd = sub.add_parser("list-projects")
    list_projects_cmd.add_argument("--group-id", action="append")
    list_projects_cmd.add_argument("--project-start-index", type=int, default=1)
    list_projects_cmd.add_argument("--project-count", type=int)

    view_cmd = sub.add_parser("view")
    view_cmd.add_argument("--host", default="127.0.0.1")
    view_cmd.add_argument("--port", type=int, default=8765)

    cleanup_cmd = sub.add_parser("cleanup")
    cleanup_cmd.add_argument("--data-source", choices=["test", "production"], default="test")
    cleanup_cmd.add_argument("--project-id", type=int)

    return parser


def _slice_project_ids(project_ids: list[int], start_index: int = 1, count: int | None = None) -> list[int]:
    if start_index < 1:
        raise ValueError("--project-start-index must be >= 1")
    if count is not None and count < 1:
        raise ValueError("--project-count must be >= 1")

    start = start_index - 1
    end = None if count is None else start + count
    selected = project_ids[start:end]
    if not selected:
        raise ValueError("No projects selected for requested index window")
    return selected


def _resolve_concurrency(args: argparse.Namespace) -> int:
    cli_value = getattr(args, "concurrency", None)
    if cli_value is not None:
        value = int(cli_value)
    else:
        value = int(os.getenv("SYNC_CONCURRENCY", "5"))
    if value < 1:
        raise ValueError("concurrency must be >= 1")
    return value


def _collect_group_projects(
    client: GitLabSourceClient,
    group_ids: list[str],
) -> list[dict[str, Any]]:
    projects: dict[int, dict[str, Any]] = {}
    for group_id in group_ids:
        for project in client.list_group_projects(group_id):
            projects[int(project["id"])] = project
    return [projects[pid] for pid in sorted(projects.keys())]


def _resolve_discovery_projects(args: argparse.Namespace, settings: Settings) -> list[dict[str, Any]]:
    client = GitLabSourceClient(settings)
    group_ids = resolve_group_ids(getattr(args, "group_id", None))
    if group_ids:
        projects = _collect_group_projects(client, group_ids)
        if not projects:
            raise ValueError(f"No projects found for groups: {group_ids}")
        return projects
    if getattr(args, "all_projects", False):
        projects = client.list_accessible_projects()
        if not projects:
            raise ValueError("No accessible projects found for current PAT")
        return projects
    return []


def _resolve_count_scope(args: argparse.Namespace, settings: Settings) -> tuple[str, list[int], list[str]]:
    explicit_group_ids = getattr(args, "group_id", None)
    if explicit_group_ids:
        discovered = _resolve_discovery_projects(args, settings)
        group_ids = sorted(set(str(g).strip() for g in explicit_group_ids if str(g).strip()))
        project_ids = sorted({int(p["id"]) for p in discovered})
        return "groups", project_ids, group_ids
    if getattr(args, "all_projects", False):
        discovered = _resolve_discovery_projects(args, settings)
        project_ids = sorted({int(p["id"]) for p in discovered})
        return "all-projects", project_ids, []
    project_ids = sorted(set(resolve_project_ids()))
    return "configured-project-ids", project_ids, []


def _resolve_project_scope_ids(args: argparse.Namespace) -> list[int]:
    explicit_project_ids = getattr(args, "project_id", None)
    if explicit_project_ids:
        ids = resolve_project_ids(explicit_project_ids)
        return _slice_project_ids(
            sorted(set(ids)),
            start_index=getattr(args, "project_start_index", 1),
            count=getattr(args, "project_count", None),
        )

    group_ids = resolve_group_ids(getattr(args, "group_id", None))
    if group_ids or getattr(args, "all_projects", False):
        settings = load_settings()
        discovered = _resolve_discovery_projects(args, settings)
        ids = [int(p["id"]) for p in discovered]
    else:
        ids = resolve_project_ids(getattr(args, "project_id", None))
    return _slice_project_ids(
        sorted(set(ids)),
        start_index=getattr(args, "project_start_index", 1),
        count=getattr(args, "project_count", None),
    )


def _rank_projects_with_mr_counts(
    projects: list[dict[str, Any]],
    client: GitLabSourceClient,
    with_mr_count: bool = True,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for p in projects:
        row = dict(p)
        row["mr_count_all_states"] = client.get_project_mr_count_all_states(int(p["id"])) if with_mr_count else None
        rows.append(row)
    rows.sort(key=lambda x: (-(x["mr_count_all_states"] or 0), int(x["id"])))
    for idx, row in enumerate(rows, start=1):
        row["rank"] = idx
    return rows


def _resolve_sync_project_ids(args: argparse.Namespace, settings: Settings) -> list[int]:
    explicit_project_ids = getattr(args, "project_id", None)
    if explicit_project_ids:
        project_ids = resolve_project_ids(explicit_project_ids)
    else:
        discovered = _resolve_discovery_projects(args, settings)
        if discovered:
            project_ids = [int(p["id"]) for p in discovered]
        else:
            project_ids = resolve_project_ids(None)
    return _slice_project_ids(
        project_ids,
        start_index=getattr(args, "project_start_index", 1),
        count=getattr(args, "project_count", None),
    )


def _parse_tools(raw: str) -> tuple[str, ...]:
    tokens = [t.strip().lower() for t in (raw or "").split(",") if t.strip()]
    aliases = {"analyse": "improve", "analyze": "improve"}
    tokens = [aliases.get(t, t) for t in tokens]
    if not tokens:
        return ("describe",)
    bad = [t for t in tokens if t not in QODO_TOOLS]
    if bad:
        raise ValueError(f"Invalid --tools values: {bad}. Allowed: {','.join(QODO_TOOLS)}")
    deduped: list[str] = []
    for t in tokens:
        if t not in deduped:
            deduped.append(t)
    return tuple(deduped)


def _resolve_classify_project_ids(args: argparse.Namespace, db: Database) -> list[int]:
    explicit_project_ids = getattr(args, "project_id", None)
    if explicit_project_ids:
        project_ids = resolve_project_ids(explicit_project_ids)
    else:
        group_ids = resolve_group_ids(getattr(args, "group_id", None))
        if group_ids:
            settings = load_settings()
            discovered = _resolve_discovery_projects(args, settings)
            project_ids = [int(p["id"]) for p in discovered]
            if not project_ids:
                raise ValueError("No projects found for classify scope.")
        elif getattr(args, "all_projects", False):
            with db.connect() as conn:
                project_ids = db.list_ingested_project_ids(conn)
            if not project_ids:
                raise ValueError("No projects found in DB. Run sync first.")
        else:
            project_ids = resolve_project_ids(None)
    return _slice_project_ids(
        project_ids,
        start_index=getattr(args, "project_start_index", 1),
        count=getattr(args, "project_count", None),
    )


def main(argv: list[str] | None = None) -> int:
    load_dotenv()
    parser = build_parser()
    args = parser.parse_args(argv)

    partial = load_partial_settings()
    db = Database(partial.db_path)

    if args.command == "init-db":
        db.init_schema()
        print(f"Initialized SQLite schema at {partial.db_path}")
        return 0

    if args.command == "sync":
        settings = load_settings()
        project_ids = _resolve_sync_project_ids(args, settings)
        concurrency = _resolve_concurrency(args)
        db.init_schema()
        if args.sync_command == "backfill":
            print(
                f"Selected projects ({len(project_ids)}): {project_ids} | "
                f"concurrency={concurrency} | light_mode={args.light_mode}"
            )
            total = 0
            for project_id in project_ids:
                count = sync_backfill(
                    db,
                    settings,
                    project_id,
                    args.since,
                    concurrency=concurrency,
                    light_mode=args.light_mode,
                )
                total += count
                print(f"[project {project_id}] Backfill complete: {count} merge requests ingested")
            print(f"Backfill total across projects: {total}")
            return 0
        if args.sync_command == "refresh":
            print(
                f"Selected projects ({len(project_ids)}): {project_ids} | "
                f"concurrency={concurrency} | light_mode={args.light_mode}"
            )
            total = 0
            for project_id in project_ids:
                count = sync_refresh(
                    db,
                    settings,
                    project_id,
                    concurrency=concurrency,
                    light_mode=args.light_mode,
                )
                total += count
                print(f"[project {project_id}] Refresh complete: {count} merge requests ingested")
            print(f"Refresh total across projects: {total}")
            return 0

    if args.command == "classify":
        db.init_schema()
        project_ids = _resolve_classify_project_ids(args, db)
        print(f"Selected projects ({len(project_ids)}): {project_ids}")
        total = 0
        for project_id in project_ids:
            count = classify_project(db, partial, project_id)
            total += count
            print(f"[project {project_id}] Classification complete: {count} merge requests processed")
        print(f"Classification total across projects: {total}")
        return 0

    if args.command == "export":
        db.init_schema()
        outputs: list[str] = []
        if args.format in ("csv", "both"):
            outputs.append(str(export_csv(db)))
        if args.format in ("jsonl", "both"):
            outputs.append(str(export_jsonl(db)))
        print("Exported:\n" + "\n".join(outputs))
        return 0

    if args.command == "audit" and args.audit_command == "sample":
        db.init_schema()
        output = create_audit_sample(db, args.size)
        print(f"Audit sample written: {output}")
        return 0

    if args.command == "demo" and args.demo_command == "seed":
        count = seed_demo_data(
            db=db,
            project_id=args.project_id,
            settings=partial,
            run_classify=not args.no_classify,
        )
        print(f"Seeded {count} demo merge requests for project {args.project_id}")
        return 0

    if args.command == "seed":
        count = seed_demo_data(
            db=db,
            project_id=args.project_id,
            settings=partial,
            run_classify=not args.no_classify,
        )
        print(f"Seeded {count} demo merge requests for project {args.project_id}")
        return 0

    if args.command == "batch" and args.batch_command == "run":
        settings = load_settings()
        db.init_schema()
        project_ids = _resolve_sync_project_ids(args, settings)
        concurrency = _resolve_concurrency(args)
        print(
            f"Selected projects ({len(project_ids)}): {project_ids} | "
            f"concurrency={concurrency} | light_mode={args.light_mode}"
        )

        sync_total = 0
        for project_id in project_ids:
            if args.since:
                count = sync_backfill(
                    db,
                    settings,
                    project_id,
                    args.since,
                    concurrency=concurrency,
                    light_mode=args.light_mode,
                )
                print(f"[project {project_id}] Backfill complete: {count} merge requests ingested")
            else:
                count = sync_refresh(
                    db,
                    settings,
                    project_id,
                    concurrency=concurrency,
                    light_mode=args.light_mode,
                )
                print(f"[project {project_id}] Refresh complete: {count} merge requests ingested")
            sync_total += count
        print(f"Sync total across projects: {sync_total}")

        classify_total = 0
        for project_id in project_ids:
            count = classify_project(db, partial, project_id)
            classify_total += count
            print(f"[project {project_id}] Classification complete: {count} merge requests processed")
        print(f"Classification total across projects: {classify_total}")

        outputs: list[str] = []
        if args.format in ("csv", "both"):
            outputs.append(str(export_csv(db)))
        if args.format in ("jsonl", "both"):
            outputs.append(str(export_jsonl(db)))
        print("Exported:\n" + "\n".join(outputs))
        return 0

    if args.command == "projects" and args.projects_command == "list":
        settings = load_settings()
        projects = _resolve_discovery_projects(args, settings)
        if not projects:
            if getattr(args, "project_id", None):
                projects = [{"id": int(pid), "path_with_namespace": "", "name": ""} for pid in args.project_id]
            else:
                client = GitLabSourceClient(settings)
                projects = client.list_accessible_projects()
        all_ids = sorted({int(p["id"]) for p in projects})
        selected_ids = _slice_project_ids(all_ids, start_index=args.project_start_index, count=args.project_count)
        selected_set = set(selected_ids)
        selected_projects = [p for p in projects if int(p["id"]) in selected_set]
        client = GitLabSourceClient(settings)
        ranked = _rank_projects_with_mr_counts(selected_projects, client, with_mr_count=args.with_mr_count)

        if args.format == "json":
            print(json.dumps(ranked))
            return 0

        print(f"Project window: start={args.project_start_index}, count={args.project_count or 'ALL'}")
        print("rank\tproject_id\tmr_count_all_states\tpath_with_namespace\tname")
        for project in ranked:
            print(
                f"{project['rank']}\t{int(project['id'])}\t{project.get('mr_count_all_states', 0) or 0}\t"
                f"{project.get('path_with_namespace','')}\t{project.get('name','')}"
            )
        return 0

    if args.command == "projects" and args.projects_command == "count":
        settings = load_settings()
        scope, project_ids, groups = _resolve_count_scope(args, settings)
        if args.format == "json":
            payload: dict[str, Any] = {
                "total_projects": len(project_ids),
                "scope": scope,
            }
            if scope == "groups":
                payload["groups"] = groups
            if args.include_ids:
                payload["project_ids"] = project_ids
            print(json.dumps(payload))
            return 0

        print(f"total_projects: {len(project_ids)}")
        print(f"scope: {scope}")
        if scope == "groups":
            print(f"groups: {','.join(groups)}")
        if args.include_ids:
            print(f"project_ids: {','.join(str(pid) for pid in project_ids)}")
        return 0

    if args.command == "list-projects":
        settings = load_settings()
        projects = _resolve_discovery_projects(args, settings)
        if not projects:
            client = GitLabSourceClient(settings)
            projects = client.list_accessible_projects()
        all_ids = [int(p["id"]) for p in projects]
        selected_ids = _slice_project_ids(
            all_ids,
            start_index=args.project_start_index,
            count=args.project_count,
        )
        selected_set = set(selected_ids)
        print(f"Project window: start={args.project_start_index}, count={args.project_count or 'ALL'}")
        print("index\tproject_id\tpath_with_namespace\tname")
        for idx, project in enumerate(projects, start=1):
            pid = int(project["id"])
            if pid in selected_set:
                print(f"{idx}\t{pid}\t{project.get('path_with_namespace','')}\t{project.get('name','')}")
        return 0

    if args.command == "view":
        db.init_schema()
        run_viewer(db_path=partial.db_path, host=args.host, port=args.port)
        return 0

    if args.command == "enrich" and args.enrich_command == "qodo":
        project_ids = _resolve_project_scope_ids(args)
        tools = _parse_tools(args.tools)
        opts = EnrichOptions(
            output_root=args.output_root,
            concurrency=args.concurrency,
            mr_limit=args.mr_limit,
            only_missing=args.only_missing,
            force=args.force,
            data_source=args.data_source,
            timeout_sec=args.timeout_sec,
            compact_max_tokens=args.compact_max_tokens,
            include_mermaid=args.include_mermaid,
            tools=tools,
            progress=not args.no_progress,
        )
        candidate_opts = CandidateOptions(
            mode=args.candidate_mode,
            count=args.candidate_count,
            scope=args.candidate_scope,
            type_balance=args.candidate_type_balance,
            data_source=args.candidate_data_source,
            preview=args.candidate_preview,
        )
        selected: list[dict[str, Any]] | None = None
        if candidate_opts.mode == "stratified":
            selected = select_enrich_candidates(db, project_ids, opts, candidate_opts)
            requested = max(1, int(candidate_opts.count))
            if selected:
                if len(selected) < requested:
                    print(
                        f"Candidate selection warning: requested={requested}, selected={len(selected)} "
                        f"(fewer eligible MRs than requested)"
                    )
                print(f"Candidate selection: selected={len(selected)}/{requested}")
            else:
                print("Candidate selection: selected=0")

            if candidate_opts.preview:
                if selected:
                    print("project_id\tmr_iid\tmr_id\tfinal_type\tcomplexity_score\tupdated_at\tweb_url")
                    for row in selected:
                        print(
                            f"{row['project_id']}\t{row['mr_iid']}\t{row['mr_id']}\t{row.get('final_type') or ''}\t"
                            f"{row.get('complexity_score')}\t{row.get('updated_at') or ''}\t{row.get('web_url') or ''}"
                        )
                return 0

        total_eligible = 0
        total_success = 0
        total_failed = 0
        total_skipped = 0
        global_total_runs = (len(selected) * len(tools)) if selected is not None else None
        global_runs_done = 0

        selected_by_project: dict[int, list[dict[str, Any]]] = {}
        if selected is not None:
            for row in selected:
                selected_by_project.setdefault(int(row["project_id"]), []).append(row)
            run_project_ids = sorted(selected_by_project.keys())
        else:
            run_project_ids = project_ids

        def _global_progress(_res: dict[str, Any], _project_done: int, _project_total: int) -> None:
            nonlocal global_runs_done
            if global_total_runs is None or args.no_progress:
                return
            global_runs_done += 1
            print(f"[enrich] tool-run progress {global_runs_done}/{global_total_runs}")

        for project_id in run_project_ids:
            project_candidates = selected_by_project.get(project_id) if selected is not None else None
            result = enrich_qodo_project(db, project_id, opts, candidates=project_candidates, on_result=_global_progress)
            comp = compact_project_qodo(db, project_id, opts)
            total_eligible += result["eligible"]
            total_success += result["success"]
            total_failed += result["failed"]
            total_skipped += result["skipped"]
            print(
                f"[project {project_id}] tools={','.join(tools)} eligible={result['eligible']} success={result['success']} "
                f"failed={result['failed']} skipped={result['skipped']} compact={comp['compact_markdown_path']}"
            )
        print(
            f"Enrich total: eligible={total_eligible} success={total_success} failed={total_failed} skipped={total_skipped}"
        )
        return 0

    if args.command == "enrich" and args.enrich_command == "status":
        project_ids = _resolve_project_scope_ids(args)
        rows = get_enrich_status(db, project_ids, data_source=args.data_source)
        if args.format == "json":
            print(json.dumps(rows))
            return 0
        print("project_id\teligible\tenriched\tfailed\tcompact_markdown_path\toverview_mermaid_path\tcompacted_at")
        for row in rows:
            print(
                f"{row['project_id']}\t{row['eligible']}\t{row['enriched']}\t{row['failed']}\t"
                f"{row.get('compact_markdown_path') or ''}\t{row.get('overview_mermaid_path') or ''}\t"
                f"{row.get('compacted_at') or ''}"
            )
        return 0

    if args.command == "cleanup":
        db.init_schema()
        with db.connect() as conn:
            deleted = db.delete_merge_requests_by_source(
                conn,
                data_source=args.data_source,
                project_id=args.project_id,
            )
        scope = f"project {args.project_id}" if args.project_id is not None else "all projects"
        print(f"Deleted {deleted} merge requests for data_source={args.data_source} in {scope}")
        return 0

    parser.print_help(sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
