from __future__ import annotations

import argparse
import json
import re
import os
import shutil
import sys
from pathlib import Path
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
from prtool.classifier import CLASSIFIER_VERSION
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
from prtool.export import export_csv, export_jsonl, export_memory_csv, export_memory_jsonl
from prtool.gitlab_client import GitLabSourceClient
from prtool.pipeline import classify_project, sync_backfill, sync_refresh
from prtool.seed_data import seed_demo_data
from prtool.viewer import run_viewer
from prtool.memory import (
    BaselineBuildOptions,
    MRBuildOptions,
    MaterializeOptions,
    build_project_baseline,
    build_runtime_for_project,
    get_memory_status,
    materialize_project_markdown_from_db,
)

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_EXPORT_DIR = str(REPO_ROOT / "exports")
DEFAULT_QODO_OUTPUT_ROOT = str(REPO_ROOT / "outputs" / "qodo")
DEFAULT_MEMORY_OUTPUT_ROOT = str(REPO_ROOT / "outputs" / "memory")


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

    reclassify_cmd = sub.add_parser("reclassify")
    reclassify_cmd.add_argument("--project-id", type=int, action="append")
    reclassify_cmd.add_argument("--group-id", action="append")
    reclassify_cmd.add_argument("--all-projects", action="store_true")
    reclassify_cmd.add_argument("--project-start-index", type=int, default=1)
    reclassify_cmd.add_argument("--project-count", type=int)
    reclassify_cmd.add_argument("--only-stale", action=argparse.BooleanOptionalAction, default=True)
    reclassify_cmd.add_argument("--force", action="store_true")
    reclassify_cmd.add_argument(
        "--qodo-inline",
        action=argparse.BooleanOptionalAction,
        default=os.getenv("QODO_INLINE_ENABLED", "false").strip().lower() in {"1", "true", "yes", "on"},
        help="Run threshold-based Qodo enrichment inline before reclassification",
    )
    reclassify_cmd.add_argument("--qodo-min-confidence", type=float, default=float(os.getenv("QODO_TRIGGER_MIN_CONF", "0.70")))
    reclassify_cmd.add_argument("--qodo-max-confidence", type=float, default=float(os.getenv("QODO_TRIGGER_MAX_CONF", "0.75")))
    reclassify_cmd.add_argument("--qodo-reasons", default=os.getenv("QODO_TRIGGER_REASONS", "missing_description,low_top2_margin"))
    reclassify_cmd.add_argument(
        "--qodo-require-empty-description",
        action=argparse.BooleanOptionalAction,
        default=os.getenv("QODO_REQUIRE_EMPTY_DESCRIPTION", "false").strip().lower() in {"1", "true", "yes", "on"},
    )
    reclassify_cmd.add_argument("--qodo-mr-limit", type=int)
    reclassify_cmd.add_argument("--qodo-tools", default=os.getenv("QODO_INLINE_TOOLS", "describe"))
    reclassify_cmd.add_argument("--qodo-concurrency", type=int, default=int(os.getenv("QODO_INLINE_CONCURRENCY", "5")))
    reclassify_cmd.add_argument("--qodo-timeout-sec", type=int, default=int(os.getenv("QODO_INLINE_TIMEOUT_SEC", "180")))
    reclassify_cmd.add_argument(
        "--qodo-only-missing",
        action=argparse.BooleanOptionalAction,
        default=os.getenv("QODO_INLINE_ONLY_MISSING", "true").strip().lower() in {"1", "true", "yes", "on"},
    )
    reclassify_cmd.add_argument("--qodo-output-root", default=DEFAULT_QODO_OUTPUT_ROOT)

    export_cmd = sub.add_parser("export")
    export_cmd.add_argument("--format", choices=["csv", "jsonl", "both"], default="both")
    export_cmd.add_argument("--project-id", type=int, action="append")
    export_cmd.add_argument("--group-id", action="append")
    export_cmd.add_argument("--all-projects", action="store_true")
    export_cmd.add_argument("--project-start-index", type=int, default=1)
    export_cmd.add_argument("--project-count", type=int)
    export_cmd.add_argument("--out-dir", default=DEFAULT_EXPORT_DIR)

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
    qodo_cmd.add_argument("--output-root", default=DEFAULT_QODO_OUTPUT_ROOT)
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

    qodo_threshold_cmd = enrich_sub.add_parser("qodo-threshold")
    qodo_threshold_cmd.add_argument("--project-id", type=int, action="append")
    qodo_threshold_cmd.add_argument("--group-id", action="append")
    qodo_threshold_cmd.add_argument("--all-projects", action="store_true")
    qodo_threshold_cmd.add_argument("--project-start-index", type=int, default=1)
    qodo_threshold_cmd.add_argument("--project-count", type=int)
    qodo_threshold_cmd.add_argument("--min-confidence", type=float, default=float(os.getenv("QODO_TRIGGER_MIN_CONF", "0.70")))
    qodo_threshold_cmd.add_argument("--max-confidence", type=float, default=float(os.getenv("QODO_TRIGGER_MAX_CONF", "0.75")))
    qodo_threshold_cmd.add_argument(
        "--reasons",
        default=os.getenv("QODO_TRIGGER_REASONS", "missing_description,low_top2_margin"),
        help="Comma-separated why_needs_review reasons to include; empty means no reason filter",
    )
    qodo_threshold_cmd.add_argument(
        "--require-empty-description",
        action=argparse.BooleanOptionalAction,
        default=os.getenv("QODO_REQUIRE_EMPTY_DESCRIPTION", "false").strip().lower() in {"1", "true", "yes", "on"},
    )
    qodo_threshold_cmd.add_argument("--mr-limit", type=int)
    qodo_threshold_cmd.add_argument("--concurrency", type=int, default=5)
    qodo_threshold_cmd.add_argument("--only-missing", action="store_true", default=True)
    qodo_threshold_cmd.add_argument("--force", action="store_true")
    qodo_threshold_cmd.add_argument("--data-source", choices=["production", "test", "all"], default="production")
    qodo_threshold_cmd.add_argument("--output-root", default=DEFAULT_QODO_OUTPUT_ROOT)
    qodo_threshold_cmd.add_argument("--compact-max-tokens", type=int, default=3000)
    qodo_threshold_cmd.add_argument("--include-mermaid", action="store_true", default=True)
    qodo_threshold_cmd.add_argument("--timeout-sec", type=int, default=180)
    qodo_threshold_cmd.add_argument("--tools", default="describe", help="Comma-separated tools: describe,review,improve")
    qodo_threshold_cmd.add_argument("--no-progress", action="store_true")
    qodo_threshold_cmd.add_argument("--dry-run", action="store_true")

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


    memory_cmd = sub.add_parser("memory")
    memory_sub = memory_cmd.add_subparsers(dest="memory_command", required=True)

    baseline_cmd = memory_sub.add_parser("baseline-build")
    baseline_cmd.add_argument("--project-id", type=int, action="append")
    baseline_cmd.add_argument("--group-id", action="append")
    baseline_cmd.add_argument("--all-projects", action="store_true")
    baseline_cmd.add_argument("--project-start-index", type=int, default=1)
    baseline_cmd.add_argument("--project-count", type=int)
    baseline_cmd.add_argument("--output-root", default=DEFAULT_MEMORY_OUTPUT_ROOT)
    baseline_cmd.add_argument("--data-source", choices=["production", "test", "all"], default="production")
    baseline_cmd.add_argument("--history-window-months", type=int, default=12)
    baseline_cmd.add_argument("--db-only", action="store_true")

    runtime_cmd = memory_sub.add_parser("mr-build")
    runtime_cmd.add_argument("--project-id", type=int, action="append")
    runtime_cmd.add_argument("--group-id", action="append")
    runtime_cmd.add_argument("--all-projects", action="store_true")
    runtime_cmd.add_argument("--project-start-index", type=int, default=1)
    runtime_cmd.add_argument("--project-count", type=int)
    runtime_cmd.add_argument("--output-root", default=DEFAULT_MEMORY_OUTPUT_ROOT)
    runtime_cmd.add_argument("--data-source", choices=["production", "test", "all"], default="production")
    runtime_cmd.add_argument("--include-similar-limit", type=int, default=5)
    runtime_cmd.add_argument("--mr-limit", type=int)
    runtime_cmd.add_argument("--compose", action=argparse.BooleanOptionalAction, default=True)
    runtime_cmd.add_argument("--only-missing", action="store_true", default=True)
    runtime_cmd.add_argument("--force", action="store_true")
    runtime_cmd.add_argument("--db-only", action="store_true")
    runtime_cmd.add_argument("--outcome-mode", choices=["template", "semantic-local"], default="template")

    memory_status_cmd = memory_sub.add_parser("status")
    memory_status_cmd.add_argument("--project-id", type=int, action="append")
    memory_status_cmd.add_argument("--group-id", action="append")
    memory_status_cmd.add_argument("--all-projects", action="store_true")
    memory_status_cmd.add_argument("--project-start-index", type=int, default=1)
    memory_status_cmd.add_argument("--project-count", type=int)
    memory_status_cmd.add_argument("--data-source", choices=["production", "test", "all"], default="production")
    memory_status_cmd.add_argument("--format", choices=["text", "json"], default="text")

    memory_export_cmd = memory_sub.add_parser("export")
    memory_export_cmd.add_argument("--project-id", type=int, action="append")
    memory_export_cmd.add_argument("--group-id", action="append")
    memory_export_cmd.add_argument("--all-projects", action="store_true")
    memory_export_cmd.add_argument("--project-start-index", type=int, default=1)
    memory_export_cmd.add_argument("--project-count", type=int)
    memory_export_cmd.add_argument("--format", choices=["csv", "jsonl", "both"], default="both")
    memory_export_cmd.add_argument("--out-dir", default=DEFAULT_EXPORT_DIR)

    memory_materialize_cmd = memory_sub.add_parser("materialize")
    memory_materialize_cmd.add_argument("--project-id", type=int, action="append")
    memory_materialize_cmd.add_argument("--group-id", action="append")
    memory_materialize_cmd.add_argument("--all-projects", action="store_true")
    memory_materialize_cmd.add_argument("--project-start-index", type=int, default=1)
    memory_materialize_cmd.add_argument("--project-count", type=int)
    memory_materialize_cmd.add_argument("--output-root", default=DEFAULT_MEMORY_OUTPUT_ROOT)
    memory_materialize_cmd.add_argument("--data-source", choices=["production", "test", "all"], default="production")
    memory_materialize_cmd.add_argument("--mr-limit", type=int)
    memory_materialize_cmd.add_argument("--compose", action=argparse.BooleanOptionalAction, default=True)
    memory_materialize_cmd.add_argument("--only-missing", action="store_true", default=True)
    memory_materialize_cmd.add_argument("--force", action="store_true")

    cleanup_cmd = sub.add_parser("cleanup")
    cleanup_cmd.add_argument("--data-source", choices=["test", "production"], default="test")
    cleanup_cmd.add_argument("--project-id", type=int)
    cleanup_cmd.add_argument("--artifacts", action="store_true", help="Delete generated artifact directories")
    cleanup_cmd.add_argument("--target", choices=["outputs", "exports", "all"], default="outputs")
    cleanup_cmd.add_argument("--yes", action="store_true", help="Confirm deletion when --artifacts is used")

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


def _cleanup_artifacts(target: str, yes: bool) -> int:
    if not yes:
        print("Error: --yes is required with --artifacts to confirm deletion", file=sys.stderr)
        return 1

    targets: list[Path]
    if target == "all":
        targets = [Path(DEFAULT_EXPORT_DIR), Path(DEFAULT_QODO_OUTPUT_ROOT), Path(DEFAULT_MEMORY_OUTPUT_ROOT)]
    elif target == "exports":
        targets = [Path(DEFAULT_EXPORT_DIR)]
    else:
        targets = [Path(DEFAULT_QODO_OUTPUT_ROOT), Path(DEFAULT_MEMORY_OUTPUT_ROOT)]

    seen: set[Path] = set()
    normalized_targets: list[Path] = []
    for p in targets:
        p = p.resolve()
        if p not in seen:
            seen.add(p)
            normalized_targets.append(p)

    deleted_paths: list[str] = []
    for path in normalized_targets:
        if path.exists():
            shutil.rmtree(path)
            deleted_paths.append(str(path))
        path.mkdir(parents=True, exist_ok=True)

    if deleted_paths:
        print("Deleted artifact roots:")
        for path in deleted_paths:
            print(f"- {path}")
    else:
        print("No artifact roots existed; created empty target directories.")
    return 0

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



def _safe_filename_tag(raw: str) -> str:
    tag = re.sub(r"[^A-Za-z0-9._-]+", "_", (raw or "").strip())
    return tag.strip("_") or "scope"


def _resolve_export_stem(args: argparse.Namespace) -> str:
    groups = resolve_group_ids(getattr(args, "group_id", None))
    if groups:
        if len(groups) == 1:
            return f"mr_classification_{_safe_filename_tag(groups[0])}"
        joined = "_".join(_safe_filename_tag(g) for g in groups[:2])
        return f"mr_classification_{joined}_and_{len(groups)}_groups"

    explicit_projects = getattr(args, "project_id", None) or []
    if explicit_projects and len(explicit_projects) == 1:
        return f"mr_classification_project_{int(explicit_projects[0])}"
    return "mr_classification"


def _parse_reason_filter(raw: str) -> tuple[str, ...]:
    parts = [p.strip() for p in (raw or "").split(",") if p.strip()]
    deduped: list[str] = []
    for p in parts:
        if p not in deduped:
            deduped.append(p)
    return tuple(deduped)


def _needs_review_stats(
    db: Database,
    project_ids: list[int],
    data_source: str,
) -> dict[str, float]:
    if not project_ids:
        return {"total": 0, "needs_review": 0, "needs_review_pct": 0.0}
    with db.connect() as conn:
        pid_placeholders = ",".join(["?"] * len(project_ids))
        params: list[Any] = [int(v) for v in project_ids]
        source_filter = ""
        if data_source != "all":
            source_filter = " AND m.data_source = ?"
            params.append(data_source)
        row = conn.execute(
            f"""
            SELECT
              COUNT(*) AS total,
              SUM(CASE WHEN c.needs_review = 1 THEN 1 ELSE 0 END) AS needs_review
            FROM mr_classifications c
            JOIN merge_requests m ON m.id = c.mr_id
            WHERE m.project_id IN ({pid_placeholders})
              {source_filter}
            """,
            tuple(params),
        ).fetchone()
    total = int((row["total"] if row else 0) or 0)
    needs_review = int((row["needs_review"] if row else 0) or 0)
    pct = round((100.0 * needs_review / total), 2) if total > 0 else 0.0
    return {"total": total, "needs_review": needs_review, "needs_review_pct": pct}


def _select_qodo_threshold_candidates(
    db: Database,
    project_ids: list[int],
    min_confidence: float,
    max_confidence: float,
    reasons: tuple[str, ...],
    require_empty_description: bool,
    data_source: str,
    tools: tuple[str, ...],
    only_missing: bool,
    force: bool,
    mr_limit: int | None,
) -> list[dict[str, Any]]:
    if not project_ids:
        return []
    with db.connect() as conn:
        pid_placeholders = ",".join(["?"] * len(project_ids))
        params: list[Any] = [int(v) for v in project_ids]
        clauses = [
            f"m.project_id IN ({pid_placeholders})",
            "m.web_url IS NOT NULL",
            "m.web_url != ''",
            "c.needs_review = 1",
            "c.classification_confidence >= ?",
            "c.classification_confidence < ?",
        ]
        params.extend([float(min_confidence), float(max_confidence)])
        if data_source != "all":
            clauses.append("m.data_source = ?")
            params.append(data_source)
        if require_empty_description:
            clauses.append("TRIM(COALESCE(m.description, '')) = ''")
        if reasons:
            r_placeholders = ",".join(["?"] * len(reasons))
            clauses.append(
                f"""EXISTS (
                    SELECT 1
                    FROM json_each(c.classification_rationale_json, '$.why_needs_review') j
                    WHERE j.value IN ({r_placeholders})
                )"""
            )
            params.extend(list(reasons))
        if only_missing and not force:
            t_placeholders = ",".join(["?"] * len(tools))
            clauses.append(
                f"""(
                    SELECT COUNT(*)
                    FROM mr_qodo_artifacts qa
                    WHERE qa.mr_id = m.id AND qa.tool IN ({t_placeholders})
                ) < {len(tools)}"""
            )
            params.extend(list(tools))

        where = " AND ".join(clauses)
        limit_sql = ""
        if mr_limit is not None:
            limit_sql = " LIMIT ?"
            params.append(int(mr_limit))
        rows = conn.execute(
            f"""
            SELECT
              m.id,
              m.project_id,
              m.iid,
              m.web_url,
              m.updated_at,
              c.final_type,
              c.classification_confidence,
              c.needs_review,
              c.classifier_version,
              TRIM(COALESCE(m.description, '')) = '' AS has_empty_description
            FROM mr_classifications c
            JOIN merge_requests m ON m.id = c.mr_id
            WHERE {where}
            ORDER BY c.classification_confidence DESC, m.updated_at DESC
            {limit_sql}
            """,
            tuple(params),
        ).fetchall()
    return [dict(r) for r in rows]
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

    if args.command == "reclassify":
        db.init_schema()
        project_ids = _resolve_classify_project_ids(args, db)
        only_stale = False if args.force else bool(args.only_stale)
        mode = f"only-stale (version={CLASSIFIER_VERSION})" if only_stale else "force-all"
        qodo_inline = bool(args.qodo_inline)
        print(f"Selected projects ({len(project_ids)}): {project_ids} | mode={mode} | qodo_inline={qodo_inline}")

        if qodo_inline:
            if not (0.0 <= float(args.qodo_min_confidence) < float(args.qodo_max_confidence) <= 1.0):
                raise ValueError("--qodo-min-confidence and --qodo-max-confidence must satisfy 0 <= min < max <= 1")

            qodo_tools = _parse_tools(args.qodo_tools)
            if "describe" not in qodo_tools:
                qodo_tools = ("describe",) + tuple(t for t in qodo_tools if t != "describe")
            qodo_reasons = _parse_reason_filter(args.qodo_reasons)
            qodo_opts = EnrichOptions(
                output_root=args.qodo_output_root,
                concurrency=args.qodo_concurrency,
                mr_limit=args.qodo_mr_limit,
                only_missing=bool(args.qodo_only_missing),
                force=False,
                data_source="production",
                timeout_sec=args.qodo_timeout_sec,
                compact_max_tokens=3000,
                include_mermaid=True,
                tools=qodo_tools,
                progress=True,
            )
            qodo_candidates = _select_qodo_threshold_candidates(
                db,
                project_ids=project_ids,
                min_confidence=float(args.qodo_min_confidence),
                max_confidence=float(args.qodo_max_confidence),
                reasons=qodo_reasons,
                require_empty_description=bool(args.qodo_require_empty_description),
                data_source="production",
                tools=qodo_tools,
                only_missing=bool(args.qodo_only_missing),
                force=False,
                mr_limit=args.qodo_mr_limit,
            )
            print(
                f"[qodo-inline] selected={len(qodo_candidates)} min_conf={float(args.qodo_min_confidence):.3f} "
                f"max_conf={float(args.qodo_max_confidence):.3f} reasons={','.join(qodo_reasons) if qodo_reasons else 'ALL'} "
                f"require_empty_description={bool(args.qodo_require_empty_description)}"
            )
            if qodo_candidates:
                selected_by_project: dict[int, list[dict[str, Any]]] = {}
                for row in qodo_candidates:
                    selected_by_project.setdefault(int(row["project_id"]), []).append(row)
                q_eligible = 0
                q_success = 0
                q_failed = 0
                q_skipped = 0
                for project_id in sorted(selected_by_project.keys()):
                    result = enrich_qodo_project(db, project_id, qodo_opts, candidates=selected_by_project[project_id])
                    compact_project_qodo(db, project_id, qodo_opts)
                    q_eligible += int(result["eligible"])
                    q_success += int(result["success"])
                    q_failed += int(result["failed"])
                    q_skipped += int(result["skipped"])
                print(
                    f"[qodo-inline] total eligible={q_eligible} success={q_success} failed={q_failed} skipped={q_skipped}"
                )
            else:
                print("[qodo-inline] No eligible candidates; proceeding to reclassification.")

        total = 0
        for project_id in project_ids:
            count = classify_project(
                db,
                partial,
                project_id,
                only_stale=only_stale,
                target_classifier_version=CLASSIFIER_VERSION,
            )
            total += count
            print(f"[project {project_id}] Reclassification complete: {count} merge requests processed")
        print(f"Reclassification total across projects: {total}")
        return 0

    if args.command == "export":
        db.init_schema()
        project_ids: list[int] | None = None
        if getattr(args, "project_id", None) or getattr(args, "group_id", None) or getattr(args, "all_projects", False):
            project_ids = _resolve_project_scope_ids(args)
        filename_stem = _resolve_export_stem(args)
        outputs: list[str] = []
        if args.format in ("csv", "both"):
            outputs.append(str(export_csv(db, out_dir=args.out_dir, project_ids=project_ids, filename_stem=filename_stem)))
        if args.format in ("jsonl", "both"):
            outputs.append(str(export_jsonl(db, out_dir=args.out_dir, project_ids=project_ids, filename_stem=filename_stem)))
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

    if args.command == "enrich" and args.enrich_command == "qodo-threshold":
        if not (0.0 <= float(args.min_confidence) < float(args.max_confidence) <= 1.0):
            raise ValueError("--min-confidence and --max-confidence must satisfy 0 <= min < max <= 1")

        db.init_schema()
        project_ids = _resolve_project_scope_ids(args)
        reasons = _parse_reason_filter(args.reasons)
        tools = _parse_tools(args.tools)
        if "describe" not in tools:
            tools = ("describe",) + tuple(t for t in tools if t != "describe")

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

        before_scope = _needs_review_stats(db, project_ids, data_source=args.data_source)
        candidates = _select_qodo_threshold_candidates(
            db,
            project_ids=project_ids,
            min_confidence=float(args.min_confidence),
            max_confidence=float(args.max_confidence),
            reasons=reasons,
            require_empty_description=bool(args.require_empty_description),
            data_source=args.data_source,
            tools=tools,
            only_missing=args.only_missing,
            force=args.force,
            mr_limit=args.mr_limit,
        )
        print(
            f"Threshold selection: projects={len(project_ids)} min_conf={float(args.min_confidence):.3f} "
            f"max_conf={float(args.max_confidence):.3f} reasons={','.join(reasons) if reasons else 'ALL'} "
            f"require_empty_description={bool(args.require_empty_description)} selected={len(candidates)}"
        )

        if not candidates:
            print("No eligible candidates in threshold band. Nothing to run.")
            return 0

        print("project_id\tmr_iid\tmr_id\tconfidence\tfinal_type\tempty_description\tupdated_at\tweb_url")
        for row in candidates:
            print(
                f"{row['project_id']}\t{row['iid']}\t{row['id']}\t{float(row['classification_confidence']):.3f}\t"
                f"{row.get('final_type') or ''}\t{int(row.get('has_empty_description') or 0)}\t"
                f"{row.get('updated_at') or ''}\t{row.get('web_url') or ''}"
            )

        if args.dry_run:
            print("Dry-run only; skipped enrichment and reclassification.")
            return 0

        candidate_ids = [int(r["id"]) for r in candidates]
        before_candidate_state = {int(r["id"]): (float(r["classification_confidence"]), int(r["needs_review"])) for r in candidates}

        selected_by_project: dict[int, list[dict[str, Any]]] = {}
        candidate_ids_by_project: dict[int, list[int]] = {}
        for row in candidates:
            pid = int(row["project_id"])
            selected_by_project.setdefault(pid, []).append(row)
            candidate_ids_by_project.setdefault(pid, []).append(int(row["id"]))

        total_eligible = 0
        total_success = 0
        total_failed = 0
        total_skipped = 0
        for project_id in sorted(selected_by_project.keys()):
            project_candidates = selected_by_project[project_id]
            result = enrich_qodo_project(db, project_id, opts, candidates=project_candidates)
            comp = compact_project_qodo(db, project_id, opts)
            total_eligible += result["eligible"]
            total_success += result["success"]
            total_failed += result["failed"]
            total_skipped += result["skipped"]
            print(
                f"[project {project_id}] tools={','.join(tools)} eligible={result['eligible']} success={result['success']} "
                f"failed={result['failed']} skipped={result['skipped']} compact={comp['compact_markdown_path']}"
            )

        reclassified_total = 0
        for project_id in sorted(candidate_ids_by_project.keys()):
            count = classify_project(
                db,
                partial,
                project_id,
                only_stale=False,
                target_classifier_version=CLASSIFIER_VERSION,
                mr_ids=candidate_ids_by_project[project_id],
            )
            reclassified_total += count
            print(f"[project {project_id}] Reclassification complete: {count} targeted merge requests processed")

        after_scope = _needs_review_stats(db, project_ids, data_source=args.data_source)

        after_candidate_state: dict[int, tuple[float, int]] = {}
        with db.connect() as conn:
            id_placeholders = ",".join(["?"] * len(candidate_ids))
            rows = conn.execute(
                f"""
                SELECT mr_id, classification_confidence, needs_review
                FROM mr_classifications
                WHERE mr_id IN ({id_placeholders})
                """,
                tuple(candidate_ids),
            ).fetchall()
            after_candidate_state = {
                int(r["mr_id"]): (float(r["classification_confidence"]), int(r["needs_review"]))
                for r in rows
            }

        promoted = 0
        improved = 0
        total_conf_delta = 0.0
        for mr_id, (before_conf, before_nr) in before_candidate_state.items():
            after_conf, after_nr = after_candidate_state.get(mr_id, (before_conf, before_nr))
            if after_conf > before_conf:
                improved += 1
            if before_nr == 1 and after_nr == 0:
                promoted += 1
            total_conf_delta += after_conf - before_conf
        avg_delta = round(total_conf_delta / len(before_candidate_state), 4) if before_candidate_state else 0.0

        print(
            f"Threshold enrich total: eligible={total_eligible} success={total_success} "
            f"failed={total_failed} skipped={total_skipped} reclassified={reclassified_total}"
        )
        print(
            f"Candidate impact: promoted_above_threshold={promoted} improved_confidence={improved}/{len(before_candidate_state)} "
            f"avg_conf_delta={avg_delta:+.4f}"
        )
        print(
            f"Scope needs_review: before={before_scope['needs_review']}/{before_scope['total']} ({before_scope['needs_review_pct']:.2f}%) "
            f"after={after_scope['needs_review']}/{after_scope['total']} ({after_scope['needs_review_pct']:.2f}%) "
            f"delta={(after_scope['needs_review_pct'] - before_scope['needs_review_pct']):+.2f}pp"
        )
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



    if args.command == "memory" and args.memory_command == "baseline-build":
        db.init_schema()
        project_ids = _resolve_project_scope_ids(args)
        print(f"Selected projects ({len(project_ids)}): {project_ids}")
        total = 0
        for project_id in project_ids:
            row = build_project_baseline(
                db,
                project_id,
                BaselineBuildOptions(
                    output_root=args.output_root,
                    data_source=args.data_source,
                    history_window_months=args.history_window_months,
                    db_only=args.db_only,
                ),
            )
            total += 1
            print(
                f"[project {project_id}] Baseline built: sample_size={row['sample_size']} path={row['markdown_path']}"
            )
        print(f"Baseline total across projects: {total}")
        return 0

    if args.command == "memory" and args.memory_command == "mr-build":
        db.init_schema()
        project_ids = _resolve_project_scope_ids(args)
        print(f"Selected projects ({len(project_ids)}): {project_ids}")
        total_eligible = 0
        total_success = 0
        total_failed = 0
        total_skipped = 0
        for project_id in project_ids:
            result = build_runtime_for_project(
                db,
                project_id,
                MRBuildOptions(
                    output_root=args.output_root,
                    data_source=args.data_source,
                    include_similar_limit=args.include_similar_limit,
                    compose=args.compose,
                    only_missing=args.only_missing and not args.force,
                    force=args.force,
                    mr_limit=args.mr_limit,
                    db_only=args.db_only,
                    outcome_mode=args.outcome_mode,
                ),
            )
            total_eligible += int(result['eligible'])
            total_success += int(result['success'])
            total_failed += int(result['failed'])
            total_skipped += int(result['skipped'])
            print(
                f"[project {project_id}] Memory runtime complete: "
                f"eligible={result['eligible']} success={result['success']} "
                f"failed={result['failed']} skipped={result['skipped']}"
            )
        print(
            f"Memory runtime total: eligible={total_eligible} success={total_success} "
            f"failed={total_failed} skipped={total_skipped}"
        )
        return 0

    if args.command == "memory" and args.memory_command == "status":
        db.init_schema()
        project_ids = _resolve_project_scope_ids(args)
        rows = get_memory_status(db, project_ids, data_source=args.data_source)
        if args.format == "json":
            print(json.dumps(rows))
            return 0
        print("project_id	eligible	scored	memory_updated_at	baseline_sample_size	baseline_markdown_path	baseline_updated_at")
        for row in rows:
            print(
                f"{row['project_id']}	{row['eligible']}	{row['scored']}	"
                f"{row.get('memory_updated_at') or ''}	{row.get('baseline_sample_size') or 0}	"
                f"{row.get('baseline_markdown_path') or ''}	{row.get('baseline_updated_at') or ''}"
            )
        return 0



    if args.command == "memory" and args.memory_command == "export":
        db.init_schema()
        project_ids: list[int] | None = None
        if getattr(args, "project_id", None) or getattr(args, "group_id", None) or getattr(args, "all_projects", False):
            project_ids = _resolve_project_scope_ids(args)
        base_stem = _resolve_export_stem(args).replace("mr_classification", "mr_memory")
        outputs: list[str] = []
        if args.format in ("csv", "both"):
            outputs.append(str(export_memory_csv(db, out_dir=args.out_dir, project_ids=project_ids, filename_stem=base_stem)))
        if args.format in ("jsonl", "both"):
            outputs.append(str(export_memory_jsonl(db, out_dir=args.out_dir, project_ids=project_ids, filename_stem=base_stem)))
        print("Memory exported:\n" + "\n".join(outputs))
        return 0

    if args.command == "memory" and args.memory_command == "materialize":
        db.init_schema()
        project_ids = _resolve_project_scope_ids(args)
        print(f"Selected projects ({len(project_ids)}): {project_ids}")
        total_baseline_written = 0
        total_runtime_eligible = 0
        total_runtime_written = 0
        total_runtime_skipped = 0
        for project_id in project_ids:
            result = materialize_project_markdown_from_db(
                db,
                project_id,
                MaterializeOptions(
                    output_root=args.output_root,
                    data_source=args.data_source,
                    compose=args.compose,
                    only_missing=args.only_missing and not args.force,
                    force=args.force,
                    mr_limit=args.mr_limit,
                ),
            )
            total_baseline_written += int(result["baseline_written"])
            total_runtime_eligible += int(result["runtime_eligible"])
            total_runtime_written += int(result["runtime_written"])
            total_runtime_skipped += int(result["runtime_skipped"])
            print(
                f"[project {project_id}] Materialize complete: "
                f"baseline_written={result['baseline_written']} "
                f"runtime_eligible={result['runtime_eligible']} "
                f"runtime_written={result['runtime_written']} "
                f"runtime_skipped={result['runtime_skipped']}"
            )
        print(
            f"Materialize total: baseline_written={total_baseline_written} "
            f"runtime_eligible={total_runtime_eligible} "
            f"runtime_written={total_runtime_written} "
            f"runtime_skipped={total_runtime_skipped}"
        )
        return 0

    if args.command == "cleanup":
        if args.artifacts:
            return _cleanup_artifacts(target=args.target, yes=args.yes)
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
