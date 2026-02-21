from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from prtool.db import Database

MEMORY_SCORE_VERSION = "memory-v1"

RISKY_CAPABILITY_WEIGHTS = {
    "infra": 0.08,
    "security": 0.12,
    "auth": 0.1,
    "payment": 0.1,
    "redis": 0.06,
    "kafka": 0.06,
}


@dataclass(frozen=True)
class BaselineBuildOptions:
    output_root: str = "outputs/memory"
    data_source: str = "production"
    history_window_months: int = 12
    db_only: bool = False


@dataclass(frozen=True)
class MRBuildOptions:
    output_root: str = "outputs/memory"
    data_source: str = "production"
    include_similar_limit: int = 5
    compose: bool = True
    only_missing: bool = True
    force: bool = False
    mr_limit: int | None = None
    db_only: bool = False
    outcome_mode: str = "template"


@dataclass(frozen=True)
class MaterializeOptions:
    output_root: str = "outputs/memory"
    data_source: str = "production"
    compose: bool = True
    only_missing: bool = True
    force: bool = False
    mr_limit: int | None = None


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _clamp(v: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, v))


def _extract_group_path(web_url: str | None) -> str | None:
    if not web_url or "/-/merge_requests/" not in web_url:
        return None
    parts = web_url.split("/", 3)
    if len(parts) < 4:
        return None
    path = parts[3]
    ns = path.split("/-/merge_requests/")[0]
    segs = [s for s in ns.split("/") if s]
    if len(segs) < 2:
        return None
    return "/".join(segs[:-1])


def _time_cutoff(months: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(days=max(1, months) * 30)).isoformat()


def _parse_json_list(raw: str | None) -> list[str]:
    if not raw:
        return []
    try:
        data = json.loads(raw)
    except Exception:
        return []
    if not isinstance(data, list):
        return []
    return [str(x).strip().lower() for x in data if str(x).strip()]


def _infer_capabilities(title: str, description: str | None, existing: list[str]) -> list[str]:
    text = f"{title} {description or ''}".lower()
    caps = set(existing)
    if any(k in text for k in ("auth", "oauth", "jwt", "session")):
        caps.add("auth")
    if any(k in text for k in ("security", "snyk", "vuln", "cve")):
        caps.add("security")
    if any(k in text for k in ("payment", "checkout", "cart")):
        caps.add("payment")
    if "redis" in text:
        caps.add("redis")
    if "kafka" in text:
        caps.add("kafka")
    return sorted(caps)



def _tokenize(text: str) -> list[str]:
    return [t for t in re.findall(r"[a-z0-9]+", (text or "").lower()) if len(t) > 2]


def _text_similarity(a: str, b: str) -> float:
    ta = _tokenize(a)
    tb = _tokenize(b)
    if not ta or not tb:
        return 0.0
    sa = set(ta)
    sb = set(tb)
    inter = len(sa & sb)
    union = len(sa | sb)
    if union == 0:
        return 0.0
    return inter / union


def _extract_topic_labels(final_type: str, capability_tags: list[str], title: str, description: str | None) -> list[str]:
    labels: list[str] = []
    if final_type:
        labels.append(final_type)
    for tag in capability_tags:
        labels.append(tag)
    text = f"{title} {description or ''}".lower()
    if "redis" in text:
        labels.append("infra.redis")
    if any(k in text for k in ["auth", "oauth", "jwt", "session"]):
        labels.append("security.auth")
    if any(k in text for k in ["payment", "checkout", "cart"]):
        labels.append("payments.checkout")
    if any(k in text for k in ["pipeline", "deploy", "release", "ci"]):
        labels.append("ci.pipeline")
    out: list[str] = []
    for item in labels:
        item = str(item).strip().lower()
        if item and item not in out:
            out.append(item)
    return out[:6]


def _build_template_achieved_outcome(row: dict[str, Any], topic_labels: list[str]) -> tuple[str, list[str], float]:
    title = str(row.get("title") or "").strip()
    final_type = str(row.get("final_type") or "change")
    complexity = float(row.get("complexity_score") or 0.0)
    files = int(row.get("files_changed") or 0)
    churn = int(row.get("churn") or 0)
    prob = float(row.get("regression_probability") or 0.0)

    area = "core flows"
    if any(t.startswith("infra") for t in topic_labels):
        area = "infrastructure and delivery paths"
    elif any(t.startswith("security") for t in topic_labels):
        area = "security/auth paths"
    elif any(t.startswith("payments") for t in topic_labels):
        area = "checkout/payment paths"

    paragraph = (
        f"This MR delivers a {final_type} change focused on {area}. "
        f"It modifies {files} files with churn {churn} and complexity score {complexity:.2f}, "
        f"with estimated regression probability {prob:.2f}."
    )

    bullets = [
        f"Change intent: {title or 'update existing behavior'}",
        f"Primary impact area: {area}",
        f"Operational signal: regression_probability={prob:.2f}, review_depth={row.get('review_depth_required', 'standard')}",
    ]
    if topic_labels:
        bullets.append("Capability topics: " + ", ".join(topic_labels[:4]))

    quality = 0.45
    if title:
        quality += 0.15
    if files > 0:
        quality += 0.1
    if topic_labels:
        quality += 0.2
    if len(paragraph.split()) >= 18:
        quality += 0.1
    return paragraph, bullets[:6], _clamp(quality)


def _build_semantic_local_outcome(row: dict[str, Any], topic_labels: list[str]) -> tuple[str, list[str], float]:
    title = str(row.get("title") or "").strip()
    description = str(row.get("description") or "").strip()
    final_type = str(row.get("final_type") or "change")
    prob = float(row.get("regression_probability") or 0.0)
    depth = str(row.get("review_depth_required") or "standard")
    paths = [p for p in str(row.get("paths_text") or "").split() if "/" in p][:5]

    area = "application logic"
    if paths:
        first = paths[0]
        seg = first.split("/")[0]
        if seg and seg not in {"src", "app", "tests", "test", "lib"}:
            area = seg.replace("-", " ")
    if any(t.startswith("infra") for t in topic_labels):
        area = "infrastructure and delivery"
    elif any(t.startswith("security") for t in topic_labels):
        area = "authentication and security"
    elif any(t.startswith("payments") for t in topic_labels):
        area = "checkout and payments"

    intent = title or "Update behavior"
    if ":" in intent:
        intent = intent.split(":", 1)[1].strip() or intent

    detail = description.split(".")[0].strip() if description else ""
    if detail and len(detail) > 140:
        detail = detail[:140].rstrip() + "..."

    paragraph = f"{intent} in {area} as a {final_type} change."
    if detail:
        paragraph += f" {detail}."
    paragraph += f" Review posture is {depth} (regression probability {prob:.2f})."

    bullets = [
        f"Outcome: {intent}",
        f"Area: {area}",
        f"Review signal: depth={depth}, regression_probability={prob:.2f}",
    ]
    if paths:
        bullets.append("Touched paths: " + ", ".join(paths[:3]))
    if topic_labels:
        bullets.append("Capability topics: " + ", ".join(topic_labels[:4]))

    quality = 0.55
    if description:
        quality += 0.15
    if paths:
        quality += 0.15
    if topic_labels:
        quality += 0.1
    return paragraph, bullets[:6], _clamp(quality)


def _build_achieved_outcome(row: dict[str, Any], topic_labels: list[str], mode: str = "template") -> tuple[str, list[str], float]:
    if mode == "semantic-local":
        return _build_semantic_local_outcome(row, topic_labels)
    return _build_template_achieved_outcome(row, topic_labels)


def _similarity_text(row: dict[str, Any]) -> str:
    parts = [
        str(row.get("title") or ""),
        str(row.get("description") or ""),
        str(row.get("final_type") or ""),
        " ".join(_parse_json_list(row.get("capability_tags_json"))),
        str(row.get("paths_text") or ""),
    ]
    return " ".join(parts)


def _compute_regression_probability(row: dict[str, Any]) -> float:
    complexity_score = float(row.get("complexity_score") or 0.0)
    complexity_norm = _clamp(complexity_score / 10.0)
    files_changed = int(row.get("files_changed") or 0)
    churn = int(row.get("churn") or 0)
    unresolved = int(row.get("unresolved_thread_count") or 0)
    ci_failed = int(row.get("pipeline_failed_count") or 0)
    final_type = str(row.get("final_type") or "")

    change_surface = _clamp(0.55 * min(1.0, files_changed / 40.0) + 0.45 * min(1.0, churn / 2500.0))

    caps = _parse_json_list(row.get("capability_tags_json"))
    caps = _infer_capabilities(str(row.get("title") or ""), row.get("description"), caps)
    cap_weight = 0.0
    for cap in caps:
        cap_weight += RISKY_CAPABILITY_WEIGHTS.get(cap, 0.0)
    cap_weight = min(0.25, cap_weight)

    type_uplift = 0.0
    if final_type in {"infra", "perf-security"}:
        type_uplift += 0.1
    elif final_type == "refactor":
        type_uplift += 0.05

    stabilizer = 0.0
    if final_type in {"docs-only", "test-only"}:
        stabilizer += 0.2

    signal = (
        0.42 * complexity_norm
        + 0.28 * change_surface
        + 0.08 * min(1.0, unresolved / 5.0)
        + 0.08 * min(1.0, ci_failed / 3.0)
        + type_uplift
        + cap_weight
        - stabilizer
    )
    return round(_clamp(signal), 4)


def _review_depth_required(regression_probability: float, complexity_score: float, row: dict[str, Any]) -> tuple[str, float]:
    complexity_norm = _clamp(float(complexity_score) / 10.0)
    files_changed = int(row.get("files_changed") or 0)
    churn = int(row.get("churn") or 0)
    change_surface = _clamp(0.5 * min(1.0, files_changed / 40.0) + 0.5 * min(1.0, churn / 2500.0))
    depth_score = 0.45 * complexity_norm + 0.35 * regression_probability + 0.20 * change_surface
    if depth_score < 0.33:
        return "shallow", depth_score
    if depth_score <= 0.66:
        return "standard", depth_score
    return "deep", depth_score


def _risk_band(regression_probability: float, p50: float, p75: float, delta: float = 0.05) -> str:
    if regression_probability < (p50 - delta):
        return "below_baseline"
    if regression_probability <= p75:
        return "at_baseline"
    return "above_baseline"


def _ensure_project_dirs(output_root: str, project_id: int, mr_iid: int | None = None) -> Path:
    root = Path(output_root) / "projects" / str(project_id)
    if mr_iid is None:
        root.mkdir(parents=True, exist_ok=True)
        return root
    sub = root / "mrs" / str(mr_iid)
    sub.mkdir(parents=True, exist_ok=True)
    return sub


def _build_baseline_payload(rows: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(rows)
    dist: dict[str, int] = {}
    risk_by_type: dict[str, list[float]] = {}
    depth_dist = {"shallow": 0, "standard": 0, "deep": 0}
    risks: list[float] = []

    for row in rows:
        t = str(row.get("final_type") or "unknown")
        dist[t] = dist.get(t, 0) + 1
        r = float(row.get("regression_probability") or 0.0)
        risks.append(r)
        risk_by_type.setdefault(t, []).append(r)
        d = str(row.get("review_depth_required") or "standard")
        if d in depth_dist:
            depth_dist[d] += 1

    pct = {k: round((v / total) * 100.0, 2) for k, v in dist.items()} if total else {}
    avg_risk_by_type = {k: round(sum(v) / len(v), 4) for k, v in risk_by_type.items() if v}

    risks_sorted = sorted(risks)

    def _q(q: float) -> float:
        if not risks_sorted:
            return 0.0
        pos = int(round((len(risks_sorted) - 1) * q))
        return round(risks_sorted[pos], 4)

    depth_mix = {k: round((v / total) * 100.0, 2) for k, v in depth_dist.items()} if total else depth_dist
    return {
        "sample_size": total,
        "distribution": pct,
        "avg_risk_by_type": avg_risk_by_type,
        "depth_mix": depth_mix,
        "risk_percentiles": {"p50": _q(0.5), "p75": _q(0.75), "p90": _q(0.9)},
    }


def _render_project_memory_markdown(project_id: int, group_path: str | None, baseline: dict[str, Any], generated_at: str) -> str:
    lines = [
        "# Project Cognitive Memory",
        f"project_id: {project_id}",
        f"group: {group_path or ''}",
        f"generated_at: {generated_at}",
        f"score_version: {MEMORY_SCORE_VERSION}",
        "",
        "## 1) Historical Context",
        "### MR Type Distribution",
    ]
    dist = baseline.get("distribution", {})
    if dist:
        for k, v in sorted(dist.items(), key=lambda x: (-x[1], x[0])):
            lines.append(f"- {k}: {v}%")
    else:
        lines.append("- no data")

    lines.extend(["", "### Baseline Risk by Type",])
    avg_risk = baseline.get("avg_risk_by_type", {})
    if avg_risk:
        for k, v in sorted(avg_risk.items(), key=lambda x: (-x[1], x[0])):
            lines.append(f"- {k}: avg_risk={v}")
    else:
        lines.append("- no data")

    lines.extend(["", "### Baseline Review Depth Mix"])
    depth_mix = baseline.get("depth_mix", {})
    for key in ("shallow", "standard", "deep"):
        lines.append(f"- {key}: {depth_mix.get(key, 0)}%")

    lines.extend([
        "",
        "## 2) Historical Patterns (Feature Store Injection)",
        "- High-signal risky changes are inferred from capability tags + complexity + churn.",
        "- Stabilizing factors include docs/test-only classification and lower change surface.",
        "",
        "## 4) Review Playbook (Depth-Aware, Project-Level)",
        "### Shallow",
        "- Validate scope and intent are aligned.",
        "- Confirm no hidden infra/security side effects.",
        "### Standard",
        "- Review changed modules and dependency impacts.",
        "- Validate tests and CI behavior for changed paths.",
        "### Deep",
        "- Inspect rollback/failure modes and operational blast radius.",
        "- Validate edge-case tests and observability signals.",
        "",
        "## 6) Agent Guidance",
        "- Use this baseline as default context, then append MR runtime addendum.",
        "- If baseline sample size is low, trust MR-specific evidence more heavily.",
        "",
    ])
    return "\n".join(lines)


def _render_addendum(row: dict[str, Any], assessment: dict[str, Any], similar: list[dict[str, Any]]) -> str:
    lines = [
        f"# MR Runtime Addendum (!{row['iid']})",
        "",
        "## 3) Current MR Assessment",
        f"mr_id: {row['id']}",
        f"mr_iid: {row['iid']}",
        f"title: {row.get('title') or ''}",
        f"web_url: {row.get('web_url') or ''}",
        f"final_type: {row.get('final_type') or ''}",
        f"complexity_score: {float(row.get('complexity_score') or 0.0):.2f}",
        f"mr_outcome: {assessment['mr_outcome']}",
        f"regression_probability: {assessment['regression_probability']:.4f}",
        f"review_depth_required: {assessment['review_depth_required']}",
        "",
        "## 3.1) Achieved Outcome",
        str(assessment.get("mr_achieved_outcome") or "").strip() or "No concise achieved outcome available.",
        "",
        "### Outcome Highlights",
    ]

    outcome_bullets = assessment.get("mr_achieved_outcome_bullets", []) or []
    if outcome_bullets:
        for bullet in outcome_bullets[:6]:
            lines.append(f"- {bullet}")
    else:
        lines.append("- no additional highlights")

    lines.extend(["", "### Why"])
    reasons = assessment.get("reasons", [])
    if reasons:
        for reason in reasons[:5]:
            lines.append(f"- {reason}")
    else:
        lines.append("- no explicit risk markers")

    lines.extend(["", "### Capability Topics"])
    topic_labels = assessment.get("topic_labels", []) or []
    if topic_labels:
        lines.append("- " + ", ".join(topic_labels[:6]))
    else:
        lines.append("- none")

    lines.extend(["", "## 5) Similar Historical MRs"])
    if not similar:
        lines.append("- no similar historical MRs found")
    else:
        for item in similar[:10]:
            lines.append(
                f"- !{item['iid']} ({item.get('final_type')}, similarity={float(item.get('similarity_score') or 0.0):.2f}, "
                f"complexity={float(item.get('complexity_score') or 0.0):.2f})"
            )
    lines.append("")
    return "\n".join(lines)


def _load_project_baseline(conn: Any, project_id: int) -> tuple[dict[str, Any], str | None]:
    row = conn.execute(
        "SELECT baseline_json, markdown_path FROM project_memory_baseline WHERE project_id = ?",
        (project_id,),
    ).fetchone()
    if not row:
        return {}, None
    try:
        baseline = json.loads(row["baseline_json"])
    except Exception:
        baseline = {}
    return baseline, row["markdown_path"]


def _baseline_for_project(conn: Any, project_id: int, data_source: str, history_window_months: int) -> tuple[dict[str, Any], str | None]:
    cutoff = _time_cutoff(history_window_months)
    clauses = ["m.project_id = ?", "m.updated_at >= ?"]
    params: list[Any] = [project_id, cutoff]
    if data_source != "all":
        clauses.append("m.data_source = ?")
        params.append(data_source)
    where_sql = " AND ".join(clauses)
    rows = conn.execute(
        f"""
        SELECT c.final_type, COALESCE(r.regression_probability, c.complexity_score/10.0, 0.0) as regression_probability,
               COALESCE(r.review_depth_required, 'standard') as review_depth_required
        FROM merge_requests m
        JOIN mr_classifications c ON c.mr_id = m.id
        LEFT JOIN mr_memory_runtime r ON r.mr_id = m.id
        WHERE {where_sql}
        """,
        tuple(params),
    ).fetchall()
    baseline = _build_baseline_payload([dict(r) for r in rows])
    group_row = conn.execute(
        "SELECT web_url FROM merge_requests WHERE project_id = ? AND web_url IS NOT NULL LIMIT 1",
        (project_id,),
    ).fetchone()
    group_path = _extract_group_path(group_row["web_url"] if group_row else None)
    return baseline, group_path


def build_project_baseline(db: Database, project_id: int, opts: BaselineBuildOptions) -> dict[str, Any]:
    now = _now_iso()
    with db.connect() as conn:
        baseline, group_path = _baseline_for_project(conn, project_id, opts.data_source, opts.history_window_months)
        project_dir = _ensure_project_dirs(opts.output_root, project_id)
        md_path = project_dir / f"project_memory_{project_id}.md"
        rendered = _render_project_memory_markdown(project_id, group_path, baseline, now)
        if not opts.db_only:
            md_path.write_text(rendered, encoding="utf-8")
        row = {
            "project_id": project_id,
            "group_path": group_path,
            "history_window_months": opts.history_window_months,
            "sample_size": int(baseline.get("sample_size", 0)),
            "baseline_json": baseline,
            "markdown_path": str(md_path),
            "content_sha256": _sha(rendered),
            "generated_at": now,
            "updated_at": now,
        }
        db.upsert_project_memory_baseline(conn, row)
    return row


def _similar_rows(conn: Any, seed_row: dict[str, Any], project_id: int, mr_id: int, final_type: str, complexity_score: float, limit: int, data_source: str) -> list[dict[str, Any]]:
    clauses = ["m.project_id = ?", "m.id != ?"]
    params: list[Any] = [project_id, mr_id]
    if data_source != "all":
        clauses.append("m.data_source = ?")
        params.append(data_source)
    where_sql = " AND ".join(clauses)
    rows = conn.execute(
        f"""
        SELECT m.id, m.iid, m.title, m.description, c.final_type, c.complexity_score, c.capability_tags_json,
               COALESCE(r.regression_probability, c.complexity_score/10.0, 0.0) as regression_probability,
               COALESCE(r.mr_achieved_outcome, '') as mr_achieved_outcome,
               m.updated_at,
               (SELECT GROUP_CONCAT(path, ' ') FROM mr_files f WHERE f.mr_id = m.id) as paths_text
        FROM merge_requests m
        JOIN mr_classifications c ON c.mr_id = m.id
        LEFT JOIN mr_memory_runtime r ON r.mr_id = m.id
        WHERE {where_sql}
        ORDER BY m.updated_at DESC, m.id ASC
        LIMIT 300
        """,
        tuple(params),
    ).fetchall()

    seed_text = _similarity_text(seed_row)
    scored: list[dict[str, Any]] = []
    for raw in rows:
        item = dict(raw)
        item["similarity_score"] = round(_text_similarity(seed_text, _similarity_text(item)), 4)
        item["type_mismatch"] = 0 if str(item.get("final_type") or "") == final_type else 1
        item["complexity_distance"] = abs(float(item.get("complexity_score") or 0.0) - complexity_score)
        scored.append(item)

    scored.sort(
        key=lambda x: (
            -float(x.get("similarity_score") or 0.0),
            int(x.get("type_mismatch") or 1),
            float(x.get("complexity_distance") or 999.0),
            str(x.get("updated_at") or ""),
        ),
        reverse=False,
    )
    scored.sort(key=lambda x: -float(x.get("similarity_score") or 0.0))
    return scored[:limit]


def _eligible_runtime_rows(conn: Any, project_id: int, opts: MRBuildOptions) -> list[dict[str, Any]]:
    clauses = ["m.project_id = ?", "m.web_url IS NOT NULL"]
    params: list[Any] = [project_id]
    if opts.data_source != "all":
        clauses.append("m.data_source = ?")
        params.append(opts.data_source)
    if opts.only_missing and not opts.force:
        clauses.append("r.mr_id IS NULL")
    where_sql = " AND ".join(clauses)
    limit_sql = ""
    if opts.mr_limit:
        limit_sql = f" LIMIT {int(opts.mr_limit)}"
    rows = conn.execute(
        f"""
        SELECT m.id, m.project_id, m.iid, m.title, m.description, m.web_url, m.updated_at,
               c.final_type, c.complexity_score, c.capability_tags_json,
               (SELECT GROUP_CONCAT(path, ' ') FROM mr_files mf WHERE mf.mr_id = m.id) as paths_text,
               f.files_changed, f.churn, f.unresolved_thread_count, f.pipeline_failed_count
        FROM merge_requests m
        JOIN mr_classifications c ON c.mr_id = m.id
        LEFT JOIN mr_features f ON f.mr_id = m.id
        LEFT JOIN mr_memory_runtime r ON r.mr_id = m.id
        WHERE {where_sql}
        ORDER BY m.updated_at DESC, m.id ASC
        {limit_sql}
        """,
        tuple(params),
    ).fetchall()
    return [dict(r) for r in rows]


def build_runtime_for_project(db: Database, project_id: int, opts: MRBuildOptions) -> dict[str, Any]:
    started = _now_iso()
    eligible = 0
    success = 0
    failed = 0
    skipped = 0
    errors: list[str] = []

    with db.connect() as conn:
        rows = _eligible_runtime_rows(conn, project_id, opts)
        eligible = len(rows)
        baseline, _ = _load_project_baseline(conn, project_id)
        p50 = float(baseline.get("risk_percentiles", {}).get("p50", 0.5))
        p75 = float(baseline.get("risk_percentiles", {}).get("p75", 0.75))

        for row in rows:
            try:
                probability = _compute_regression_probability(row)
                depth, depth_score = _review_depth_required(probability, float(row.get("complexity_score") or 0.0), row)
                outcome = _risk_band(probability, p50, p75)

                reasons: list[str] = []
                if float(row.get("complexity_score") or 0.0) >= 7.0:
                    reasons.append("high complexity compared to project baseline")
                if int(row.get("pipeline_failed_count") or 0) > 0:
                    reasons.append("pipeline failures observed")
                if int(row.get("unresolved_thread_count") or 0) > 0:
                    reasons.append("unresolved review discussion threads present")
                if row.get("final_type") in {"infra", "perf-security"}:
                    reasons.append("change type historically has elevated operational risk")

                capability_tags = _parse_json_list(row.get("capability_tags_json"))
                topic_labels = _extract_topic_labels(
                    str(row.get("final_type") or ""),
                    capability_tags,
                    str(row.get("title") or ""),
                    row.get("description"),
                )
                row_for_similarity = dict(row)
                similar = _similar_rows(
                    conn,
                    seed_row=row_for_similarity,
                    project_id=project_id,
                    mr_id=int(row["id"]),
                    final_type=str(row.get("final_type") or ""),
                    complexity_score=float(row.get("complexity_score") or 0.0),
                    limit=max(1, opts.include_similar_limit),
                    data_source=opts.data_source,
                )

                row_for_outcome = dict(row)
                row_for_outcome["regression_probability"] = probability
                row_for_outcome["review_depth_required"] = depth
                outcome_mode = opts.outcome_mode if opts.outcome_mode in {"template", "semantic-local"} else "template"
                achieved_outcome, achieved_bullets, outcome_quality = _build_achieved_outcome(
                    row_for_outcome,
                    topic_labels,
                    mode=outcome_mode,
                )

                assessment = {
                    "mr_outcome": outcome,
                    "mr_achieved_outcome": achieved_outcome,
                    "mr_achieved_outcome_bullets": achieved_bullets,
                    "outcome_quality_score": outcome_quality,
                    "topic_labels": topic_labels,
                    "outcome_mode": outcome_mode,
                    "regression_probability": probability,
                    "review_depth_required": depth,
                    "depth_score": round(depth_score, 4),
                    "reasons": reasons,
                    "baseline": {"p50": p50, "p75": p75},
                }

                mr_dir = _ensure_project_dirs(opts.output_root, project_id, int(row["iid"]))
                addendum_path = mr_dir / "addendum.md"
                addendum_md = _render_addendum(row, assessment, similar)
                if not opts.db_only:
                    addendum_path.write_text(addendum_md, encoding="utf-8")

                context_path: Path | None = None
                context_raw_path: str | None = None
                if opts.compose:
                    project_dir = _ensure_project_dirs(opts.output_root, project_id)
                    baseline_path = project_dir / f"project_memory_{project_id}.md"
                    composed = ""
                    if baseline_path.exists():
                        composed = baseline_path.read_text(encoding="utf-8")
                    else:
                        composed = "# Project Cognitive Memory\n\nbaseline_missing=true\n"
                    composed += "\n---\n\n## MR Runtime Addendum\n\n" + addendum_md
                    context_path = mr_dir / "context.md"
                    if not opts.db_only:
                        context_path.write_text(composed, encoding="utf-8")
                    context_raw_path = str(context_path)

                now = _now_iso()
                db.upsert_mr_memory_runtime(
                    conn,
                    {
                        "mr_id": int(row["id"]),
                        "project_id": int(row["project_id"]),
                        "mr_iid": int(row["iid"]),
                        "mr_outcome": outcome,
                        "mr_achieved_outcome": achieved_outcome,
                        "mr_achieved_outcome_bullets": achieved_bullets,
                        "outcome_source": "heuristic",
                        "outcome_mode": outcome_mode,
                        "outcome_quality_score": outcome_quality,
                        "topic_labels": topic_labels,
                        "similarity_strategy": "lexical",
                        "regression_probability": probability,
                        "review_depth_required": depth,
                        "assessment_json": assessment,
                        "similar_mrs_json": similar,
                        "addendum_markdown_path": str(addendum_path),
                        "context_markdown_path": context_raw_path,
                        "memory_score_version": MEMORY_SCORE_VERSION,
                        "content_sha256": _sha(addendum_md),
                        "generated_at": now,
                        "updated_at": now,
                    },
                )
                success += 1
            except Exception as exc:  # pragma: no cover
                failed += 1
                errors.append(str(exc))

        finished = _now_iso()
        db.insert_memory_run(
            conn,
            {
                "run_type": "mr-runtime",
                "scope_json": {"project_id": project_id},
                "mode": "incremental" if opts.only_missing and not opts.force else "force",
                "eligible_count": eligible,
                "success_count": success,
                "failed_count": failed,
                "skipped_count": skipped,
                "started_at": started,
                "finished_at": finished,
                "status": "success" if failed == 0 else "partial",
                "error_excerpt": "; ".join(errors[:3]) if errors else None,
            },
        )

    # Recompute baseline after runtime scores are updated.
    build_project_baseline(
        db,
        project_id,
        BaselineBuildOptions(
            output_root=opts.output_root,
            data_source=opts.data_source,
            history_window_months=12,
            db_only=opts.db_only,
        ),
    )

    return {
        "project_id": project_id,
        "eligible": eligible,
        "success": success,
        "failed": failed,
        "skipped": skipped,
    }


def get_memory_status(db: Database, project_ids: list[int], data_source: str = "production") -> list[dict[str, Any]]:
    if not project_ids:
        return []
    placeholders = ",".join(["?"] * len(project_ids))
    clauses = [f"m.project_id IN ({placeholders})"]
    params: list[Any] = list(project_ids)
    if data_source != "all":
        clauses.append("m.data_source = ?")
        params.append(data_source)
    where_sql = " AND ".join(clauses)

    with db.connect() as conn:
        rows = conn.execute(
            f"""
            SELECT
              m.project_id,
              COUNT(*) as eligible,
              SUM(CASE WHEN r.mr_id IS NOT NULL THEN 1 ELSE 0 END) as scored,
              MAX(r.updated_at) as memory_updated_at,
              b.markdown_path as baseline_markdown_path,
              b.updated_at as baseline_updated_at,
              b.sample_size as baseline_sample_size
            FROM merge_requests m
            JOIN mr_classifications c ON c.mr_id = m.id
            LEFT JOIN mr_memory_runtime r ON r.mr_id = m.id
            LEFT JOIN project_memory_baseline b ON b.project_id = m.project_id
            WHERE {where_sql}
            GROUP BY m.project_id
            ORDER BY m.project_id ASC
            """,
            tuple(params),
        ).fetchall()
    return [dict(r) for r in rows]



def _default_baseline_path(output_root: str, project_id: int) -> Path:
    return Path(output_root) / "projects" / str(project_id) / f"project_memory_{project_id}.md"


def _default_addendum_path(output_root: str, project_id: int, mr_iid: int) -> Path:
    return Path(output_root) / "projects" / str(project_id) / "mrs" / str(mr_iid) / "addendum.md"


def _default_context_path(output_root: str, project_id: int, mr_iid: int) -> Path:
    return Path(output_root) / "projects" / str(project_id) / "mrs" / str(mr_iid) / "context.md"


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def materialize_project_markdown_from_db(db: Database, project_id: int, opts: MaterializeOptions) -> dict[str, Any]:
    started = _now_iso()
    baseline_written = 0
    runtime_eligible = 0
    runtime_written = 0
    runtime_skipped = 0

    with db.connect() as conn:
        baseline_row = conn.execute(
            """
            SELECT baseline_json, markdown_path, group_path, updated_at
            FROM project_memory_baseline
            WHERE project_id = ?
            """,
            (project_id,),
        ).fetchone()

        baseline_text = "# Project Cognitive Memory\n\nbaseline_missing=true\n"
        baseline_path = _default_baseline_path(opts.output_root, project_id)

        if baseline_row:
            try:
                baseline_json = json.loads(baseline_row["baseline_json"] or "{}")
            except Exception:
                baseline_json = {}
            baseline_text = _render_project_memory_markdown(
                project_id,
                baseline_row["group_path"],
                baseline_json,
                str(baseline_row["updated_at"] or _now_iso()),
            )
            baseline_path = Path(baseline_row["markdown_path"] or str(baseline_path))
            if not baseline_path.is_absolute():
                baseline_path = Path.cwd() / baseline_path

            should_write_baseline = opts.force or (not opts.only_missing) or (not baseline_path.exists())
            if should_write_baseline:
                _ensure_parent(baseline_path)
                baseline_path.write_text(baseline_text, encoding="utf-8")
                baseline_written = 1

        clauses = ["m.project_id = ?"]
        params: list[Any] = [project_id]
        if opts.data_source != "all":
            clauses.append("m.data_source = ?")
            params.append(opts.data_source)
        where_sql = " AND ".join(clauses)
        limit_sql = f" LIMIT {int(opts.mr_limit)}" if opts.mr_limit else ""

        rows = conn.execute(
            f"""
            SELECT
              r.mr_id as id,
              r.project_id,
              r.mr_iid as iid,
              m.title,
              m.web_url,
              c.final_type,
              c.complexity_score,
              r.assessment_json,
              r.similar_mrs_json,
              r.mr_achieved_outcome,
              r.mr_achieved_outcome_bullets_json,
              r.topic_labels_json,
              r.addendum_markdown_path,
              r.context_markdown_path
            FROM mr_memory_runtime r
            JOIN merge_requests m ON m.id = r.mr_id
            LEFT JOIN mr_classifications c ON c.mr_id = r.mr_id
            WHERE {where_sql}
            ORDER BY r.updated_at DESC, r.mr_iid ASC
            {limit_sql}
            """,
            tuple(params),
        ).fetchall()

        runtime_eligible = len(rows)

        for row in rows:
            item = dict(row)
            try:
                assessment = json.loads(item.get("assessment_json") or "{}")
            except Exception:
                assessment = {}
            if not assessment.get("mr_achieved_outcome"):
                assessment["mr_achieved_outcome"] = item.get("mr_achieved_outcome")
            if not assessment.get("mr_achieved_outcome_bullets"):
                try:
                    assessment["mr_achieved_outcome_bullets"] = json.loads(item.get("mr_achieved_outcome_bullets_json") or "[]")
                except Exception:
                    assessment["mr_achieved_outcome_bullets"] = []
            if not assessment.get("topic_labels"):
                try:
                    assessment["topic_labels"] = json.loads(item.get("topic_labels_json") or "[]")
                except Exception:
                    assessment["topic_labels"] = []
            try:
                similar = json.loads(item.get("similar_mrs_json") or "[]")
            except Exception:
                similar = []

            addendum_path = Path(item.get("addendum_markdown_path") or str(_default_addendum_path(opts.output_root, project_id, int(item["iid"]))))
            if not addendum_path.is_absolute():
                addendum_path = Path.cwd() / addendum_path
            context_path = Path(item.get("context_markdown_path") or str(_default_context_path(opts.output_root, project_id, int(item["iid"]))))
            if not context_path.is_absolute():
                context_path = Path.cwd() / context_path

            should_write_addendum = opts.force or (not opts.only_missing) or (not addendum_path.exists())
            should_write_context = opts.compose and (opts.force or (not opts.only_missing) or (not context_path.exists()))

            if not should_write_addendum and not should_write_context:
                runtime_skipped += 1
                continue

            addendum_md = _render_addendum(item, assessment, similar)
            if should_write_addendum:
                _ensure_parent(addendum_path)
                addendum_path.write_text(addendum_md, encoding="utf-8")

            if should_write_context:
                composed = baseline_text + "\n---\n\n## MR Runtime Addendum\n\n" + addendum_md
                _ensure_parent(context_path)
                context_path.write_text(composed, encoding="utf-8")

            runtime_written += 1

        finished = _now_iso()
        db.insert_memory_run(
            conn,
            {
                "run_type": "materialize",
                "scope_json": {"project_id": project_id},
                "mode": "only-missing" if opts.only_missing and not opts.force else "force",
                "eligible_count": runtime_eligible,
                "success_count": runtime_written + baseline_written,
                "failed_count": 0,
                "skipped_count": runtime_skipped,
                "started_at": started,
                "finished_at": finished,
                "status": "success",
                "error_excerpt": None,
            },
        )

    return {
        "project_id": project_id,
        "baseline_written": baseline_written,
        "runtime_eligible": runtime_eligible,
        "runtime_written": runtime_written,
        "runtime_skipped": runtime_skipped,
    }
