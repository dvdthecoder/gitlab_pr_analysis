from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


PR_TYPES = [
    "feature",
    "bugfix",
    "refactor",
    "test-only",
    "docs-only",
    "chore",
    "perf-security",
    "infra",
]


@dataclass(frozen=True)
class ClassificationConfig:
    infra_strong_threshold: float
    infra_weak_threshold: float
    needs_review_threshold: float = 0.75


CLASSIFIER_VERSION = "v2.8"


def _safe_text(v: Any) -> str:
    return str(v or "").lower()


def _collect_paths(files: list[dict[str, Any]]) -> list[str]:
    return [(_safe_text(f.get("new_path") or f.get("old_path") or "")).strip() for f in files]


def _joined_text_and_paths(mr: dict[str, Any], files: list[dict[str, Any]], features: dict[str, Any] | None = None) -> tuple[str, list[str]]:
    title = _safe_text(mr.get("title"))
    desc = _safe_text(mr.get("description"))
    labels = [str(l).lower() for l in mr.get("labels", [])]
    source_branch = _safe_text(mr.get("source_branch"))
    target_branch = _safe_text(mr.get("target_branch"))
    paths = _collect_paths(files)
    commit_text = _safe_text((features or {}).get("commit_message_text"))
    text = f"{title}\n{desc}\n{' '.join(labels)}\n{source_branch} {target_branch}\n{commit_text}\n{' '.join(paths)}"
    return text, paths


def _has_any(text: str, needles: list[str]) -> bool:
    return any(n in text for n in needles)


def _infraish_terms() -> list[str]:
    return [
        "deploy",
        "deployment",
        "redeploy",
        "release",
        "rollout",
        "lambda",
        "serverless",
    ]


def _score_hits(text: str, terms: list[str], weight: float) -> tuple[float, list[str]]:
    hits = [term for term in terms if term in text]
    return len(hits) * weight, hits


def infer_base_type(mr: dict[str, Any], files: list[dict[str, Any]], features: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    text, paths = _joined_text_and_paths(mr, files, features)
    path_count = max(1, len(paths))
    docs_paths = [p for p in paths if p.endswith(".md") or p.startswith("docs/")]
    test_paths = [
        p
        for p in paths
        if "test" in p
        or p.endswith("_test.py")
        or p.endswith(".spec.ts")
        or p.endswith(".test.ts")
        or p.endswith(".test.js")
    ]

    dep_only = bool(features.get("dep_only_change", False))
    docs_ratio = float(features.get("docs_file_ratio", len(docs_paths) / path_count))
    test_ratio = float(features.get("test_file_ratio", len(test_paths) / path_count))
    code_ratio = float(features.get("code_file_ratio", 0.0) or 0.0)

    if dep_only:
        return "chore", {
            "reason": "all_changed_files_are_dependency_manifests",
            "scoreboard": {"chore": 10.0},
            "evidence": {"dep_only_change": ["true"]},
            "certainty": "high",
            "top_margin": 10.0,
            "docs_ratio": round(docs_ratio, 3),
            "test_ratio": round(test_ratio, 3),
        }

    if paths and len(docs_paths) == len(paths):
        return "docs-only", {
            "reason": "all_changed_files_are_docs",
            "scoreboard": {"docs-only": 10.0},
            "evidence": {"docs_paths": docs_paths},
            "certainty": "high",
            "top_margin": 10.0,
            "docs_ratio": round(docs_ratio, 3),
            "test_ratio": round(test_ratio, 3),
        }

    if paths and len(test_paths) == len(paths):
        return "test-only", {
            "reason": "all_changed_files_are_tests",
            "scoreboard": {"test-only": 10.0},
            "evidence": {"test_paths": test_paths},
            "certainty": "high",
            "top_margin": 10.0,
            "docs_ratio": round(docs_ratio, 3),
            "test_ratio": round(test_ratio, 3),
        }

    # High-certainty templates for common non-ambiguous class patterns.
    if _has_any(text, ["bugfix", "hotfix", "regression", "fix ", "fix:"]) and not _has_any(text, ["new feature", "introduce", "implement new"]):
        if code_ratio >= 0.25:
            return "bugfix", {
                "reason": "template_strong_bugfix",
                "scoreboard": {"bugfix": 9.0},
                "evidence": {"bugfix_template": ["keyword+code_ratio"]},
                "certainty": "high",
                "top_margin": 9.0,
                "docs_ratio": round(docs_ratio, 3),
                "test_ratio": round(test_ratio, 3),
            }

    if _has_any(text, ["refactor", "cleanup", "restructure", "rename", "extract"]) and not _has_any(text, ["feature", "new endpoint", "new api"]):
        return "refactor", {
            "reason": "template_strong_refactor",
            "scoreboard": {"refactor": 8.5},
            "evidence": {"refactor_template": ["keyword"]},
            "certainty": "high",
            "top_margin": 8.5,
            "docs_ratio": round(docs_ratio, 3),
            "test_ratio": round(test_ratio, 3),
        }

    if _has_any(text, ["snyk", "cve", "vulnerability", "security patch"]):
        return "perf-security", {
            "reason": "template_strong_security",
            "scoreboard": {"perf-security": 8.5},
            "evidence": {"security_template": ["keyword"]},
            "certainty": "high",
            "top_margin": 8.5,
            "docs_ratio": round(docs_ratio, 3),
            "test_ratio": round(test_ratio, 3),
        }

    scores = {
        "feature": 0.6,
        "bugfix": 0.0,
        "refactor": 0.0,
        "test-only": 0.0,
        "docs-only": 0.0,
        "chore": 0.0,
        "perf-security": 0.0,
    }
    evidence: dict[str, list[str]] = {}

    bugfix_terms = ["fix", "bug", "issue", "regression", "hotfix", "incident", "defect", "patch"]
    refactor_terms = ["refactor", "cleanup", "restructure", "simplify", "rename", "extract method"]
    test_terms = ["test", "unit test", "integration test", "e2e", "spec", "coverage"]
    docs_terms = ["docs", "documentation", "readme", "changelog", "runbook", "adr"]
    chore_terms = ["chore", "deps", "dependency", "bump", "build", "ci", "lint", "format"]
    perf_security_terms = [
        "security",
        "vulnerability",
        "cve",
        "snyk",
        "auth",
        "authorization",
        "token",
        "rbac",
        "perf",
        "performance",
        "latency",
        "throughput",
        "optimize",
    ]

    for t, terms, wt in [
        ("bugfix", bugfix_terms, 1.45),
        ("refactor", refactor_terms, 1.1),
        ("test-only", test_terms, 0.9),
        ("docs-only", docs_terms, 0.9),
        ("chore", chore_terms, 0.95),
        ("perf-security", perf_security_terms, 1.3),
    ]:
        add, hits = _score_hits(text, terms, wt)
        if add > 0:
            scores[t] += add
            evidence[t] = hits

    if docs_ratio >= 0.6:
        scores["docs-only"] += 1.4
    if test_ratio >= 0.6:
        scores["test-only"] += 1.4
    if dep_only:
        scores["chore"] += 1.8

    if _has_any(text, ["feature", "feat:", "new feature", "introduce", "new endpoint", "new api"]):
        scores["feature"] += 0.7

    title = _safe_text(mr.get("title"))
    if title.startswith("fix") or title.startswith("bugfix") or title.startswith("hotfix"):
        scores["bugfix"] += 0.8
        scores["feature"] = max(0.0, scores["feature"] - 0.2)

    sorted_scores = sorted(scores.items(), key=lambda kv: kv[1], reverse=True)
    top_type, top_score = sorted_scores[0]
    second_score = sorted_scores[1][1] if len(sorted_scores) > 1 else 0.0
    margin = round(top_score - second_score, 3)

    if top_type != "feature" and margin < 0.25 and scores["feature"] >= top_score - 0.15:
        top_type = "feature"

    certainty = "low"
    if margin >= 2.2:
        certainty = "high"
    elif margin >= 1.0:
        certainty = "medium"

    if top_type == "docs-only" and docs_ratio >= 0.9:
        certainty = "high"
    if top_type == "test-only" and test_ratio >= 0.9:
        certainty = "high"
    if top_type == "chore" and dep_only:
        certainty = "high"

    return top_type, {
        "reason": "weighted_signal_score",
        "scoreboard": {k: round(v, 3) for k, v in sorted_scores},
        "top_margin": margin,
        "certainty": certainty,
        "evidence": evidence,
        "docs_ratio": round(docs_ratio, 3),
        "test_ratio": round(test_ratio, 3),
        "dep_only_change": dep_only,
    }


def detect_infra_intent_override(mr: dict[str, Any], files: list[dict[str, Any]], features: dict[str, Any] | None = None) -> tuple[bool, list[str]]:
    text, paths = _joined_text_and_paths(mr, files, features)
    title = _safe_text(mr.get("title"))
    evidence: list[str] = []

    strong_terms = [
        "codedeploy",
        "deployment pipeline",
        "deploy pipeline",
        "gitlab-ci",
        "github actions",
        "terraform",
        "terragrunt",
        "kubernetes",
        "k8s",
        "helm",
        "dockerfile",
        "infrastructure as code",
        "serverless",
        "lambda",
    ]
    for term in strong_terms:
        if term in text:
            evidence.append(f"term:{term}")

    for term in _infraish_terms():
        if term in title:
            evidence.append(f"title:{term}")

    for p in paths:
        if p in {".gitlab-ci.yml", ".gitlab-ci.yaml", "dockerfile", "serverless.yml"}:
            evidence.append(f"path:{p}")
        if p.startswith(".github/workflows/"):
            evidence.append(f"path:{p}")
        if p.startswith("infra/") or p.startswith("infrastructure/"):
            evidence.append(f"path:{p}")
        if p.startswith("terraform/") or p.startswith("helm/") or p.startswith("k8s/"):
            evidence.append(f"path:{p}")
        if p.startswith("lambda/") or p.startswith("lambdas/"):
            evidence.append(f"path:{p}")
        if p.startswith("scripts/deploy") or p.endswith("/deploy.sh") or p == "deploy.sh":
            evidence.append(f"path:{p}")
        if p.endswith(".tf") or p.endswith(".tfvars"):
            evidence.append(f"path:{p}")

    return (len(evidence) > 0), sorted(set(evidence))


def detect_capability_tags(
    mr: dict[str, Any],
    files: list[dict[str, Any]],
    features: dict[str, Any],
    final_type: str,
) -> tuple[list[str], dict[str, list[str]]]:
    text, paths = _joined_text_and_paths(mr, files, features)
    evidence: dict[str, list[str]] = {}
    tags: set[str] = set()

    def add_tag(tag: str, hits: list[str]) -> None:
        if not hits:
            return
        tags.add(tag)
        evidence[tag] = sorted(set(hits))

    if "redis" in text:
        add_tag("infra.redis", [k for k in ["redis", "cache"] if k in text])

    tf_hits = [p for p in paths if p.endswith(".tf")] + [k for k in ["terraform", "terragrunt"] if k in text]
    add_tag("infra.terraform", tf_hits)

    k8s_hits = [k for k in ["k8s", "kubernetes", "helm", "cluster"] if k in text]
    add_tag("infra.k8s", k8s_hits)

    cicd_hits = [k for k in ["ci/cd", "pipeline", "gitlab-ci", "github actions", "jenkins", "codedeploy", "deploy", "deployment", "release", "rollout", "lambda", "serverless"] if k in text]
    add_tag("infra.cicd", cicd_hits)

    obs_hits = [k for k in ["observability", "prometheus", "grafana", "datadog", "tracing", "metrics", "newrelic"] if k in text]
    add_tag("observability", obs_hits)

    deps_hits = [k for k in ["dependency", "deps", "bump", "renovate", "package-lock", "pnpm-lock", "poetry.lock"] if k in text]
    add_tag("deps.update", deps_hits)

    snyk_hits = [k for k in ["snyk", "sca", "dependency scan"] if k in text]
    add_tag("security.sca", snyk_hits)

    auth_hits = [k for k in ["auth", "oauth", "jwt", "token", "rbac", "authorization"] if k in text]
    add_tag("security.auth", auth_hits)

    migration_hits = [k for k in ["migration", "schema", "alembic", "flyway", "liquibase"] if k in text]
    add_tag("data.migration", migration_hits)

    api_hits = [k for k in ["openapi", "swagger", "api contract", "graphql schema"] if k in text]
    add_tag("api.contract", api_hits)

    perf_hits = [k for k in ["latency", "throughput", "performance", "perf"] if k in text]
    add_tag("performance", perf_hits)

    if float(features.get("infra_signal_score", 0.0)) >= 0.1 or final_type == "infra":
        tags.add("infra.general")
        evidence.setdefault("infra.general", []).append(f"infra_signal={features.get('infra_signal_score', 0.0)}")

    return sorted(tags), evidence


def detect_risk_tags(
    mr: dict[str, Any],
    files: list[dict[str, Any]],
    features: dict[str, Any],
    capability_tags: list[str],
    final_type: str,
) -> list[str]:
    text, _ = _joined_text_and_paths(mr, files, features)
    risks: set[str] = set()

    if any(t.startswith("security.") for t in capability_tags):
        risks.add("risk.security")
    if "data.migration" in capability_tags:
        risks.add("risk.migration")
    if _has_any(text, ["breaking", "breaking change", "backward incompatible"]):
        risks.add("risk.breaking-change")
    if final_type == "infra" or any(t.startswith("infra.") for t in capability_tags):
        risks.add("risk.infra")
    if int(features.get("churn", 0) or 0) > 1500:
        risks.add("risk.large-change")

    return sorted(risks)


def confidence_band(score: float, needs_review_threshold: float = 0.75) -> str:
    medium_floor = max(0.45, min(0.8, needs_review_threshold))
    if score >= 0.8:
        return "high"
    if score >= medium_floor:
        return "medium"
    return "low"


def _conflict_penalty(base_type: str, scoreboard: dict[str, float], margin: float) -> tuple[float, list[str]]:
    if len(scoreboard) < 2:
        return 0.0, []
    items = list(scoreboard.items())
    top_label, _ = items[0]
    second_label, _ = items[1]
    pair = tuple(sorted([top_label, second_label]))

    penalties = {
        ("bugfix", "feature"): 0.07,
        ("feature", "infra"): 0.12,
        ("chore", "perf-security"): 0.10,
        ("feature", "refactor"): 0.08,
    }
    if margin >= 1.0:
        return 0.0, []

    if pair == ("bugfix", "feature") and margin >= 0.75:
        return 0.0, []

    penalty = penalties.get(pair, 0.0)
    if penalty <= 0:
        return 0.0, []
    return penalty, [f"{top_label}|{second_label}"]


def _label_support_stats(mr: dict[str, Any], final_type: str) -> tuple[int, int]:
    labels = [str(l).strip().lower() for l in mr.get("labels", []) if str(l).strip()]
    if not labels:
        return 0, 0

    support_map: dict[str, set[str]] = {
        "feature": {"feature", "enhancement"},
        "bugfix": {"bug", "bugfix", "fix", "defect", "hotfix"},
        "refactor": {"refactor", "cleanup"},
        "test-only": {"test", "tests"},
        "docs-only": {"docs", "documentation"},
        "chore": {"chore", "maintenance", "dependencies", "deps"},
        "perf-security": {"security", "perf", "performance", "snyk", "vulnerability"},
        "infra": {"infra", "platform", "devops", "sre"},
    }

    support_aliases = support_map.get(final_type, set())
    support = sum(1 for lbl in labels if lbl in support_aliases)

    conflict = 0
    for lbl in labels:
        for k, aliases in support_map.items():
            if k != final_type and lbl in aliases:
                conflict += 1
                break
    return support, conflict


def compute_confidence(
    base_type: str,
    final_type: str,
    needs_review_threshold: float,
    mr: dict[str, Any],
    files: list[dict[str, Any]],
    features: dict[str, Any],
    base_reason: dict[str, Any],
    capability_tags: list[str],
    infra_intent_override_applied: bool = False,
) -> tuple[float, dict[str, Any]]:
    text, paths = _joined_text_and_paths(mr, files, features)
    score = 0.52

    has_description = bool(features.get("has_description", False))
    label_count = int(features.get("label_count", 0) or 0)
    docs_ratio = float(features.get("docs_file_ratio", 0.0) or 0.0)
    test_ratio = float(features.get("test_file_ratio", 0.0) or 0.0)
    dep_only = bool(features.get("dep_only_change", False))
    code_ratio = float(features.get("code_file_ratio", 0.0) or 0.0)

    richness = 0
    if has_description:
        richness += 1
    if label_count > 0:
        richness += 1
    if int(features.get("commit_count", 0)) > 0:
        richness += 1
    if len(paths) > 1:
        richness += 1
    score += 0.05 * richness

    if base_type in {"docs-only", "test-only"}:
        score += 0.16
    if final_type in {"bugfix", "refactor", "chore", "perf-security", "infra"}:
        score += 0.1

    margin = float(base_reason.get("top_margin", 0.0) or 0.0)
    if margin >= 2.0:
        score += 0.18
    elif margin >= 1.0:
        score += 0.11
    elif margin >= 0.5:
        score += 0.05
    else:
        score -= 0.08

    certainty = str(base_reason.get("certainty", "low"))
    if certainty == "high":
        score += 0.18
    elif certainty == "medium":
        score += 0.07
    else:
        score -= 0.03

    if dep_only and base_type == "chore":
        score += 0.22
    if docs_ratio >= 0.9 and base_type == "docs-only":
        score += 0.2
    if test_ratio >= 0.9 and base_type == "test-only":
        score += 0.18

    if code_ratio > 0.6 and base_type in {"docs-only", "test-only"}:
        score -= 0.2

    if capability_tags:
        score += min(0.16, 0.02 * len(capability_tags))

    evidence_classes = len(base_reason.get("evidence", {}))
    if margin < 0.6 and evidence_classes >= 3:
        score -= 0.1

    conflict_pen, conflict_pairs = _conflict_penalty(base_type, dict(base_reason.get("scoreboard", {})), margin)
    score -= conflict_pen

    label_support, label_conflict = _label_support_stats(mr, final_type)
    score += min(0.08, 0.04 * label_support)
    score -= min(0.12, 0.04 * label_conflict)

    if base_type == "feature" and not capability_tags and len(paths) <= 1 and len(text) < 120:
        score -= 0.08

    if infra_intent_override_applied:
        score += 0.06

    if not has_description and label_count == 0 and int(features.get("commit_count", 0)) == 0:
        score -= 0.08

    score = max(0.3, min(0.95, round(score, 3)))
    factors = {
        "richness": richness,
        "top_margin": margin,
        "certainty": certainty,
        "capability_tag_count": len(capability_tags),
        "dep_only_change": dep_only,
        "docs_ratio": docs_ratio,
        "test_ratio": test_ratio,
        "code_ratio": code_ratio,
        "label_support_count": label_support,
        "label_conflict_count": label_conflict,
        "conflict_pairs": conflict_pairs,
        "infra_intent_override_applied": infra_intent_override_applied,
        "confidence_band": confidence_band(score, needs_review_threshold=needs_review_threshold),
    }
    return score, factors


def complexity_score(features: dict[str, Any]) -> tuple[float, str]:
    score = 0.0
    score += min(features["churn"] / 250.0, 4.0)
    score += min(features["files_changed"] / 10.0, 2.0)
    score += min(features["commit_count"] / 8.0, 1.5)
    score += min(features["review_comment_count"] / 20.0, 1.5)
    score += min(features["review_thread_count"] / 10.0, 1.0)
    score += min(features["unresolved_thread_count"] / 5.0, 1.0)
    score += min(features["pipeline_failed_count"] / 3.0, 1.0)

    if score < 1.5:
        return score, "Very Low"
    if score < 3.0:
        return score, "Low"
    if score < 5.0:
        return score, "Medium"
    if score < 7.0:
        return score, "High"
    return score, "Very High"


def classify(
    mr: dict[str, Any],
    files: list[dict[str, Any]],
    features: dict[str, Any],
    config: ClassificationConfig,
) -> dict[str, Any]:
    base_type, base_reason = infer_base_type(mr, files, features)

    infra_signal_score = float(features["infra_signal_score"])
    is_infra_related = infra_signal_score >= config.infra_weak_threshold
    infra_override_applied = infra_signal_score >= config.infra_strong_threshold

    infra_intent_override, infra_intent_evidence = detect_infra_intent_override(mr, files, features)
    infra_path_strong = any(e.startswith("path:") for e in infra_intent_evidence)

    intent_override_applied = False
    if infra_intent_override:
        if base_type in {"bugfix", "chore"}:
            intent_override_applied = infra_path_strong
        else:
            intent_override_applied = True

    final_type = "infra" if (infra_override_applied or intent_override_applied) else base_type
    infra_override_applied = infra_override_applied or intent_override_applied
    is_infra_related = is_infra_related or infra_intent_override

    capability_tags, capability_evidence = detect_capability_tags(mr, files, features, final_type)
    risk_tags = detect_risk_tags(mr, files, features, capability_tags, final_type)
    confidence, confidence_factors = compute_confidence(
        base_type,
        final_type,
        config.needs_review_threshold,
        mr,
        files,
        features,
        base_reason,
        capability_tags,
        infra_intent_override_applied=intent_override_applied,
    )

    c_score, c_level = complexity_score(features)

    needs_review = confidence < config.needs_review_threshold
    why_needs_review: list[str] = []
    if needs_review:
        if float(base_reason.get("top_margin", 0.0) or 0.0) < 0.8:
            why_needs_review.append("low_top2_margin")
        if confidence_factors.get("conflict_pairs"):
            why_needs_review.append("conflicting_class_signals")
        if int(confidence_factors.get("label_conflict_count", 0) or 0) > 0:
            why_needs_review.append("conflicting_labels")
        if not bool(features.get("has_description", False)):
            why_needs_review.append("missing_description")
        if not why_needs_review:
            why_needs_review.append("composite_low_confidence")

    rationale = {
        "base_reason": base_reason,
        "infra_signal_score": infra_signal_score,
        "infra_signal_level": features["infra_signal_level"],
        "matched_infra_tickets": features.get("matched_infra_tickets", []),
        "matched_infra_keywords": features.get("matched_infra_keywords", []),
        "matched_infra_labels": features.get("matched_infra_labels", []),
        "infra_intent_override": infra_intent_override,
        "infra_intent_evidence": infra_intent_evidence,
        "infra_path_strong": infra_path_strong,
        "intent_override_applied": intent_override_applied,
        "capability_tag_evidence": capability_evidence,
        "risk_tags": risk_tags,
        "confidence_factors": confidence_factors,
        "needs_review_threshold": config.needs_review_threshold,
        "why_needs_review": why_needs_review,
        "complexity_components": {
            "churn": features["churn"],
            "files_changed": features["files_changed"],
            "commit_count": features["commit_count"],
            "review_comment_count": features["review_comment_count"],
            "review_thread_count": features["review_thread_count"],
            "unresolved_thread_count": features["unresolved_thread_count"],
            "pipeline_failed_count": features["pipeline_failed_count"],
        },
    }

    return {
        "base_type": base_type,
        "final_type": final_type,
        "is_infra_related": is_infra_related,
        "infra_override_applied": infra_override_applied,
        "complexity_level": c_level,
        "complexity_score": c_score,
        "capability_tags": capability_tags,
        "risk_tags": risk_tags,
        "classification_confidence": confidence,
        "confidence_band": confidence_band(confidence, needs_review_threshold=config.needs_review_threshold),
        "needs_review": needs_review,
        "classifier_version": CLASSIFIER_VERSION,
        "rationale": rationale,
        "classified_at": datetime.now(timezone.utc).isoformat(),
    }
