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


CLASSIFIER_VERSION = "v2.2"


def infer_base_type(mr: dict[str, Any], files: list[dict[str, Any]]) -> tuple[str, dict[str, Any]]:
    title = (mr.get("title") or "").lower()
    desc = (mr.get("description") or "").lower()
    labels = [str(l).lower() for l in mr.get("labels", [])]
    paths = [(f.get("new_path") or f.get("old_path") or "").lower() for f in files]

    text = f"{title} {desc} {' '.join(labels)}"

    if paths and all(p.endswith(".md") or "docs/" in p for p in paths):
        return "docs-only", {"reason": "all_changed_files_are_docs"}
    if paths and all("test" in p or p.endswith("_test.py") or p.endswith(".spec.ts") for p in paths):
        return "test-only", {"reason": "all_changed_files_are_tests"}
    if any(k in text for k in ["security", "vulnerability", "cve", "perf", "performance"]):
        return "perf-security", {"reason": "security_or_perf_keyword"}
    if any(k in text for k in ["refactor", "cleanup", "restructure"]):
        return "refactor", {"reason": "refactor_keyword"}
    if any(k in text for k in ["fix", "bug", "issue", "regression", "hotfix"]):
        return "bugfix", {"reason": "bugfix_keyword"}
    if any(k in text for k in ["chore", "deps", "dependency", "bump", "build", "ci"]):
        return "chore", {"reason": "chore_keyword"}
    return "feature", {"reason": "default_feature"}


def _joined_text_and_paths(mr: dict[str, Any], files: list[dict[str, Any]]) -> tuple[str, list[str]]:
    title = (mr.get("title") or "").lower()
    desc = (mr.get("description") or "").lower()
    labels = [str(l).lower() for l in mr.get("labels", [])]
    paths = [(f.get("new_path") or f.get("old_path") or "").lower() for f in files]
    text = f"{title}\n{desc}\n{' '.join(labels)}\n{' '.join(paths)}"
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


def detect_infra_intent_override(mr: dict[str, Any], files: list[dict[str, Any]]) -> tuple[bool, list[str]]:
    text, paths = _joined_text_and_paths(mr, files)
    title = (mr.get("title") or "").lower()
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
    text, paths = _joined_text_and_paths(mr, files)
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
    text, _ = _joined_text_and_paths(mr, files)
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


def compute_confidence(
    base_type: str,
    mr: dict[str, Any],
    files: list[dict[str, Any]],
    capability_tags: list[str],
    infra_intent_override: bool = False,
) -> float:
    text, paths = _joined_text_and_paths(mr, files)
    score = 0.55
    if base_type in {"docs-only", "test-only"}:
        score += 0.25
    if base_type in {"bugfix", "refactor", "chore", "perf-security", "infra"}:
        score += 0.15
    if capability_tags:
        score += min(0.2, 0.03 * len(capability_tags))
    if base_type == "feature" and not capability_tags and len(paths) <= 1 and len(text) < 80:
        score -= 0.1

    if base_type == "feature" and any(t in text for t in _infraish_terms()) and not any(
        t.startswith("infra.") for t in capability_tags
    ):
        score -= 0.08
    if infra_intent_override:
        score += 0.08

    return max(0.3, min(0.95, round(score, 3)))


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
    base_type, base_reason = infer_base_type(mr, files)

    infra_signal_score = float(features["infra_signal_score"])
    is_infra_related = infra_signal_score >= config.infra_weak_threshold
    infra_override_applied = infra_signal_score >= config.infra_strong_threshold

    infra_intent_override, infra_intent_evidence = detect_infra_intent_override(mr, files)

    final_type = "infra" if (infra_override_applied or infra_intent_override) else base_type
    infra_override_applied = infra_override_applied or infra_intent_override
    is_infra_related = is_infra_related or infra_intent_override

    capability_tags, capability_evidence = detect_capability_tags(mr, files, features, final_type)
    risk_tags = detect_risk_tags(mr, files, features, capability_tags, final_type)
    confidence = compute_confidence(base_type, mr, files, capability_tags, infra_intent_override=infra_intent_override)

    c_score, c_level = complexity_score(features)

    rationale = {
        "base_reason": base_reason,
        "infra_signal_score": infra_signal_score,
        "infra_signal_level": features["infra_signal_level"],
        "matched_infra_tickets": features.get("matched_infra_tickets", []),
        "matched_infra_keywords": features.get("matched_infra_keywords", []),
        "matched_infra_labels": features.get("matched_infra_labels", []),
        "infra_intent_override": infra_intent_override,
        "infra_intent_evidence": infra_intent_evidence,
        "capability_tag_evidence": capability_evidence,
        "risk_tags": risk_tags,
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
        "classifier_version": CLASSIFIER_VERSION,
        "rationale": rationale,
        "classified_at": datetime.now(timezone.utc).isoformat(),
    }
