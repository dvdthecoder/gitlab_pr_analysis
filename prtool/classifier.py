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
    final_type = "infra" if infra_override_applied else base_type

    c_score, c_level = complexity_score(features)

    rationale = {
        "base_reason": base_reason,
        "infra_signal_score": infra_signal_score,
        "infra_signal_level": features["infra_signal_level"],
        "matched_infra_tickets": features.get("matched_infra_tickets", []),
        "matched_infra_keywords": features.get("matched_infra_keywords", []),
        "matched_infra_labels": features.get("matched_infra_labels", []),
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
        "rationale": rationale,
        "classified_at": datetime.now(timezone.utc).isoformat(),
    }
