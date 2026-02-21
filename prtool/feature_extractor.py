from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from prtool.config import PartialSettings


@dataclass(frozen=True)
class InfraSignals:
    ticket_match_count: int
    keyword_score: float
    label_match_count: int
    signal_score: float
    signal_level: str
    matched_keywords: list[str]
    matched_tickets: list[str]
    matched_labels: list[str]


class FeatureExtractor:
    def __init__(self, settings: PartialSettings) -> None:
        self.settings = settings
        self.ticket_patterns = [re.compile(p, flags=re.IGNORECASE) for p in settings.infra_ticket_regex]

    def _extract_infra_signals(self, title: str, description: str, labels: list[str]) -> InfraSignals:
        text = f"{title}\n{description}".lower()

        matched_tickets: list[str] = []
        for pattern in self.ticket_patterns:
            matched_tickets.extend(pattern.findall(f"{title}\n{description}"))

        keyword_hits = [kw for kw in self.settings.infra_keyword_list if kw in text]
        label_hits = [l for l in labels if l.lower() in self.settings.infra_label_allowlist]

        ticket_score = len(matched_tickets) * 2.5
        label_score = len(label_hits) * 2.0
        keyword_score = min(float(len(keyword_hits)) * 1.5, 3.0)
        signal_score = ticket_score + label_score + keyword_score

        if signal_score >= self.settings.infra_strong_threshold:
            signal_level = "strong"
        elif signal_score >= self.settings.infra_weak_threshold:
            signal_level = "weak"
        else:
            signal_level = "none"

        return InfraSignals(
            ticket_match_count=len(matched_tickets),
            keyword_score=keyword_score,
            label_match_count=len(label_hits),
            signal_score=signal_score,
            signal_level=signal_level,
            matched_keywords=sorted(set(keyword_hits)),
            matched_tickets=matched_tickets,
            matched_labels=sorted(set(label_hits)),
        )

    @staticmethod
    def _path_stats(files: list[dict[str, Any]]) -> dict[str, Any]:
        paths = [str(f.get("new_path") or f.get("old_path") or "").strip().lower() for f in files]
        total = max(1, len(paths))

        dep_files = {
            "package-lock.json",
            "pnpm-lock.yaml",
            "yarn.lock",
            "poetry.lock",
            "requirements.txt",
            "requirements-dev.txt",
            "pom.xml",
            "build.gradle",
            "build.gradle.kts",
            "gemfile.lock",
            "composer.lock",
            "cargo.lock",
            "go.mod",
            "go.sum",
        }

        def basename(p: str) -> str:
            return p.split("/")[-1] if p else ""

        docs = [p for p in paths if p.endswith(".md") or p.startswith("docs/") or "/docs/" in p]
        tests = [
            p
            for p in paths
            if "test" in p
            or p.endswith("_test.py")
            or p.endswith(".spec.ts")
            or p.endswith(".spec.js")
            or p.endswith(".test.ts")
            or p.endswith(".test.js")
        ]
        deps = [p for p in paths if basename(p) in dep_files]
        infra = [
            p
            for p in paths
            if p in {".gitlab-ci.yml", ".gitlab-ci.yaml", "dockerfile", "serverless.yml"}
            or p.startswith("infra/")
            or p.startswith("infrastructure/")
            or p.startswith("terraform/")
            or p.startswith("helm/")
            or p.startswith("k8s/")
            or p.endswith(".tf")
            or p.endswith(".tfvars")
            or p.startswith(".github/workflows/")
        ]
        config = [
            p
            for p in paths
            if p.endswith(".yml")
            or p.endswith(".yaml")
            or p.endswith(".json")
            or p.endswith(".toml")
            or p.endswith(".ini")
            or p.endswith(".conf")
            or p.endswith(".properties")
            or p.endswith(".env")
        ]

        code = [p for p in paths if p and p not in docs and p not in tests and p not in deps]
        dep_only = bool(paths) and len(deps) == len(paths)

        return {
            "docs_file_count": len(docs),
            "test_file_count": len(tests),
            "dep_file_count": len(deps),
            "infra_file_count": len(infra),
            "config_file_count": len(config),
            "code_file_count": len(code),
            "docs_file_ratio": round(len(docs) / total, 4),
            "test_file_ratio": round(len(tests) / total, 4),
            "dep_file_ratio": round(len(deps) / total, 4),
            "infra_file_ratio": round(len(infra) / total, 4),
            "code_file_ratio": round(len(code) / total, 4),
            "dep_only_change": dep_only,
        }

    def extract(
        self,
        mr: dict[str, Any],
        commits: list[dict[str, Any]],
        files: list[dict[str, Any]],
        discussions: dict[str, int],
        pipelines: dict[str, int],
    ) -> dict[str, Any]:
        title = mr.get("title", "")
        description = mr.get("description") or ""
        labels = [str(l) for l in mr.get("labels", [])]
        commit_message_text = " ".join(str(c.get("title") or c.get("message") or "") for c in commits).lower()

        additions = sum(int(f.get("additions", 0)) for f in files)
        deletions = sum(int(f.get("deletions", 0)) for f in files)
        infra = self._extract_infra_signals(title, description, labels)
        path_stats = self._path_stats(files)

        return {
            "files_changed": len(files),
            "additions": additions,
            "deletions": deletions,
            "churn": additions + deletions,
            "commit_count": len(commits),
            "review_comment_count": discussions["note_count"],
            "review_thread_count": discussions["thread_count"],
            "unresolved_thread_count": discussions["unresolved_count"],
            "pipeline_failed_count": pipelines["failed_count"],
            "infra_ticket_match_count": infra.ticket_match_count,
            "infra_keyword_score": infra.keyword_score,
            "infra_label_match_count": infra.label_match_count,
            "infra_signal_score": infra.signal_score,
            "infra_signal_level": infra.signal_level,
            "matched_infra_keywords": infra.matched_keywords,
            "matched_infra_tickets": infra.matched_tickets,
            "matched_infra_labels": infra.matched_labels,
            "label_count": len(labels),
            "has_description": bool(description.strip()),
            "commit_message_text": commit_message_text,
            **path_stats,
        }
