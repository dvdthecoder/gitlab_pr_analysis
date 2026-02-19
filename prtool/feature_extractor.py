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

        additions = sum(int(f.get("additions", 0)) for f in files)
        deletions = sum(int(f.get("deletions", 0)) for f in files)
        infra = self._extract_infra_signals(title, description, labels)

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
        }
