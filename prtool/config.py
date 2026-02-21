from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


DEFAULT_INFRA_KEYWORDS = [
    "terraform",
    "k8s",
    "kubernetes",
    "helm",
    "ci/cd",
    "docker",
    "cluster",
    "network",
    "networking",
    "deployment",
    "observability",
    "prometheus",
    "grafana",
    "sre",
    "infra",
]


@dataclass(frozen=True)
class Settings:
    gitlab_base_url: str
    gitlab_token: str
    db_path: str
    page_size: int
    max_retries: int
    backoff_ms: int
    request_timeout: int
    infra_ticket_regex: list[str]
    infra_label_allowlist: list[str]
    infra_keyword_list: list[str]
    infra_strong_threshold: float
    infra_weak_threshold: float
    classification_needs_review_threshold: float = 0.75


@dataclass(frozen=True)
class PartialSettings:
    db_path: str
    infra_ticket_regex: list[str]
    infra_label_allowlist: list[str]
    infra_keyword_list: list[str]
    infra_strong_threshold: float
    infra_weak_threshold: float
    classification_needs_review_threshold: float = 0.75


def _split_csv(value: str | None, default: list[str]) -> list[str]:
    if not value:
        return default
    return [v.strip() for v in value.split(",") if v.strip()]


def load_dotenv(path: str | None = None) -> None:
    env_path = Path(path or os.getenv("PRTOOL_ENV_FILE", ".env"))
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("\"").strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def resolve_project_ids(project_id_overrides: list[int] | None = None) -> list[int]:
    if project_id_overrides:
        return sorted(set(project_id_overrides))

    env_many = os.getenv("GITLAB_PROJECT_IDS")
    if env_many:
        values = [v.strip() for v in env_many.split(",") if v.strip()]
        if not values:
            raise ValueError("GITLAB_PROJECT_IDS is set but empty")
        return sorted(set(int(v) for v in values))

    env_one = os.getenv("GITLAB_PROJECT_ID")
    if env_one:
        return [int(env_one)]

    raise ValueError("Provide --project-id (repeatable) or set GITLAB_PROJECT_IDS / GITLAB_PROJECT_ID")


def resolve_group_ids(group_id_overrides: list[str] | None = None) -> list[str]:
    if group_id_overrides:
        return sorted(set(g.strip() for g in group_id_overrides if g.strip()))

    env_many = os.getenv("GITLAB_GROUP_IDS")
    if env_many:
        values = [v.strip() for v in env_many.split(",") if v.strip()]
        if not values:
            raise ValueError("GITLAB_GROUP_IDS is set but empty")
        return sorted(set(values))

    env_one = os.getenv("GITLAB_GROUP_ID")
    if env_one:
        return [env_one.strip()]

    return []


def load_partial_settings() -> PartialSettings:
    return PartialSettings(
        db_path=os.getenv("DB_PATH", "./pr_analysis.db"),
        infra_ticket_regex=_split_csv(os.getenv("INFRA_TICKET_REGEX"), [r"INFRA-\d+", r"OPS-\d+"]),
        infra_label_allowlist=[s.lower() for s in _split_csv(os.getenv("INFRA_LABEL_ALLOWLIST"), ["infra", "platform", "devops", "sre"])],
        infra_keyword_list=[s.lower() for s in _split_csv(os.getenv("INFRA_KEYWORD_LIST"), DEFAULT_INFRA_KEYWORDS)],
        infra_strong_threshold=float(os.getenv("INFRA_STRONG_THRESHOLD", "4.0")),
        infra_weak_threshold=float(os.getenv("INFRA_WEAK_THRESHOLD", "1.5")),
        classification_needs_review_threshold=float(os.getenv("CLASSIFICATION_NEEDS_REVIEW_THRESHOLD", "0.75")),
    )


def load_settings() -> Settings:
    partial = load_partial_settings()
    base_url = os.getenv("GITLAB_BASE_URL")
    token = os.getenv("GITLAB_TOKEN")

    if not base_url:
        raise ValueError("GITLAB_BASE_URL is required")
    if not token:
        raise ValueError("GITLAB_TOKEN is required")

    return Settings(
        gitlab_base_url=base_url.rstrip("/"),
        gitlab_token=token,
        db_path=partial.db_path,
        page_size=int(os.getenv("PAGE_SIZE", "100")),
        max_retries=int(os.getenv("MAX_RETRIES", "3")),
        backoff_ms=int(os.getenv("BACKOFF_MS", "500")),
        request_timeout=int(os.getenv("REQUEST_TIMEOUT", "30")),
        infra_ticket_regex=partial.infra_ticket_regex,
        infra_label_allowlist=partial.infra_label_allowlist,
        infra_keyword_list=partial.infra_keyword_list,
        infra_strong_threshold=partial.infra_strong_threshold,
        infra_weak_threshold=partial.infra_weak_threshold,
        classification_needs_review_threshold=partial.classification_needs_review_threshold,
    )
