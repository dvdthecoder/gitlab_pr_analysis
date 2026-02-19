from __future__ import annotations

from prtool.classifier import ClassificationConfig, classify
from prtool.config import PartialSettings
from prtool.feature_extractor import FeatureExtractor


def _settings() -> PartialSettings:
    return PartialSettings(
        db_path=":memory:",
        infra_ticket_regex=[r"INFRA-\d+", r"OPS-\d+"],
        infra_label_allowlist=["infra", "platform", "devops", "sre"],
        infra_keyword_list=["terraform", "k8s", "deployment", "infra", "docker"],
        infra_strong_threshold=4.0,
        infra_weak_threshold=1.5,
    )


def test_strong_infra_ticket_with_label_overrides_to_infra() -> None:
    extractor = FeatureExtractor(_settings())
    mr = {
        "title": "INFRA-123 Add cluster deployment automation",
        "description": "terraform and k8s updates",
        "labels": ["infra", "automation"],
    }
    files = [{"new_path": "infra/main.tf", "additions": 20, "deletions": 2}]
    features = extractor.extract(mr, commits=[], files=files, discussions={"thread_count": 0, "note_count": 0, "unresolved_count": 0}, pipelines={"failed_count": 0})

    result = classify(mr, files, features, ClassificationConfig(4.0, 1.5))
    assert result["final_type"] == "infra"
    assert result["infra_override_applied"] is True


def test_weak_infra_signal_keeps_bugfix_type_but_marks_related() -> None:
    extractor = FeatureExtractor(_settings())
    mr = {
        "title": "Fix null pointer in parser",
        "description": "touch deployment script",
        "labels": ["bug"],
    }
    files = [{"new_path": "src/parser.py", "additions": 3, "deletions": 1}]
    features = extractor.extract(mr, commits=[], files=files, discussions={"thread_count": 0, "note_count": 0, "unresolved_count": 0}, pipelines={"failed_count": 0})

    result = classify(mr, files, features, ClassificationConfig(4.0, 1.5))
    assert result["base_type"] == "bugfix"
    assert result["final_type"] == "bugfix"
    assert result["is_infra_related"] is True
    assert result["infra_override_applied"] is False


def test_docs_only_with_strong_infra_evidence_overrides() -> None:
    extractor = FeatureExtractor(_settings())
    mr = {
        "title": "OPS-45 Update deployment runbook",
        "description": "infra docker cluster deployment docs",
        "labels": ["platform"],
    }
    files = [{"new_path": "docs/runbook.md", "additions": 40, "deletions": 10}]
    features = extractor.extract(mr, commits=[], files=files, discussions={"thread_count": 0, "note_count": 0, "unresolved_count": 0}, pipelines={"failed_count": 0})

    result = classify(mr, files, features, ClassificationConfig(4.0, 1.5))
    assert result["base_type"] == "docs-only"
    assert result["final_type"] == "infra"


def test_no_infra_signal_keeps_non_infra_behavior() -> None:
    extractor = FeatureExtractor(_settings())
    mr = {
        "title": "Add payment webhook feature",
        "description": "new endpoint and handlers",
        "labels": ["feature"],
    }
    files = [{"new_path": "src/payments/webhook.py", "additions": 120, "deletions": 5}]
    features = extractor.extract(mr, commits=[], files=files, discussions={"thread_count": 1, "note_count": 2, "unresolved_count": 0}, pipelines={"failed_count": 0})

    result = classify(mr, files, features, ClassificationConfig(4.0, 1.5))
    assert result["final_type"] == "feature"
    assert result["is_infra_related"] is False


def test_threshold_boundaries() -> None:
    cfg = ClassificationConfig(4.0, 1.5)
    mr = {"title": "Fix bug", "description": "", "labels": []}
    files = [{"new_path": "src/a.py", "additions": 1, "deletions": 0}]

    weak_features = {
        "churn": 1,
        "files_changed": 1,
        "commit_count": 1,
        "review_comment_count": 0,
        "review_thread_count": 0,
        "unresolved_thread_count": 0,
        "pipeline_failed_count": 0,
        "infra_signal_score": 1.5,
        "infra_signal_level": "weak",
        "matched_infra_tickets": [],
        "matched_infra_keywords": ["deployment"],
        "matched_infra_labels": [],
    }
    weak = classify(mr, files, weak_features, cfg)
    assert weak["is_infra_related"] is True
    assert weak["infra_override_applied"] is False

    strong_features = dict(weak_features)
    strong_features["infra_signal_score"] = 4.0
    strong_features["infra_signal_level"] = "strong"
    strong = classify(mr, files, strong_features, cfg)
    assert strong["final_type"] == "infra"
    assert strong["infra_override_applied"] is True


def test_redis_signal_is_capability_tag_without_forcing_infra_when_weak() -> None:
    extractor = FeatureExtractor(_settings())
    mr = {
        "title": "Add Redis cache layer for catalog endpoint",
        "description": "feature rollout for cache reads",
        "labels": ["feature"],
    }
    files = [{"new_path": "src/catalog/cache.ts", "additions": 40, "deletions": 3}]
    features = extractor.extract(
        mr,
        commits=[],
        files=files,
        discussions={"thread_count": 0, "note_count": 0, "unresolved_count": 0},
        pipelines={"failed_count": 0},
    )

    result = classify(mr, files, features, ClassificationConfig(4.0, 1.5))
    assert result["final_type"] == "feature"
    assert "infra.redis" in result["capability_tags"]


def test_snyk_signal_maps_to_security_tag() -> None:
    extractor = FeatureExtractor(_settings())
    mr = {
        "title": "Snyk fixes for vulnerable dependencies",
        "description": "dependency scan remediation",
        "labels": ["security", "chore"],
    }
    files = [{"new_path": "package-lock.json", "additions": 10, "deletions": 10}]
    features = extractor.extract(
        mr,
        commits=[],
        files=files,
        discussions={"thread_count": 0, "note_count": 0, "unresolved_count": 0},
        pipelines={"failed_count": 0},
    )

    result = classify(mr, files, features, ClassificationConfig(4.0, 1.5))
    assert "security.sca" in result["capability_tags"]
    assert "risk.security" in result["risk_tags"]
    assert result["classifier_version"] == "v2.2"


def test_deploy_pipeline_title_overrides_to_infra_even_without_strong_infra_score() -> None:
    extractor = FeatureExtractor(_settings())
    mr = {
        "title": "CodeDeploy setup for prod deployment pipeline",
        "description": "bootstrap deployment flow",
        "labels": ["feature"],
    }
    files = [{"new_path": "src/app.ts", "additions": 12, "deletions": 2}]
    features = extractor.extract(
        mr,
        commits=[],
        files=files,
        discussions={"thread_count": 0, "note_count": 0, "unresolved_count": 0},
        pipelines={"failed_count": 0},
    )

    result = classify(mr, files, features, ClassificationConfig(4.0, 1.5))
    assert result["final_type"] == "infra"
    assert result["infra_override_applied"] is True


def test_infra_file_path_overrides_to_infra_without_infra_ticket() -> None:
    extractor = FeatureExtractor(_settings())
    mr = {
        "title": "Build tweaks",
        "description": "minor updates",
        "labels": [],
    }
    files = [{"new_path": ".gitlab-ci.yml", "additions": 5, "deletions": 1}]
    features = extractor.extract(
        mr,
        commits=[],
        files=files,
        discussions={"thread_count": 0, "note_count": 0, "unresolved_count": 0},
        pipelines={"failed_count": 0},
    )

    result = classify(mr, files, features, ClassificationConfig(4.0, 1.5))
    assert result["final_type"] == "infra"
    assert result["infra_override_applied"] is True


def test_classifier_version_bumped_after_rule_update() -> None:
    extractor = FeatureExtractor(_settings())
    mr = {
        "title": "Add endpoint",
        "description": "normal feature",
        "labels": ["feature"],
    }
    files = [{"new_path": "src/api.ts", "additions": 10, "deletions": 0}]
    features = extractor.extract(
        mr,
        commits=[],
        files=files,
        discussions={"thread_count": 0, "note_count": 0, "unresolved_count": 0},
        pipelines={"failed_count": 0},
    )
    result = classify(mr, files, features, ClassificationConfig(4.0, 1.5))
    assert result["classifier_version"] == "v2.2"



def test_deploy_script_path_overrides_to_infra() -> None:
    extractor = FeatureExtractor(_settings())
    mr = {
        "title": "Update helper script",
        "description": "script cleanup",
        "labels": ["chore"],
    }
    files = [{"new_path": "scripts/deploy.sh", "additions": 6, "deletions": 1}]
    features = extractor.extract(
        mr,
        commits=[],
        files=files,
        discussions={"thread_count": 0, "note_count": 0, "unresolved_count": 0},
        pipelines={"failed_count": 0},
    )
    result = classify(mr, files, features, ClassificationConfig(4.0, 1.5))
    assert result["final_type"] == "infra"
    assert result["infra_override_applied"] is True


def test_lambda_title_overrides_to_infra() -> None:
    extractor = FeatureExtractor(_settings())
    mr = {
        "title": "Deploying Invoice Reminder Lambda",
        "description": "release prep",
        "labels": ["feature"],
    }
    files = [{"new_path": "src/reminders/index.ts", "additions": 20, "deletions": 2}]
    features = extractor.extract(
        mr,
        commits=[],
        files=files,
        discussions={"thread_count": 0, "note_count": 0, "unresolved_count": 0},
        pipelines={"failed_count": 0},
    )
    result = classify(mr, files, features, ClassificationConfig(4.0, 1.5))
    assert result["final_type"] == "infra"
    assert result["infra_override_applied"] is True
