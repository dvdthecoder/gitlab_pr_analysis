from __future__ import annotations

import hashlib
import math
import os
import re
import shlex
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from prtool.db import Database

PARSER_VERSION = "qodo-v2"
QODO_TOOLS = ("describe", "review", "improve")
MIN_REQUIRED_SECTIONS = ("summary", "changes")
PROMPT_LEAK_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    ("system_prompt", re.compile(r"\bsystem prompt\b", re.IGNORECASE)),
    ("user_prompt", re.compile(r"\buser prompt\b", re.IGNORECASE)),
    ("assistant_role", re.compile(r"\byou are\b", re.IGNORECASE)),
    ("role_json", re.compile(r'"role"\s*:\s*"(system|user|assistant)"', re.IGNORECASE)),
]


@dataclass(frozen=True)
class EnrichOptions:
    output_root: str
    concurrency: int = 5
    mr_limit: int | None = None
    only_missing: bool = True
    force: bool = False
    data_source: str = "production"
    timeout_sec: int = 180
    compact_max_tokens: int = 3000
    include_mermaid: bool = True
    tools: tuple[str, ...] = ("describe",)
    progress: bool = True


@dataclass(frozen=True)
class CandidateOptions:
    mode: str = "none"
    count: int = 10
    scope: str = "global"
    type_balance: str = "soft"
    data_source: str = "production"
    preview: bool = False


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sha256(content: str) -> str:
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def _infer_qodo_type(content: str) -> str | None:
    lowered = content.lower()
    if "bug" in lowered:
        return "bugfix"
    if "refactor" in lowered:
        return "refactor"
    if "doc" in lowered:
        return "docs-only"
    if "test" in lowered:
        return "test-only"
    if "perf" in lowered or "security" in lowered:
        return "perf-security"
    if "infra" in lowered or "deployment" in lowered:
        return "infra"
    if "feature" in lowered:
        return "feature"
    return None


def _detect_prompt_leaks(content: str) -> list[str]:
    markers: list[str] = []
    for name, pattern in PROMPT_LEAK_PATTERNS:
        if pattern.search(content):
            markers.append(name)
    return markers


def _sanitize_content(content: str) -> str:
    out_lines: list[str] = []
    for line in _strip_ansi(content).splitlines():
        keep = True
        for _, pattern in PROMPT_LEAK_PATTERNS:
            if pattern.search(line):
                keep = False
                break
        if keep:
            out_lines.append(line.rstrip())
    return "\n".join(out_lines).strip()


def _looks_like_diff_text(text: str) -> bool:
    t = (text or "").strip()
    if not t:
        return False
    prefixes = ("@@ ", "diff --git", "+++ ", "--- ", "+", "-")
    lines = [ln.strip() for ln in t.splitlines()[:8] if ln.strip()]
    if not lines:
        return False
    hits = 0
    for ln in lines:
        if any(ln.startswith(p) for p in prefixes):
            hits += 1
    return hits >= max(2, len(lines) // 2)


def _select_reviewer_summary(
    parsed: dict[str, Any],
    quality_status: str,
    prompt_leak_markers: list[str],
) -> tuple[str | None, str]:
    if quality_status == "failed":
        return None, "failed"
    if prompt_leak_markers:
        return None, "unsafe"

    sections = parsed.get("sections") if isinstance(parsed.get("sections"), dict) else {}
    candidates: list[str] = []
    for candidate in (
        parsed.get("summary"),
        sections.get("summary"),
        sections.get("overview"),
        sections.get("changes"),
    ):
        if isinstance(candidate, str):
            candidates.append(candidate)

    for text in candidates:
        cleaned = _sanitize_content(text)
        if not cleaned:
            continue
        if _looks_like_diff_text(cleaned):
            continue
        if len(cleaned.split()) < 8:
            continue
        return cleaned[:1000], "clean"
    return None, "missing"


def _context_quality_score(
    quality_status: str,
    reviewer_summary_status: str,
    prompt_leak_count: int,
    missing_required_count: int,
) -> float:
    score = 0.6
    if quality_status == "ok":
        score += 0.2
    elif quality_status == "failed":
        score -= 0.4
    if reviewer_summary_status == "clean":
        score += 0.2
    elif reviewer_summary_status == "unsafe":
        score -= 0.25
    score -= min(0.25, 0.08 * max(0, int(prompt_leak_count)))
    score -= min(0.2, 0.05 * max(0, int(missing_required_count)))
    return max(0.0, min(1.0, round(score, 3)))


def _collect_secret_values() -> list[str]:
    secrets: list[str] = []
    for key, value in os.environ.items():
        if not value:
            continue
        upper = key.upper()
        if any(token in upper for token in ("TOKEN", "SECRET", "PASSWORD", "API_KEY", "ACCESS_KEY", "PRIVATE_KEY")):
            if len(value.strip()) >= 6:
                secrets.append(value.strip())
    return sorted(set(secrets), key=len, reverse=True)


def _redact_secrets(text: str) -> str:
    redacted = text
    patterns = [
        re.compile(r"(glpat-)[A-Za-z0-9._-]+"),
        re.compile(r"(sk-)[A-Za-z0-9][A-Za-z0-9_-]{8,}"),
        re.compile(r"(--gitlab\.personal_access_token=)(\S+)"),
        re.compile(r"(GITLAB\.PERSONAL_ACCESS_TOKEN to:\s*\")([^\"]+)(\")"),
        re.compile(r"(OPENAI_API_KEY\s*=\s*)(\S+)"),
    ]
    for pattern in patterns:
        redacted = pattern.sub(lambda m: f"{m.group(1)}[REDACTED]{m.group(3)}" if m.lastindex == 3 else f"{m.group(1)}[REDACTED]", redacted)
    for secret in _collect_secret_values():
        redacted = redacted.replace(secret, "[REDACTED]")
    return redacted


def _parse_markdown_sections(content: str) -> dict[str, Any]:
    lines = content.splitlines()
    title = ""
    sections: dict[str, str] = {}
    current_key: str | None = None
    current_lines: list[str] = []

    def flush() -> None:
        nonlocal current_key, current_lines
        if current_key is not None:
            body = "\n".join(current_lines).strip()
            if body:
                sections[current_key] = body
        current_key = None
        current_lines = []

    for idx, line in enumerate(lines):
        stripped = line.strip()
        if idx == 0 and stripped.startswith("#"):
            title = stripped.lstrip("#").strip()
            continue
        if stripped.startswith("##"):
            flush()
            key = re.sub(r"[^a-z0-9]+", "_", stripped.lstrip("#").strip().lower()).strip("_")
            current_key = key or f"section_{len(sections) + 1}"
            continue
        current_lines.append(line)
    flush()

    if not sections:
        paragraphs = [p.strip() for p in re.split(r"\n\s*\n", content) if p.strip()]
        if paragraphs:
            sections["summary"] = paragraphs[0]
            if len(paragraphs) > 1:
                sections["changes"] = "\n\n".join(paragraphs[1:])

    labels: list[str] = []
    for key, value in sections.items():
        if "label" in key:
            labels.extend([t.strip().lower() for t in value.split(",") if t.strip()])

    summary = ""
    for candidate in ("summary", "overview", "description"):
        if candidate in sections:
            summary = sections[candidate]
            break
    if not summary and sections:
        summary = next(iter(sections.values()))

    return {
        "title": title or None,
        "summary": summary[:1200] if summary else None,
        "sections": sections,
        "labels": sorted(set(labels)),
    }


def _extract_yaml_block(raw: str) -> str | None:
    cleaned = _strip_ansi(raw)
    for block in reversed(_extract_ai_response_blocks(cleaned)):
        tail = block.strip()
        if tail.startswith("```yaml"):
            tail = tail.removeprefix("```yaml").strip()
            if tail.endswith("```"):
                tail = tail[:-3].strip()
        if tail:
            return tail

    candidates = re.findall(r"```yaml\s*(.*?)```", cleaned, flags=re.IGNORECASE | re.DOTALL)
    if candidates:
        best = max(candidates, key=len).strip()
        if best:
            return best
    return None


def _parse_yaml_payload(raw: str) -> dict[str, Any] | None:
    block = _extract_yaml_block(raw)
    if not block:
        return None
    try:
        import yaml  # type: ignore
    except Exception:
        return None
    try:
        data = yaml.safe_load(block)
    except Exception:
        return None
    if not isinstance(data, dict):
        return None

    # PR review schema
    if isinstance(data.get("review"), dict):
        review = data["review"]
        findings: list[str] = []
        key_issues = review.get("key_issues_to_review")
        if isinstance(key_issues, list):
            for issue in key_issues:
                if isinstance(issue, dict):
                    header = str(issue.get("issue_header") or "").strip()
                    body = str(issue.get("issue_content") or "").strip()
                    if header and body:
                        findings.append(f"{header}: {body}")
                    elif body:
                        findings.append(body)
        sec = str(review.get("security_concerns") or "").strip()
        if sec and sec.lower() != "no":
            findings.append(f"Security: {sec}")
        effort = str(review.get("estimated_effort_to_review_[1-5]") or "").strip()
        tests = str(review.get("relevant_tests") or "").strip()
        summary_bits = []
        if effort:
            summary_bits.append(f"Estimated review effort: {effort}/5.")
        if tests:
            summary_bits.append(f"Relevant tests: {tests}.")
        summary = " ".join(summary_bits) or "Review completed."
        return {
            "title": "PR Review Summary",
            "summary": summary[:1200],
            "sections": {
                "findings": "\n".join(f"- {f}" for f in findings) if findings else "- No key issues identified by review.",
            },
            "labels": ["review"],
        }

    # PR improve schema
    if isinstance(data.get("code_suggestions"), list):
        suggestions: list[str] = []
        labels: list[str] = []
        for item in data["code_suggestions"]:
            if not isinstance(item, dict):
                continue
            sentence = str(item.get("one_sentence_summary") or "").strip()
            if not sentence:
                sentence = str(item.get("suggestion_summary") or "").strip()
            content = str(item.get("suggestion_content") or "").strip()
            if not content:
                content = str(item.get("why") or "").strip()
            label = str(item.get("label") or "").strip().lower()
            if label:
                labels.append(label)
            if sentence:
                suggestions.append(sentence)
            elif content:
                suggestions.append(content.splitlines()[0].strip())
        summary = suggestions[0] if suggestions else "No actionable code suggestions generated."
        return {
            "title": "Code Improvement Suggestions",
            "summary": summary[:1200],
            "sections": {
                "suggestions": "\n".join(f"- {s}" for s in suggestions)
                if suggestions
                else "- No actionable suggestions generated by improve.",
            },
            "labels": sorted(set(labels or ["improve"])),
        }

    title = str(data.get("title") or "").strip() or None
    description = str(data.get("description") or "").strip() or None
    sections: dict[str, str] = {}
    if description:
        sections["summary"] = description

    files = data.get("pr_files")
    file_lines: list[str] = []
    if isinstance(files, list):
        for item in files:
            if not isinstance(item, dict):
                continue
            fname = str(item.get("filename") or "").strip()
            cs = str(item.get("changes_summary") or "").strip()
            ct = str(item.get("changes_title") or "").strip()
            if fname and ct:
                file_lines.append(f"- {fname}: {ct}")
            elif fname and cs:
                first = cs.splitlines()[0].strip()
                file_lines.append(f"- {fname}: {first}")
            elif fname:
                file_lines.append(f"- {fname}")
    if file_lines:
        sections["changes"] = "\n".join(file_lines)

    labels: list[str] = []
    typ = data.get("type")
    if isinstance(typ, list):
        labels = [str(t).strip().lower() for t in typ if str(t).strip()]
    elif isinstance(typ, str) and typ.strip():
        labels = [typ.strip().lower()]

    summary = sections.get("summary") or ""
    return {
        "title": title,
        "summary": summary[:1200] if summary else None,
        "sections": sections,
        "labels": sorted(set(labels)),
    }


def _parse_actionable_items(text: str, limit: int = 12) -> list[str]:
    items: list[str] = []
    for line in text.splitlines():
        s = line.strip()
        if not s:
            continue
        if re.match(r"^[-*]\s+", s) or re.match(r"^\d+\.\s+", s):
            item = re.sub(r"^[-*\d\.\s]+", "", s).strip()
            if len(item) >= 16:
                items.append(item)
                if len(items) >= limit:
                    return items
    if items:
        return items

    sentence_candidates = re.split(r"[.\n]", text)
    for sent in sentence_candidates:
        s = sent.strip()
        if len(s) < 24:
            continue
        lowered = s.lower()
        if any(k in lowered for k in ("should", "consider", "recommend", "risk", "improve", "fix")):
            items.append(s)
            if len(items) >= limit:
                break
    return items


def _post_process_for_tool(parsed: dict[str, Any], cleaned_source: str, tool: str) -> dict[str, Any]:
    out = dict(parsed)
    sections = dict(out.get("sections") or {})
    if tool in {"review", "improve"}:
        key = "findings" if tool == "review" else "suggestions"
        existing = str(sections.get(key) or "").strip()
        if not existing:
            items = _parse_actionable_items(cleaned_source, limit=12)
            if items:
                sections[key] = "\n".join(f"- {i}" for i in items)
            if not out.get("summary") and items:
                out["summary"] = items[0][:300]
    out["sections"] = sections
    return out


def _render_clean_markdown(parsed: dict[str, Any], default_title: str = "Qodo Describe") -> str:
    title = parsed.get("title") or default_title
    sections: dict[str, str] = dict(parsed.get("sections") or {})
    lines = [f"# {title}", ""]

    summary = parsed.get("summary")
    if summary:
        lines.extend(["## Summary", str(summary).strip(), ""])
    if "changes" in sections:
        lines.extend(["## Changes", sections["changes"].strip(), ""])

    for key, value in sections.items():
        if key in {"summary", "changes"}:
            continue
        if not value.strip():
            continue
        heading = key.replace("_", " ").strip().title()
        lines.extend([f"## {heading}", value.strip(), ""])

    rendered = "\n".join(lines).strip()
    if rendered == f"# {title}":
        rendered = f"# {title}\n\n## Summary\nNo structured content was extracted for this tool run."
    return rendered + "\n"


def _resolve_qodo_command(mr_url: str, tool: str) -> str:
    env_map = {
        "describe": "QODO_DESCRIBE_CMD",
        "review": "QODO_REVIEW_CMD",
        "improve": "QODO_IMPROVE_CMD",
    }
    alt_env_map = {
        "improve": "QODO_ANALYZE_CMD",
    }
    env_key = env_map[tool]
    template = os.getenv(env_key, "").strip()
    if not template and tool in alt_env_map:
        template = os.getenv(alt_env_map[tool], "").strip()
    if not template and tool != "describe":
        base = os.getenv("QODO_DESCRIBE_CMD", "").strip()
        if base:
            # fallback: adapt describe command to target tool
            template = re.sub(r"\bdescribe\b", tool, base)
    if not template:
        raise ValueError(f"{env_key} is required (must include {{mr_url}})")
    if "{mr_url}" not in template:
        raise ValueError(f"{env_key} must include {{mr_url}} placeholder")
    return template.replace("{mr_url}", mr_url)


def _is_pr_agent_command(cmd: str) -> bool:
    lowered = cmd.lower()
    return "pr_agent.cli" in lowered or "pr-agent" in lowered


def _has_arg(cmd_args: list[str], prefix: str) -> bool:
    return any(a.startswith(prefix) for a in cmd_args)


def _ensure_gitlab_provider_args(cmd_args: list[str]) -> list[str]:
    has_provider = _has_arg(cmd_args, "--config.git_provider=")
    has_gitlab_url = _has_arg(cmd_args, "--gitlab.url=")
    has_publish_output = _has_arg(cmd_args, "--config.publish_output=")
    has_publish_labels = _has_arg(cmd_args, "--pr_description.publish_labels=")
    has_verbosity = _has_arg(cmd_args, "--config.verbosity_level=")

    gitlab_url = os.getenv("GITLAB_BASE_URL", "").strip()

    # Never pass PAT via CLI args; keep credentials in environment only.
    merged = [arg for arg in cmd_args if not arg.startswith("--gitlab.personal_access_token=")]
    if not has_provider:
        merged.append("--config.git_provider=gitlab")
    if gitlab_url and not has_gitlab_url:
        merged.append(f"--gitlab.url={gitlab_url.rstrip('/')}")
    if not has_publish_output:
        merged.append("--config.publish_output=false")
    if not has_publish_labels:
        merged.append("--pr_description.publish_labels=false")
    if not has_verbosity:
        merged.append("--config.verbosity_level=2")
    return merged


def _build_pr_agent_env() -> dict[str, str]:
    env = dict(os.environ)
    gitlab_url = os.getenv("GITLAB_BASE_URL", "").strip()
    gitlab_token = os.getenv("GITLAB_TOKEN", "").strip()
    env.setdefault("CONFIG__GIT_PROVIDER", "gitlab")
    if gitlab_url:
        env.setdefault("GITLAB__URL", gitlab_url.rstrip("/"))
    if gitlab_token:
        env.setdefault("GITLAB__PERSONAL_ACCESS_TOKEN", gitlab_token)
    return env


def _strip_ansi(text: str) -> str:
    return re.sub(r"\x1B\[[0-9;]*[A-Za-z]", "", text)


def _extract_markdown(raw: str) -> str:
    cleaned = _strip_ansi(raw)
    log_line = re.compile(r"^\d{4}-\d{2}-\d{2} .* \| (DEBUG|INFO|WARNING|ERROR)\s+\| ")
    lines = [line for line in cleaned.splitlines() if not log_line.match(line)]
    text = "\n".join(lines).strip()
    if not text:
        return ""

    md_starts = ("#", "##", "###", "####", "-", "*", "```", ">")
    split = text.splitlines()
    start_idx = None
    for i, line in enumerate(split):
        if line.strip().startswith(md_starts):
            start_idx = i
            break
    if start_idx is not None:
        text = "\n".join(split[start_idx:]).strip()
    return text


def _extract_ai_response(raw: str) -> str:
    blocks = _extract_ai_response_blocks(raw)
    if not blocks:
        return ""
    return blocks[-1].strip()


def _extract_ai_response_blocks(raw: str) -> list[str]:
    cleaned = _strip_ansi(raw)
    marker = "AI response:"
    starts = [m.start() for m in re.finditer(re.escape(marker), cleaned)]
    if not starts:
        return []
    out: list[str] = []
    for idx, start in enumerate(starts):
        seg_start = start + len(marker)
        seg_end = starts[idx + 1] if idx + 1 < len(starts) else len(cleaned)
        tail = cleaned[seg_start:seg_end].lstrip()
        stop = re.search(r"\n\d{4}-\d{2}-\d{2} .* \| (DEBUG|INFO|WARNING|ERROR)\s+\| ", tail)
        if stop:
            tail = tail[: stop.start()]
        tail = tail.strip()
        if tail:
            out.append(tail)
    return out


def _run_qodo_for_mr(
    mr_row: dict[str, Any],
    options: EnrichOptions,
    tool: str,
) -> dict[str, Any]:
    mr_url = (mr_row.get("web_url") or "").strip()
    if not mr_url:
        return {
            "mr_id": mr_row["id"],
            "status": "skipped",
            "reason": "missing_web_url",
        }

    cmd = _resolve_qodo_command(mr_url, tool)
    cmd_args = shlex.split(cmd)
    cmd_env: dict[str, str] | None = None
    if _is_pr_agent_command(cmd):
        cmd_args = _ensure_gitlab_provider_args(cmd_args)
        cmd_env = _build_pr_agent_env()
    display_cmd = _redact_secrets(" ".join(shlex.quote(arg) for arg in cmd_args))
    started = _now_iso()
    try:
        proc = subprocess.run(
            cmd_args,
            capture_output=True,
            text=True,
            timeout=options.timeout_sec,
            shell=False,
            env=cmd_env,
        )
        finished = _now_iso()
    except subprocess.TimeoutExpired as exc:
        return {
            "mr_id": mr_row["id"],
            "status": "failed",
            "command": display_cmd,
            "exit_code": None,
            "stderr": f"timeout after {options.timeout_sec}s: {exc}",
            "started_at": started,
            "finished_at": _now_iso(),
        }

    if proc.returncode != 0:
        return {
            "mr_id": mr_row["id"],
            "status": "failed",
            "command": display_cmd,
            "exit_code": proc.returncode,
            "stderr": _redact_secrets((proc.stderr or "")[-2000:]),
            "started_at": started,
            "finished_at": finished,
        }

    raw_output = "\n".join(part for part in [proc.stdout or "", proc.stderr or ""] if part).strip()
    redacted_raw_output = _redact_secrets(raw_output)
    if _is_pr_agent_command(cmd):
        raw_markdown = _extract_ai_response(raw_output) or _extract_markdown(raw_output)
    else:
        raw_stdout = (proc.stdout or "").strip()
        raw_markdown = _extract_markdown(raw_stdout) or raw_stdout

    yaml_parsed = _parse_yaml_payload(raw_output)
    cleaned_source = _sanitize_content(raw_markdown or raw_output)
    prompt_leak_markers = _detect_prompt_leaks(raw_output)
    parsed = yaml_parsed or _parse_markdown_sections(cleaned_source)
    parsed = _post_process_for_tool(parsed, cleaned_source, tool)
    parsed["qodo_type"] = _infer_qodo_type(cleaned_source)

    required_by_tool: dict[str, tuple[str, ...]] = {
        "describe": MIN_REQUIRED_SECTIONS,
        "review": ("findings",),
        "improve": ("suggestions",),
    }
    required_keys = required_by_tool.get(tool, MIN_REQUIRED_SECTIONS)
    missing_required = [key for key in required_keys if not (parsed.get("sections", {}) or {}).get(key)]
    quality_status = "ok" if cleaned_source and not missing_required else "partial"
    if not cleaned_source:
        quality_status = "failed"
    if tool in {"review", "improve"} and any(x in cleaned_source for x in ("__new hunk__", "__old hunk__", "\n@@")) and missing_required:
        quality_status = "failed"

    if quality_status != "failed" and not (parsed.get("sections") or {}):
        snippet = cleaned_source.strip().splitlines()
        snippet_text = "\n".join(snippet[:10]).strip()
        if snippet_text:
            parsed["summary"] = snippet_text[:1200]
            parsed["sections"] = {"summary": parsed["summary"]}

    reviewer_summary, reviewer_summary_status = _select_reviewer_summary(parsed, quality_status, prompt_leak_markers)
    context_quality_score = _context_quality_score(
        quality_status=quality_status,
        reviewer_summary_status=reviewer_summary_status,
        prompt_leak_count=len(prompt_leak_markers),
        missing_required_count=len(missing_required),
    )

    markdown = (
        _render_clean_markdown(parsed, default_title=f"Qodo {tool.title()}")
        if quality_status != "failed"
        else f"# Qodo {tool.title()}\n\n(Parsing failed.)\n"
    )

    base = Path(options.output_root) / str(mr_row["project_id"]) / str(mr_row["iid"])
    base.mkdir(parents=True, exist_ok=True)
    md_path = base / f"{tool}.md"
    md_path.write_text(markdown, encoding="utf-8")
    raw_path = base / f"{tool}.raw.log"
    raw_path.write_text(redacted_raw_output, encoding="utf-8")
    return {
        "mr_id": mr_row["id"],
        "project_id": mr_row["project_id"],
        "mr_iid": mr_row["iid"],
        "tool": tool,
        "status": "success" if quality_status != "failed" else "failed",
        "command": display_cmd,
        "exit_code": proc.returncode,
        "stderr": _redact_secrets((proc.stderr or "")[-2000:]) if quality_status != "failed" else "parse_failed",
        "started_at": started,
        "finished_at": finished,
        "markdown_path": str(md_path),
        "raw_output_path": str(raw_path),
        "markdown": markdown,
        "content_sha256": _sha256(markdown),
        "qodo_title": parsed.get("title"),
        "qodo_type": parsed.get("qodo_type"),
        "qodo_summary": parsed.get("summary"),
        "qodo_sections": parsed.get("sections", {}),
        "qodo_labels": parsed.get("labels", []),
        "parser_version": PARSER_VERSION,
        "quality_status": quality_status,
        "reviewer_summary": reviewer_summary,
        "reviewer_summary_status": reviewer_summary_status,
        "context_quality_score": context_quality_score,
        "prompt_leak_count": len(prompt_leak_markers),
        "prompt_leak_markers": prompt_leak_markers,
        "structured_payload": parsed,
    }


def _build_project_compaction(markdowns: list[str], max_tokens: int) -> str:
    def _extract_title(md: str) -> str:
        for line in md.splitlines():
            if line.strip().startswith("#"):
                return line.lstrip("#").strip()
        return "Untitled MR"

    def _extract_summary(md: str) -> str:
        lines = md.splitlines()
        for i, line in enumerate(lines):
            if line.strip().lower().startswith("## summary"):
                body: list[str] = []
                for nxt in lines[i + 1 :]:
                    if nxt.strip().startswith("## "):
                        break
                    if nxt.strip():
                        body.append(nxt.strip())
                if body:
                    return " ".join(body)[:320]
        non_empty = [l.strip() for l in lines if l.strip() and not l.strip().startswith("#")]
        return (" ".join(non_empty)[:320]) if non_empty else ""

    def _is_dependency_change(text: str) -> bool:
        lowered = text.lower()
        keywords = (
            "dependency",
            "version",
            "package.json",
            "package-lock",
            "npm",
            "yarn",
            "bump",
            "upgrade",
        )
        return any(k in lowered for k in keywords)

    def _risk_tags(text: str) -> list[str]:
        lowered = text.lower()
        tags: list[str] = []
        mapping = [
            ("security", "security"),
            ("auth", "auth"),
            ("payment", "payment"),
            ("checkout", "checkout"),
            ("address", "address"),
            ("error", "error-handling"),
            ("fail", "failure-path"),
            ("migration", "migration"),
            ("breaking", "breaking-change"),
            ("rollback", "rollback-risk"),
            ("infra", "infra"),
        ]
        for needle, tag in mapping:
            if needle in lowered and tag not in tags:
                tags.append(tag)
        return tags

    tokens_budget = max(600, max_tokens)
    used = 0
    dependency_lines: list[str] = []
    risk_lines: list[str] = []
    chunks: list[str] = [
        "# Project Compact (Agent Context)",
        "",
        "## Reading Guide",
        "- Focus on intent, high-impact changes, and risk signals.",
        "- Use this as quick context before opening full MR artifacts.",
        "",
        "## MR Digests",
    ]
    for idx, text in enumerate(markdowns, start=1):
        title = _extract_title(text)
        summary = _extract_summary(text)
        merged = f"{title} {summary}".strip()
        if _is_dependency_change(merged):
            dependency_lines.append(f"- MR {idx}: {title}")
        tags = _risk_tags(merged)
        if tags:
            risk_lines.append(f"- MR {idx}: {title} [{', '.join(tags)}]")
        block = f"### MR {idx}: {title}\n- Summary: {summary or 'No summary extracted.'}\n"
        words = block.split()
        if used + len(words) > tokens_budget:
            break
        chunks.append(block)
        used += len(words)
    chunks.extend(["", "## Risk Signals"])
    if risk_lines:
        chunks.extend(risk_lines[:20])
    else:
        chunks.append("- No high-risk keywords detected in compacted MRs.")
    chunks.extend(["", "## Dependency Changes"])
    if dependency_lines:
        chunks.extend(dependency_lines[:20])
    else:
        chunks.append("- No explicit dependency/version changes detected in compacted MRs.")
    if used == 0:
        chunks.append("No enrichment content found.")
    return "\n".join(chunks).strip() + "\n"


def _build_project_mermaid(type_counts: dict[str, int], markdowns: list[str] | None = None) -> str:
    markdowns = markdowns or []

    def _is_dependency_change(text: str) -> bool:
        lowered = text.lower()
        keywords = ("dependency", "version", "package.json", "package-lock", "npm", "yarn", "bump", "upgrade")
        return any(k in lowered for k in keywords)

    def _has_risk_signal(text: str) -> bool:
        lowered = text.lower()
        needles = ("security", "auth", "payment", "checkout", "error", "fail", "migration", "breaking", "rollback", "infra")
        return any(n in lowered for n in needles)

    dep_count = 0
    risk_count = 0
    for md in markdowns:
        compact = " ".join(md.splitlines()[:40])
        if _is_dependency_change(compact):
            dep_count += 1
        if _has_risk_signal(compact):
            risk_count += 1

    lines = ["graph TD"]
    lines.append('  root["Project Knowledge Map"]')
    if not type_counts and not markdowns:
        lines.append('  root --> none["No enriched data"]')
        return "\n".join(lines) + "\n"
    lines.append('  root --> types["Change Types"]')
    for t, count in sorted(type_counts.items(), key=lambda x: (-x[1], x[0])):
        node_id = re.sub(r"[^a-zA-Z0-9_]", "_", t or "unknown")
        lines.append(f'  types --> {node_id}["{t}: {count}"]')
    lines.append('  root --> signals["Signals"]')
    lines.append(f'  signals --> risk["Risk Signals: {risk_count}"]')
    lines.append(f'  signals --> deps["Dependency Changes: {dep_count}"]')
    return "\n".join(lines) + "\n"


def _load_project_candidates(conn, project_id: int, options: EnrichOptions) -> list[dict[str, Any]]:
    filters = ["m.project_id = ?", "m.web_url IS NOT NULL", "m.web_url != ''"]
    params: list[Any] = [project_id]

    if options.data_source != "all":
        filters.append("m.data_source = ?")
        params.append(options.data_source)

    if options.only_missing and not options.force:
        tool_placeholders = ",".join(["?"] * len(options.tools))
        filters.append(
            f"""(
                SELECT COUNT(*)
                FROM mr_qodo_artifacts qa
                WHERE qa.mr_id = m.id AND qa.tool IN ({tool_placeholders})
            ) < {len(options.tools)}"""
        )
        params.extend(list(options.tools))

    where = " AND ".join(filters)
    limit_sql = ""
    if options.mr_limit is not None:
        limit_sql = " LIMIT ?"
        params.append(options.mr_limit)

    rows = conn.execute(
        f"""
        SELECT m.id, m.project_id, m.iid, m.web_url, m.updated_at
        FROM merge_requests m
        WHERE {where}
        ORDER BY m.updated_at DESC
        {limit_sql}
        """,
        tuple(params),
    ).fetchall()
    return [dict(r) for r in rows]


def _load_stratified_pool(
    conn,
    project_ids: list[int],
    options: EnrichOptions,
    candidate_data_source: str,
) -> list[dict[str, Any]]:
    if not project_ids:
        return []
    placeholders = ",".join(["?"] * len(project_ids))
    filters = [
        f"m.project_id IN ({placeholders})",
        "m.web_url IS NOT NULL",
        "m.web_url != ''",
    ]
    params: list[Any] = list(project_ids)

    if candidate_data_source != "all":
        filters.append("m.data_source = ?")
        params.append(candidate_data_source)

    if options.only_missing and not options.force:
        tool_placeholders = ",".join(["?"] * len(options.tools))
        filters.append(
            f"""(
                SELECT COUNT(*)
                FROM mr_qodo_artifacts qa
                WHERE qa.mr_id = m.id AND qa.tool IN ({tool_placeholders})
            ) < {len(options.tools)}"""
        )
        params.extend(list(options.tools))

    where = " AND ".join(filters)
    rows = conn.execute(
        f"""
        SELECT
          m.id AS mr_id,
          m.id AS id,
          m.project_id,
          m.iid AS mr_iid,
          m.iid,
          m.web_url,
          m.updated_at,
          c.final_type,
          c.complexity_score
        FROM merge_requests m
        JOIN mr_classifications c ON c.mr_id = m.id
        WHERE {where}
        ORDER BY c.complexity_score DESC, m.updated_at DESC, m.project_id ASC, m.iid ASC
        """,
        tuple(params),
    ).fetchall()
    return [dict(r) for r in rows]


def _select_stratified_soft_global(candidates: list[dict[str, Any]], count: int) -> list[dict[str, Any]]:
    if count <= 0 or not candidates:
        return []

    ranked = list(candidates)
    count = min(count, len(ranked))
    if count == len(ranked):
        return ranked

    by_type: dict[str, list[dict[str, Any]]] = {}
    for row in ranked:
        t = (row.get("final_type") or "unknown").strip() or "unknown"
        by_type.setdefault(t, []).append(row)

    seed_slots = min(len(by_type), math.floor(count * 0.4))
    max_per_type = max(1, math.ceil(count * 0.4))
    selected: list[dict[str, Any]] = []
    selected_ids: set[int] = set()
    per_type_count: dict[str, int] = {}

    type_order = sorted(by_type.keys(), key=lambda t: ranked.index(by_type[t][0]))
    for t in type_order:
        if len(selected) >= seed_slots:
            break
        row = by_type[t][0]
        selected.append(row)
        selected_ids.add(int(row["mr_id"]))
        per_type_count[t] = 1

    for row in ranked:
        if len(selected) >= count:
            break
        mr_id = int(row["mr_id"])
        if mr_id in selected_ids:
            continue
        t = (row.get("final_type") or "unknown").strip() or "unknown"
        if per_type_count.get(t, 0) >= max_per_type:
            continue
        selected.append(row)
        selected_ids.add(mr_id)
        per_type_count[t] = per_type_count.get(t, 0) + 1

    if len(selected) < count:
        for row in ranked:
            if len(selected) >= count:
                break
            mr_id = int(row["mr_id"])
            if mr_id in selected_ids:
                continue
            selected.append(row)
            selected_ids.add(mr_id)
    return selected


def _select_with_type_balance(candidates: list[dict[str, Any]], count: int, type_balance: str) -> list[dict[str, Any]]:
    if type_balance == "none":
        return candidates[:count]
    if type_balance == "soft":
        return _select_stratified_soft_global(candidates, count)
    if type_balance == "hard":
        selected = _select_stratified_soft_global(candidates, count)
        max_per_type = max(1, math.ceil(count * 0.4))
        out: list[dict[str, Any]] = []
        per_type: dict[str, int] = {}
        for row in selected:
            t = (row.get("final_type") or "unknown").strip() or "unknown"
            if per_type.get(t, 0) >= max_per_type:
                continue
            out.append(row)
            per_type[t] = per_type.get(t, 0) + 1
        return out
    return candidates[:count]


def select_enrich_candidates(
    db: Database,
    project_ids: list[int],
    options: EnrichOptions,
    candidate_options: CandidateOptions,
) -> list[dict[str, Any]]:
    if candidate_options.mode != "stratified":
        return []

    with db.connect() as conn:
        pool = _load_stratified_pool(conn, project_ids, options, candidate_options.data_source)
    if not pool:
        return []

    count = max(1, int(candidate_options.count))
    if candidate_options.scope == "global":
        return _select_with_type_balance(pool, count, candidate_options.type_balance)

    if candidate_options.scope == "per-project":
        grouped: dict[int, list[dict[str, Any]]] = {}
        for row in pool:
            grouped.setdefault(int(row["project_id"]), []).append(row)
        project_order = sorted(grouped.keys())
        base = count // len(project_order)
        rem = count % len(project_order)
        out: list[dict[str, Any]] = []
        for idx, pid in enumerate(project_order):
            quota = base + (1 if idx < rem else 0)
            if quota <= 0:
                continue
            out.extend(_select_with_type_balance(grouped[pid], quota, candidate_options.type_balance))
        return out[:count]

    # hybrid: seed one top candidate per project first, then fill globally.
    grouped = {}
    for row in pool:
        grouped.setdefault(int(row["project_id"]), []).append(row)
    seeds: list[dict[str, Any]] = []
    for pid in sorted(grouped.keys()):
        seeds.append(grouped[pid][0])
    seed_ids = {int(r["mr_id"]) for r in seeds}
    remaining = [r for r in pool if int(r["mr_id"]) not in seed_ids]
    needed = max(0, count - len(seeds))
    filled = _select_with_type_balance(remaining, needed, candidate_options.type_balance)
    return (seeds + filled)[:count]


def enrich_qodo_project(
    db: Database,
    project_id: int,
    options: EnrichOptions,
    candidates: list[dict[str, Any]] | None = None,
    on_result: Callable[[dict[str, Any], int, int], None] | None = None,
) -> dict[str, Any]:
    db.init_schema()
    selected_candidates = candidates
    if selected_candidates is None:
        with db.connect() as conn:
            selected_candidates = _load_project_candidates(conn, project_id, options)

    if not selected_candidates:
        return {"project_id": project_id, "eligible": 0, "success": 0, "failed": 0, "skipped": 0}

    success = 0
    failed = 0
    skipped = 0
    results: list[dict[str, Any]] = []

    with ThreadPoolExecutor(max_workers=max(1, options.concurrency)) as ex:
        future_map = {
            ex.submit(_run_qodo_for_mr, row, options, tool): (row, tool)
            for row in selected_candidates
            for tool in options.tools
        }
        total_runs = len(future_map)
        processed_runs = 0
        running_success = 0
        running_failed = 0
        running_skipped = 0
        for future in as_completed(future_map):
            res = future.result()
            results.append(res)
            processed_runs += 1
            if res["status"] == "success":
                running_success += 1
            elif res["status"] == "failed":
                running_failed += 1
            else:
                running_skipped += 1
            if on_result is not None:
                on_result(res, processed_runs, total_runs)
            if options.progress:
                print(
                    f"[project {project_id}] progress {processed_runs}/{total_runs} "
                    f"success={running_success} failed={running_failed} skipped={running_skipped}"
                )

    with db.connect() as conn:
        for res in results:
            status = res["status"]
            db.insert_qodo_run(
                conn,
                {
                    "mr_id": res["mr_id"],
                    "tool": res.get("tool", "describe"),
                    "status": status,
                    "command": res.get("command", ""),
                    "exit_code": res.get("exit_code"),
                    "stderr_excerpt": res.get("stderr"),
                    "started_at": res.get("started_at", _now_iso()),
                    "finished_at": res.get("finished_at", _now_iso()),
                    "attempt": 1,
                },
            )
            if status == "success":
                success += 1
                db.upsert_qodo_artifact(
                    conn,
                    {
                        "mr_id": res["mr_id"],
                        "project_id": res["project_id"],
                        "mr_iid": res["mr_iid"],
                        "tool": res.get("tool", "describe"),
                        "markdown_path": res["markdown_path"],
                        "content_sha256": res["content_sha256"],
                        "qodo_title": res.get("qodo_title"),
                        "qodo_type": res.get("qodo_type"),
                        "qodo_summary": res.get("qodo_summary"),
                        "qodo_sections": res.get("qodo_sections", {}),
                        "qodo_labels": res.get("qodo_labels", []),
                        "raw_output_path": res.get("raw_output_path"),
                        "parser_version": res.get("parser_version"),
                        "quality_status": res.get("quality_status"),
                        "reviewer_summary": res.get("reviewer_summary"),
                        "reviewer_summary_status": res.get("reviewer_summary_status", "missing"),
                        "context_quality_score": float(res.get("context_quality_score", 0.0)),
                        "prompt_leak_count": res.get("prompt_leak_count", 0),
                        "prompt_leak_markers": res.get("prompt_leak_markers", []),
                        "structured_payload": res.get("structured_payload", {}),
                        "updated_at": _now_iso(),
                    },
                )
                if res.get("tool") != "describe":
                    continue
                db.upsert_qodo_describe(
                    conn,
                    {
                        "mr_id": res["mr_id"],
                        "project_id": res["project_id"],
                        "mr_iid": res["mr_iid"],
                        "markdown_path": res["markdown_path"],
                        "content_sha256": res["content_sha256"],
                        "qodo_title": res.get("qodo_title"),
                        "qodo_type": res.get("qodo_type"),
                        "qodo_summary": res.get("qodo_summary"),
                        "qodo_sections": res.get("qodo_sections", {}),
                        "qodo_labels": res.get("qodo_labels", []),
                        "raw_output_path": res.get("raw_output_path"),
                        "parser_version": res.get("parser_version"),
                        "quality_status": res.get("quality_status"),
                        "reviewer_summary": res.get("reviewer_summary"),
                        "reviewer_summary_status": res.get("reviewer_summary_status", "missing"),
                        "context_quality_score": float(res.get("context_quality_score", 0.0)),
                        "prompt_leak_count": res.get("prompt_leak_count", 0),
                        "prompt_leak_markers": res.get("prompt_leak_markers", []),
                        "structured_payload": res.get("structured_payload", {}),
                        "updated_at": _now_iso(),
                    },
                )
            elif status == "failed":
                failed += 1
            else:
                skipped += 1

    return {
        "project_id": project_id,
        "eligible": len(selected_candidates),
        "success": success,
        "failed": failed,
        "skipped": skipped,
    }


def compact_project_qodo(db: Database, project_id: int, options: EnrichOptions) -> dict[str, Any]:
    db.init_schema()
    with db.connect() as conn:
        rows = conn.execute(
            """
            SELECT q.markdown_path, q.qodo_type
            FROM mr_qodo_artifacts q
            JOIN merge_requests m ON m.id = q.mr_id
            WHERE q.project_id = ? AND q.tool = 'describe' AND (? = 'all' OR m.data_source = ?)
            ORDER BY m.updated_at DESC
            """,
            (project_id, options.data_source, options.data_source),
        ).fetchall()

    markdowns: list[str] = []
    type_counts: dict[str, int] = {}
    for row in rows:
        path = Path(str(row["markdown_path"]))
        if path.exists():
            markdowns.append(path.read_text(encoding="utf-8"))
        t = (row["qodo_type"] or "unknown").strip() if row["qodo_type"] else "unknown"
        type_counts[t] = type_counts.get(t, 0) + 1

    project_dir = Path(options.output_root) / str(project_id)
    project_dir.mkdir(parents=True, exist_ok=True)

    compact_md = _build_project_compaction(markdowns, options.compact_max_tokens)
    compact_path = project_dir / "compact.md"
    compact_path.write_text(compact_md, encoding="utf-8")

    mermaid_path = project_dir / "overview.mmd"
    if options.include_mermaid:
        mermaid_path.write_text(_build_project_mermaid(type_counts, markdowns), encoding="utf-8")

    content_hash = _sha256(compact_md + (mermaid_path.read_text(encoding="utf-8") if options.include_mermaid else ""))
    with db.connect() as conn:
        db.upsert_project_qodo_compaction(
            conn,
            {
                "project_id": project_id,
                "compact_markdown_path": str(compact_path),
                "overview_mermaid_path": str(mermaid_path) if options.include_mermaid else None,
                "source_mr_count": len(rows),
                "content_sha256": content_hash,
                "updated_at": _now_iso(),
            },
        )

    return {
        "project_id": project_id,
        "source_mr_count": len(rows),
        "compact_markdown_path": str(compact_path),
        "overview_mermaid_path": str(mermaid_path) if options.include_mermaid else None,
    }


def get_enrich_status(db: Database, project_ids: list[int], data_source: str = "production") -> list[dict[str, Any]]:
    db.init_schema()
    out: list[dict[str, Any]] = []
    with db.connect() as conn:
        for project_id in project_ids:
            row = conn.execute(
                """
                SELECT
                  (SELECT COUNT(*) FROM merge_requests m WHERE m.project_id = ? AND m.web_url IS NOT NULL AND (?='all' OR m.data_source=?)) as eligible,
                  (SELECT COUNT(*) FROM mr_qodo_artifacts q JOIN merge_requests m ON m.id=q.mr_id WHERE q.project_id = ? AND q.tool='describe' AND (?='all' OR m.data_source=?)) as enriched,
                  (SELECT COUNT(DISTINCT r.mr_id) FROM mr_qodo_runs r JOIN merge_requests m ON m.id=r.mr_id WHERE m.project_id = ? AND r.status='failed' AND (?='all' OR m.data_source=?)) as failed
                """,
                (project_id, data_source, data_source, project_id, data_source, data_source, project_id, data_source, data_source),
            ).fetchone()
            comp = conn.execute(
                "SELECT compact_markdown_path, overview_mermaid_path, updated_at FROM project_qodo_compaction WHERE project_id = ?",
                (project_id,),
            ).fetchone()
            out.append(
                {
                    "project_id": project_id,
                    "eligible": int(row["eligible"] or 0),
                    "enriched": int(row["enriched"] or 0),
                    "failed": int(row["failed"] or 0),
                    "compact_markdown_path": comp["compact_markdown_path"] if comp else None,
                    "overview_mermaid_path": comp["overview_mermaid_path"] if comp else None,
                    "compacted_at": comp["updated_at"] if comp else None,
                }
            )
    return out
