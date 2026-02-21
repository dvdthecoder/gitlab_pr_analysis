#!/usr/bin/env bash
set -euo pipefail

# Enforce MR metadata quality for classifier confidence.
# Runs in GitLab MR pipelines and validates:
# 1) Structured non-empty MR description sections.
# 2) Exactly one primary label from allowed set.

if [[ "${CI_PIPELINE_SOURCE:-}" != "merge_request_event" ]]; then
  echo "metadata-check: skipping (CI_PIPELINE_SOURCE=${CI_PIPELINE_SOURCE:-unknown})"
  exit 0
fi

description="${CI_MERGE_REQUEST_DESCRIPTION:-}"
labels_raw="${CI_MERGE_REQUEST_LABELS:-}"

if [[ -z "${description//[[:space:]]/}" ]]; then
  echo "metadata-check: CI_MERGE_REQUEST_DESCRIPTION is empty."
  echo "Fill the MR description using the template sections."
  exit 1
fi

required_sections=("problem" "change" "risk" "test plan")
for section in "${required_sections[@]}"; do
  if ! printf '%s\n' "$description" | grep -Eiq "^##[[:space:]]*${section}[[:space:]]*$"; then
    echo "metadata-check: missing required section header: '## ${section^}'"
    exit 1
  fi
done

# Require some content after each section (not just blank/template text).
# This keeps the signal useful for downstream classification.
for section in "problem" "change" "risk" "test plan"; do
  block="$(printf '%s\n' "$description" | awk -v sec="$section" '
    BEGIN { insec=0 }
    tolower($0) ~ "^##[[:space:]]*" sec "[[:space:]]*$" { insec=1; next }
    /^##[[:space:]]*/ && insec==1 { exit }
    insec==1 { print }
  ')"

  cleaned="$(printf '%s\n' "$block" \
    | sed -E 's/<!--.*-->//g' \
    | sed -E 's/`//g' \
    | tr -d '[:space:]')"

  if [[ -z "$cleaned" ]]; then
    echo "metadata-check: section '## ${section^}' has no content."
    exit 1
  fi
done

allowed_primary=("feature" "bugfix" "refactor" "test-only" "docs-only" "chore" "perf-security" "infra")

labels_normalized="$(printf '%s' "$labels_raw" | tr '[:upper:]' '[:lower:]')"
IFS=',' read -r -a labels <<< "$labels_normalized"

primary_count=0
matched=()
for raw in "${labels[@]:-}"; do
  lbl="$(printf '%s' "$raw" | sed -E 's/^[[:space:]]+|[[:space:]]+$//g')"
  [[ -z "$lbl" ]] && continue
  for p in "${allowed_primary[@]}"; do
    if [[ "$lbl" == "$p" ]]; then
      primary_count=$((primary_count + 1))
      matched+=("$lbl")
      break
    fi
  done
done

if [[ "$primary_count" -ne 1 ]]; then
  echo "metadata-check: expected exactly 1 primary label from: ${allowed_primary[*]}"
  echo "metadata-check: found $primary_count in CI_MERGE_REQUEST_LABELS='${labels_raw}'"
  if [[ ${#matched[@]} -gt 0 ]]; then
    echo "metadata-check: matched primary labels: ${matched[*]}"
  fi
  exit 1
fi

echo "metadata-check: passed (primary_label=${matched[0]})."
