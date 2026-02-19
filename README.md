# prtool

Python CLI for GitLab Merge Request analysis with SQLite storage, infra-aware type classification, and complexity scoring.

## Quick start

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
prtool init-db
```

You can use a `.env` file (auto-loaded by `prtool`). Start from `.env.example`:

```bash
cp .env.example .env
```

Set environment variables (this is where PAT goes):

- `GITLAB_BASE_URL`
- `GITLAB_TOKEN`  <- put your GitLab PAT here
- `GITLAB_PROJECT_ID` (single project) or `GITLAB_PROJECT_IDS` (comma-separated multiple projects)
- `GITLAB_GROUP_ID` / `GITLAB_GROUP_IDS` (optional: restrict discovery to specific group(s))
- `DB_PATH` (default `./pr_analysis.db`)
- `QODO_DESCRIBE_CMD` (required for `enrich qodo`, must include `{mr_url}`; use `--config.publish_output=false` for read-only mode)
- `QODO_REVIEW_CMD` / `QODO_IMPROVE_CMD` (optional overrides for multi-tool runs)

Example:

```bash
export GITLAB_BASE_URL="https://gitlab.com"
export GITLAB_TOKEN="glpat-xxxxxxxxxxxxxxxx"
export GITLAB_PROJECT_IDS="12345678,23456789,34567890"
export GITLAB_GROUP_ID="your-org/your-group"
export QODO_DESCRIBE_CMD="python -m pr_agent.cli --pr_url={mr_url} describe --config.publish_output=false --pr_description.publish_labels=false --config.verbosity_level=2"
# Optional overrides:
# export QODO_REVIEW_CMD="python -m pr_agent.cli --pr_url={mr_url} review --config.publish_output=false --config.verbosity_level=2"
# export QODO_IMPROVE_CMD="python -m pr_agent.cli --pr_url={mr_url} improve --config.publish_output=false --config.verbosity_level=2"
export DB_PATH="./pr_analysis.db"
```

Optional infra config:

- `INFRA_TICKET_REGEX` (default `INFRA-\\d+,OPS-\\d+`)
- `INFRA_LABEL_ALLOWLIST` (default `infra,platform,devops,sre`)
- `INFRA_KEYWORD_LIST`
- `INFRA_STRONG_THRESHOLD` (default `4.0`)
- `INFRA_WEAK_THRESHOLD` (default `1.5`)

Commands:

```bash
prtool list-projects --project-start-index 1 --project-count 10
prtool list-projects --group-id your-org/your-group --project-start-index 1 --project-count 10
prtool projects count --all-projects
prtool projects count --group-id your-org/your-group
prtool projects count --group-id your-org/your-group --format json --include-ids
prtool projects list --all-projects --format json
prtool sync backfill --project-id 123 --project-id 456 --since 2025-01-01
prtool sync refresh --project-id 123 --project-id 456
prtool sync refresh --all-projects
prtool sync refresh --group-id your-org/your-group
prtool sync refresh --group-id your-org/your-group --concurrency 5
prtool sync refresh --group-id your-org/your-group --light-mode
prtool sync refresh --all-projects --project-start-index 1 --project-count 1
prtool sync refresh --all-projects --project-start-index 2 --project-count 5
prtool classify --project-id 123 --project-id 456
prtool classify --group-id your-org/your-group
prtool classify --all-projects
prtool classify --all-projects --project-start-index 2 --project-count 5
prtool batch run --all-projects
prtool batch run --group-id your-org/your-group
prtool batch run --group-id your-org/your-group --concurrency 5 --light-mode
prtool batch run --all-projects --since 2025-01-01
prtool projects list --project-start-index 2 --project-count 5
prtool seed --project-id 999
prtool view --host 127.0.0.1 --port 8765
prtool enrich qodo --project-id 123 --mr-limit 50 --concurrency 5
prtool enrich qodo --project-id 123 --mr-limit 50 --tools describe,review,improve
prtool enrich qodo --project-id 123 --tools describe,review,improve --candidate-mode stratified --candidate-count 10 --candidate-scope global --candidate-type-balance soft --candidate-data-source production --candidate-preview
prtool enrich qodo --project-id 123 --tools describe,review,improve --candidate-mode stratified --candidate-count 10 --candidate-scope global --candidate-type-balance soft
prtool enrich qodo --group-id your-org/your-group --project-start-index 1 --project-count 3
prtool enrich status --group-id your-org/your-group --format json
prtool cleanup --data-source test
prtool cleanup --data-source test --project-id 12345
prtool export --format csv
prtool audit sample --size 50
prtool demo seed --project-id 999
```

If `--project-id` is omitted, `prtool` uses `GITLAB_PROJECT_IDS` first, then `GITLAB_PROJECT_ID`.
If `--group-id` (or `GITLAB_GROUP_ID(S)`) is provided, project discovery is scoped to that group.
`--all-projects` on `sync` discovers all projects accessible by your PAT.
`--all-projects` on `classify` uses all project IDs already present in SQLite.
`--project-start-index` is 1-based, and `--project-count` selects a window for chunked batch runs.
`projects count` is the canonical command to get total project count for batching.
`projects list` now ranks by `mr_count_all_states` high-to-low by default.
`view` starts a read-only local web screen backed by SQLite.
`enrich qodo` supports a stratified candidate selector (`--candidate-mode stratified`) to pick top-complexity MRs with soft type diversification before running tools.
`--concurrency` controls MR detail fetch workers (default 5).
`--light-mode` fetches metadata/commits/files only (skips discussions/approvals/pipelines for faster sync).
Viewer defaults to `production` data-source rows, supports complexity-level filter, and sorts by complexity high-to-low by default.

## Demo mode (no GitLab credentials required)

```bash
prtool init-db
prtool demo seed --project-id 999
prtool export --format both
```

This seeds representative merge requests for infra and non-infra scenarios, runs classification by default, and allows export/audit flows locally.

## CI and scheduled refresh

Workflow: `.github/workflows/ci-and-refresh.yml`

- Runs tests on push and pull request.
- Runs scheduled refresh every 6 hours.
- For scheduled/manual refresh, configure repository secrets:
  - `GITLAB_BASE_URL`
  - `GITLAB_TOKEN`
  - `GITLAB_PROJECT_IDS` (preferred for multiple projects) or `GITLAB_PROJECT_ID`

GitLab CI pipeline: `.gitlab-ci.yml`

- Runs tests for push/MR/web pipelines.
- Runs refresh/classify/export for scheduled/web pipelines.
- Configure CI/CD variables in GitLab project settings:
  - `GITLAB_BASE_URL`
  - `GITLAB_TOKEN`
  - `GITLAB_PROJECT_IDS` (preferred for multiple projects) or `GITLAB_PROJECT_ID`
