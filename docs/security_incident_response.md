# Security Incident Response: Disabled/Leaked API Key

Use this checklist when a provider disables a key (for example `OPENAI_API_KEY`) due to leak detection.

## 1) Immediate containment

1. Revoke the disabled key in provider dashboard.
2. Create a new key scoped to the minimum required project/org permissions.
3. Update secrets everywhere the key is used:
   - GitHub: repository/environment secrets
   - GitLab: CI/CD variables
   - Local `.env` (never commit)

## 2) Exposure audit

Run local repo and history scans:

```bash
cd /Users/abhishekdwivedi/programming-projects/gitlab_pr_analysis
rg -n "sk-[A-Za-z0-9_-]{20,}|OPENAI_API_KEY|glpat-|-----BEGIN (RSA|EC|OPENSSH|PRIVATE) KEY-----" -S . --hidden --glob '!*.db' --glob '!.venv/**' --glob '!.venv-qodo/**' --glob '!.git/**'
git log --all --oneline -S"OPENAI_API_KEY" -S"sk-" -- .
```

Review provider-side surfaces:

1. GitHub secret-scanning alert details (path, commit, workflow log reference).
2. GitHub Actions logs/artifacts around alert timestamp.
3. GitLab pipeline logs/artifacts around the same timestamp.

## 3) Hardening controls

1. Enable pre-commit secret scanning:

```bash
source .venv/bin/activate
pip install pre-commit
pre-commit install
```

2. Run all hooks once:

```bash
pre-commit run --all-files
```

3. Ensure CI secret scan runs on push/PR.
4. Keep all docs/examples masked (`sk-xxxxxxxx`, `glpat-xxxxxxxx`).

## 4) Verification and closure

1. Confirm the old leaked key is revoked.
2. Confirm new key works in required environments only.
3. Confirm no open secret-scanning alerts on default branch.
4. Document timeline:
   - Detection time
   - Rotation completion time
   - Audit findings
   - Follow-up hardening tasks
