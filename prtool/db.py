from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator


SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS merge_requests (
  id INTEGER PRIMARY KEY,
  project_id INTEGER NOT NULL,
  iid INTEGER NOT NULL,
  title TEXT NOT NULL,
  description TEXT,
  state TEXT,
  author_username TEXT,
  labels_json TEXT NOT NULL,
  web_url TEXT,
  created_at TEXT,
  updated_at TEXT,
  merged_at TEXT,
  closed_at TEXT,
  source_branch TEXT,
  target_branch TEXT,
  data_source TEXT NOT NULL DEFAULT 'production',
  UNIQUE(project_id, iid)
);

CREATE TABLE IF NOT EXISTS mr_commits (
  id INTEGER PRIMARY KEY,
  mr_id INTEGER NOT NULL,
  commit_sha TEXT NOT NULL,
  title TEXT,
  authored_date TEXT,
  FOREIGN KEY(mr_id) REFERENCES merge_requests(id) ON DELETE CASCADE,
  UNIQUE(mr_id, commit_sha)
);

CREATE TABLE IF NOT EXISTS mr_files (
  id INTEGER PRIMARY KEY,
  mr_id INTEGER NOT NULL,
  path TEXT NOT NULL,
  additions INTEGER NOT NULL DEFAULT 0,
  deletions INTEGER NOT NULL DEFAULT 0,
  FOREIGN KEY(mr_id) REFERENCES merge_requests(id) ON DELETE CASCADE,
  UNIQUE(mr_id, path)
);

CREATE TABLE IF NOT EXISTS mr_discussions (
  mr_id INTEGER PRIMARY KEY,
  thread_count INTEGER NOT NULL DEFAULT 0,
  note_count INTEGER NOT NULL DEFAULT 0,
  unresolved_count INTEGER NOT NULL DEFAULT 0,
  FOREIGN KEY(mr_id) REFERENCES merge_requests(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS mr_approvals (
  mr_id INTEGER PRIMARY KEY,
  approvals_required INTEGER NOT NULL DEFAULT 0,
  approvals_given INTEGER NOT NULL DEFAULT 0,
  FOREIGN KEY(mr_id) REFERENCES merge_requests(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS mr_pipelines (
  mr_id INTEGER PRIMARY KEY,
  pipeline_count INTEGER NOT NULL DEFAULT 0,
  failed_count INTEGER NOT NULL DEFAULT 0,
  success_count INTEGER NOT NULL DEFAULT 0,
  retry_count INTEGER NOT NULL DEFAULT 0,
  FOREIGN KEY(mr_id) REFERENCES merge_requests(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS mr_features (
  mr_id INTEGER PRIMARY KEY,
  files_changed INTEGER NOT NULL,
  additions INTEGER NOT NULL,
  deletions INTEGER NOT NULL,
  churn INTEGER NOT NULL,
  commit_count INTEGER NOT NULL,
  review_comment_count INTEGER NOT NULL,
  review_thread_count INTEGER NOT NULL,
  unresolved_thread_count INTEGER NOT NULL,
  pipeline_failed_count INTEGER NOT NULL,
  infra_ticket_match_count INTEGER NOT NULL,
  infra_keyword_score REAL NOT NULL,
  infra_label_match_count INTEGER NOT NULL,
  infra_signal_score REAL NOT NULL,
  infra_signal_level TEXT NOT NULL,
  feature_json TEXT NOT NULL,
  FOREIGN KEY(mr_id) REFERENCES merge_requests(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS mr_classifications (
  mr_id INTEGER PRIMARY KEY,
  base_type TEXT NOT NULL,
  final_type TEXT NOT NULL,
  is_infra_related INTEGER NOT NULL,
  infra_override_applied INTEGER NOT NULL,
  complexity_level TEXT NOT NULL,
  complexity_score REAL NOT NULL,
  classification_rationale_json TEXT NOT NULL,
  classified_at TEXT NOT NULL,
  FOREIGN KEY(mr_id) REFERENCES merge_requests(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS raw_snapshots (
  id INTEGER PRIMARY KEY,
  project_id INTEGER NOT NULL,
  entity_type TEXT NOT NULL,
  entity_key TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  fetched_at TEXT NOT NULL,
  UNIQUE(project_id, entity_type, entity_key)
);

CREATE TABLE IF NOT EXISTS sync_checkpoints (
  id INTEGER PRIMARY KEY,
  project_id INTEGER NOT NULL,
  source TEXT NOT NULL,
  watermark_updated_at TEXT,
  last_mr_iid INTEGER,
  updated_at TEXT NOT NULL,
  UNIQUE(project_id, source)
);

CREATE TABLE IF NOT EXISTS mr_qodo_describe (
  mr_id INTEGER PRIMARY KEY,
  project_id INTEGER NOT NULL,
  mr_iid INTEGER NOT NULL,
  markdown_path TEXT NOT NULL,
  raw_output_path TEXT,
  content_sha256 TEXT NOT NULL,
  qodo_title TEXT,
  qodo_type TEXT,
  qodo_summary TEXT,
  qodo_sections_json TEXT,
  qodo_labels_json TEXT,
  parser_version TEXT,
  quality_status TEXT,
  prompt_leak_count INTEGER NOT NULL DEFAULT 0,
  prompt_leak_markers_json TEXT,
  structured_payload_json TEXT,
  updated_at TEXT NOT NULL,
  FOREIGN KEY(mr_id) REFERENCES merge_requests(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS mr_qodo_runs (
  id INTEGER PRIMARY KEY,
  mr_id INTEGER NOT NULL,
  tool TEXT NOT NULL DEFAULT 'describe',
  status TEXT NOT NULL,
  command TEXT NOT NULL,
  exit_code INTEGER,
  stderr_excerpt TEXT,
  started_at TEXT NOT NULL,
  finished_at TEXT NOT NULL,
  attempt INTEGER NOT NULL DEFAULT 1,
  FOREIGN KEY(mr_id) REFERENCES merge_requests(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS project_qodo_compaction (
  project_id INTEGER PRIMARY KEY,
  compact_markdown_path TEXT NOT NULL,
  overview_mermaid_path TEXT,
  source_mr_count INTEGER NOT NULL,
  content_sha256 TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS mr_qodo_artifacts (
  mr_id INTEGER NOT NULL,
  project_id INTEGER NOT NULL,
  mr_iid INTEGER NOT NULL,
  tool TEXT NOT NULL,
  markdown_path TEXT NOT NULL,
  raw_output_path TEXT,
  content_sha256 TEXT NOT NULL,
  qodo_title TEXT,
  qodo_type TEXT,
  qodo_summary TEXT,
  qodo_sections_json TEXT,
  qodo_labels_json TEXT,
  parser_version TEXT,
  quality_status TEXT,
  prompt_leak_count INTEGER NOT NULL DEFAULT 0,
  prompt_leak_markers_json TEXT,
  structured_payload_json TEXT,
  updated_at TEXT NOT NULL,
  PRIMARY KEY (mr_id, tool),
  FOREIGN KEY(mr_id) REFERENCES merge_requests(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_mrs_project_iid ON merge_requests(project_id, iid);
CREATE INDEX IF NOT EXISTS idx_mrs_updated_at ON merge_requests(updated_at);
CREATE INDEX IF NOT EXISTS idx_commits_mr_id ON mr_commits(mr_id);
CREATE INDEX IF NOT EXISTS idx_files_mr_id ON mr_files(mr_id);
CREATE INDEX IF NOT EXISTS idx_qodo_runs_mr_started ON mr_qodo_runs(mr_id, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_qodo_runs_status ON mr_qodo_runs(status);
CREATE INDEX IF NOT EXISTS idx_qodo_artifacts_project_tool ON mr_qodo_artifacts(project_id, tool, updated_at DESC);
"""


class Database:
    def __init__(self, path: str) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("PRAGMA foreign_keys = ON")
            yield conn
            conn.commit()
        finally:
            conn.close()

    def init_schema(self) -> None:
        with self.connect() as conn:
            conn.executescript(SCHEMA_SQL)
            self._migrate_schema(conn)

    def _migrate_schema(self, conn: sqlite3.Connection) -> None:
        columns = {r["name"] for r in conn.execute("PRAGMA table_info(merge_requests)").fetchall()}
        if "data_source" not in columns:
            conn.execute("ALTER TABLE merge_requests ADD COLUMN data_source TEXT NOT NULL DEFAULT 'production'")
            # Existing seeded demo data uses example.local URLs; mark as test for filtering.
            conn.execute(
                """
                UPDATE merge_requests
                SET data_source = 'test'
                WHERE web_url LIKE 'https://example.local/%'
                """
            )

        qodo_columns = {r["name"] for r in conn.execute("PRAGMA table_info(mr_qodo_describe)").fetchall()}
        if "raw_output_path" not in qodo_columns:
            conn.execute("ALTER TABLE mr_qodo_describe ADD COLUMN raw_output_path TEXT")
        if "parser_version" not in qodo_columns:
            conn.execute("ALTER TABLE mr_qodo_describe ADD COLUMN parser_version TEXT")
        if "quality_status" not in qodo_columns:
            conn.execute("ALTER TABLE mr_qodo_describe ADD COLUMN quality_status TEXT")
        if "prompt_leak_count" not in qodo_columns:
            conn.execute("ALTER TABLE mr_qodo_describe ADD COLUMN prompt_leak_count INTEGER NOT NULL DEFAULT 0")
        if "prompt_leak_markers_json" not in qodo_columns:
            conn.execute("ALTER TABLE mr_qodo_describe ADD COLUMN prompt_leak_markers_json TEXT")
        if "structured_payload_json" not in qodo_columns:
            conn.execute("ALTER TABLE mr_qodo_describe ADD COLUMN structured_payload_json TEXT")
        qodo_run_columns = {r["name"] for r in conn.execute("PRAGMA table_info(mr_qodo_runs)").fetchall()}
        if "tool" not in qodo_run_columns:
            conn.execute("ALTER TABLE mr_qodo_runs ADD COLUMN tool TEXT NOT NULL DEFAULT 'describe'")

        # Backfill legacy describe rows into tool-specific artifacts table for compatibility.
        conn.execute(
            """
            INSERT OR IGNORE INTO mr_qodo_artifacts (
              mr_id, project_id, mr_iid, tool, markdown_path, raw_output_path, content_sha256,
              qodo_title, qodo_type, qodo_summary, qodo_sections_json, qodo_labels_json,
              parser_version, quality_status, prompt_leak_count, prompt_leak_markers_json,
              structured_payload_json, updated_at
            )
            SELECT
              mr_id, project_id, mr_iid, 'describe', markdown_path, raw_output_path, content_sha256,
              qodo_title, qodo_type, qodo_summary, qodo_sections_json, qodo_labels_json,
              parser_version, quality_status, prompt_leak_count, prompt_leak_markers_json,
              structured_payload_json, updated_at
            FROM mr_qodo_describe
            """
        )

    def upsert_merge_request(self, conn: sqlite3.Connection, mr: dict[str, Any]) -> int:
        conn.execute(
            """
            INSERT INTO merge_requests (
              id, project_id, iid, title, description, state, author_username, labels_json,
              web_url, created_at, updated_at, merged_at, closed_at, source_branch, target_branch, data_source
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(project_id, iid) DO UPDATE SET
              id=excluded.id,
              title=excluded.title,
              description=excluded.description,
              state=excluded.state,
              author_username=excluded.author_username,
              labels_json=excluded.labels_json,
              web_url=excluded.web_url,
              created_at=excluded.created_at,
              updated_at=excluded.updated_at,
              merged_at=excluded.merged_at,
              closed_at=excluded.closed_at,
              source_branch=excluded.source_branch,
              target_branch=excluded.target_branch,
              data_source=excluded.data_source
            """,
            (
                mr["id"],
                mr["project_id"],
                mr["iid"],
                mr.get("title", ""),
                mr.get("description"),
                mr.get("state"),
                mr.get("author_username"),
                json.dumps(mr.get("labels", [])),
                mr.get("web_url"),
                mr.get("created_at"),
                mr.get("updated_at"),
                mr.get("merged_at"),
                mr.get("closed_at"),
                mr.get("source_branch"),
                mr.get("target_branch"),
                mr.get("data_source", "production"),
            ),
        )
        row = conn.execute(
            "SELECT id FROM merge_requests WHERE project_id = ? AND iid = ?",
            (mr["project_id"], mr["iid"]),
        ).fetchone()
        assert row is not None
        return int(row["id"])

    def replace_mr_commits(self, conn: sqlite3.Connection, mr_id: int, commits: list[dict[str, Any]]) -> None:
        conn.execute("DELETE FROM mr_commits WHERE mr_id = ?", (mr_id,))
        conn.executemany(
            """
            INSERT INTO mr_commits (mr_id, commit_sha, title, authored_date)
            VALUES (?, ?, ?, ?)
            """,
            [
                (mr_id, c.get("id") or c.get("sha"), c.get("title"), c.get("authored_date"))
                for c in commits
                if c.get("id") or c.get("sha")
            ],
        )

    def replace_mr_files(self, conn: sqlite3.Connection, mr_id: int, files: list[dict[str, Any]]) -> None:
        conn.execute("DELETE FROM mr_files WHERE mr_id = ?", (mr_id,))
        conn.executemany(
            """
            INSERT INTO mr_files (mr_id, path, additions, deletions)
            VALUES (?, ?, ?, ?)
            """,
            [
                (
                    mr_id,
                    f.get("new_path") or f.get("old_path") or "unknown",
                    int(f.get("additions", 0)),
                    int(f.get("deletions", 0)),
                )
                for f in files
            ],
        )

    def upsert_discussions(self, conn: sqlite3.Connection, mr_id: int, d: dict[str, int]) -> None:
        conn.execute(
            """
            INSERT INTO mr_discussions (mr_id, thread_count, note_count, unresolved_count)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(mr_id) DO UPDATE SET
              thread_count=excluded.thread_count,
              note_count=excluded.note_count,
              unresolved_count=excluded.unresolved_count
            """,
            (mr_id, d["thread_count"], d["note_count"], d["unresolved_count"]),
        )

    def upsert_approvals(self, conn: sqlite3.Connection, mr_id: int, a: dict[str, int]) -> None:
        conn.execute(
            """
            INSERT INTO mr_approvals (mr_id, approvals_required, approvals_given)
            VALUES (?, ?, ?)
            ON CONFLICT(mr_id) DO UPDATE SET
              approvals_required=excluded.approvals_required,
              approvals_given=excluded.approvals_given
            """,
            (mr_id, a["approvals_required"], a["approvals_given"]),
        )

    def upsert_pipelines(self, conn: sqlite3.Connection, mr_id: int, p: dict[str, int]) -> None:
        conn.execute(
            """
            INSERT INTO mr_pipelines (mr_id, pipeline_count, failed_count, success_count, retry_count)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(mr_id) DO UPDATE SET
              pipeline_count=excluded.pipeline_count,
              failed_count=excluded.failed_count,
              success_count=excluded.success_count,
              retry_count=excluded.retry_count
            """,
            (mr_id, p["pipeline_count"], p["failed_count"], p["success_count"], p["retry_count"]),
        )

    def upsert_feature_row(self, conn: sqlite3.Connection, mr_id: int, features: dict[str, Any]) -> None:
        conn.execute(
            """
            INSERT INTO mr_features (
              mr_id, files_changed, additions, deletions, churn, commit_count,
              review_comment_count, review_thread_count, unresolved_thread_count,
              pipeline_failed_count, infra_ticket_match_count, infra_keyword_score,
              infra_label_match_count, infra_signal_score, infra_signal_level, feature_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(mr_id) DO UPDATE SET
              files_changed=excluded.files_changed,
              additions=excluded.additions,
              deletions=excluded.deletions,
              churn=excluded.churn,
              commit_count=excluded.commit_count,
              review_comment_count=excluded.review_comment_count,
              review_thread_count=excluded.review_thread_count,
              unresolved_thread_count=excluded.unresolved_thread_count,
              pipeline_failed_count=excluded.pipeline_failed_count,
              infra_ticket_match_count=excluded.infra_ticket_match_count,
              infra_keyword_score=excluded.infra_keyword_score,
              infra_label_match_count=excluded.infra_label_match_count,
              infra_signal_score=excluded.infra_signal_score,
              infra_signal_level=excluded.infra_signal_level,
              feature_json=excluded.feature_json
            """,
            (
                mr_id,
                int(features["files_changed"]),
                int(features["additions"]),
                int(features["deletions"]),
                int(features["churn"]),
                int(features["commit_count"]),
                int(features["review_comment_count"]),
                int(features["review_thread_count"]),
                int(features["unresolved_thread_count"]),
                int(features["pipeline_failed_count"]),
                int(features["infra_ticket_match_count"]),
                float(features["infra_keyword_score"]),
                int(features["infra_label_match_count"]),
                float(features["infra_signal_score"]),
                features["infra_signal_level"],
                json.dumps(features),
            ),
        )

    def upsert_classification(self, conn: sqlite3.Connection, mr_id: int, c: dict[str, Any]) -> None:
        conn.execute(
            """
            INSERT INTO mr_classifications (
              mr_id, base_type, final_type, is_infra_related, infra_override_applied,
              complexity_level, complexity_score, classification_rationale_json, classified_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(mr_id) DO UPDATE SET
              base_type=excluded.base_type,
              final_type=excluded.final_type,
              is_infra_related=excluded.is_infra_related,
              infra_override_applied=excluded.infra_override_applied,
              complexity_level=excluded.complexity_level,
              complexity_score=excluded.complexity_score,
              classification_rationale_json=excluded.classification_rationale_json,
              classified_at=excluded.classified_at
            """,
            (
                mr_id,
                c["base_type"],
                c["final_type"],
                1 if c["is_infra_related"] else 0,
                1 if c["infra_override_applied"] else 0,
                c["complexity_level"],
                float(c["complexity_score"]),
                json.dumps(c["rationale"]),
                c["classified_at"],
            ),
        )

    def upsert_raw_snapshot(
        self,
        conn: sqlite3.Connection,
        project_id: int,
        entity_type: str,
        entity_key: str,
        payload: dict[str, Any],
        fetched_at: str,
    ) -> None:
        conn.execute(
            """
            INSERT INTO raw_snapshots (project_id, entity_type, entity_key, payload_json, fetched_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(project_id, entity_type, entity_key) DO UPDATE SET
              payload_json=excluded.payload_json,
              fetched_at=excluded.fetched_at
            """,
            (project_id, entity_type, entity_key, json.dumps(payload), fetched_at),
        )

    def load_checkpoint(self, conn: sqlite3.Connection, project_id: int, source: str) -> dict[str, Any] | None:
        row = conn.execute(
            """
            SELECT watermark_updated_at, last_mr_iid
            FROM sync_checkpoints
            WHERE project_id = ? AND source = ?
            """,
            (project_id, source),
        ).fetchone()
        return dict(row) if row else None

    def upsert_checkpoint(
        self,
        conn: sqlite3.Connection,
        project_id: int,
        source: str,
        watermark_updated_at: str | None,
        last_mr_iid: int | None,
        updated_at: str,
    ) -> None:
        conn.execute(
            """
            INSERT INTO sync_checkpoints (project_id, source, watermark_updated_at, last_mr_iid, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(project_id, source) DO UPDATE SET
              watermark_updated_at=excluded.watermark_updated_at,
              last_mr_iid=excluded.last_mr_iid,
              updated_at=excluded.updated_at
            """,
            (project_id, source, watermark_updated_at, last_mr_iid, updated_at),
        )

    def list_ingested_project_ids(self, conn: sqlite3.Connection) -> list[int]:
        rows = conn.execute(
            """
            SELECT DISTINCT project_id
            FROM merge_requests
            ORDER BY project_id ASC
            """
        ).fetchall()
        return [int(r["project_id"]) for r in rows]

    def get_mr_updated_at_map(
        self,
        conn: sqlite3.Connection,
        project_id: int,
        iids: list[int],
    ) -> dict[int, str]:
        if not iids:
            return {}
        placeholders = ",".join(["?"] * len(iids))
        rows = conn.execute(
            f"""
            SELECT iid, updated_at
            FROM merge_requests
            WHERE project_id = ? AND iid IN ({placeholders})
            """,
            (project_id, *iids),
        ).fetchall()
        result: dict[int, str] = {}
        for row in rows:
            updated_at = row["updated_at"]
            if updated_at:
                result[int(row["iid"])] = str(updated_at)
        return result

    def delete_merge_requests_by_source(
        self,
        conn: sqlite3.Connection,
        data_source: str,
        project_id: int | None = None,
    ) -> int:
        if project_id is None:
            cur = conn.execute("DELETE FROM merge_requests WHERE data_source = ?", (data_source,))
        else:
            cur = conn.execute(
                "DELETE FROM merge_requests WHERE data_source = ? AND project_id = ?",
                (data_source, project_id),
            )
        return int(cur.rowcount or 0)

    def upsert_qodo_describe(self, conn: sqlite3.Connection, row: dict[str, Any]) -> None:
        conn.execute(
            """
            INSERT INTO mr_qodo_describe (
              mr_id, project_id, mr_iid, markdown_path, content_sha256, qodo_title, qodo_type,
              qodo_summary, qodo_sections_json, qodo_labels_json, raw_output_path, parser_version,
              quality_status, prompt_leak_count, prompt_leak_markers_json, structured_payload_json, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(mr_id) DO UPDATE SET
              project_id=excluded.project_id,
              mr_iid=excluded.mr_iid,
              markdown_path=excluded.markdown_path,
              content_sha256=excluded.content_sha256,
              qodo_title=excluded.qodo_title,
              qodo_type=excluded.qodo_type,
              qodo_summary=excluded.qodo_summary,
              qodo_sections_json=excluded.qodo_sections_json,
              qodo_labels_json=excluded.qodo_labels_json,
              raw_output_path=excluded.raw_output_path,
              parser_version=excluded.parser_version,
              quality_status=excluded.quality_status,
              prompt_leak_count=excluded.prompt_leak_count,
              prompt_leak_markers_json=excluded.prompt_leak_markers_json,
              structured_payload_json=excluded.structured_payload_json,
              updated_at=excluded.updated_at
            """,
            (
                row["mr_id"],
                row["project_id"],
                row["mr_iid"],
                row["markdown_path"],
                row["content_sha256"],
                row.get("qodo_title"),
                row.get("qodo_type"),
                row.get("qodo_summary"),
                json.dumps(row.get("qodo_sections", {})),
                json.dumps(row.get("qodo_labels", [])),
                row.get("raw_output_path"),
                row.get("parser_version"),
                row.get("quality_status"),
                int(row.get("prompt_leak_count", 0)),
                json.dumps(row.get("prompt_leak_markers", [])),
                json.dumps(row.get("structured_payload", {})),
                row["updated_at"],
            ),
        )

    def insert_qodo_run(self, conn: sqlite3.Connection, row: dict[str, Any]) -> None:
        conn.execute(
            """
            INSERT INTO mr_qodo_runs (
              mr_id, tool, status, command, exit_code, stderr_excerpt, started_at, finished_at, attempt
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                row["mr_id"],
                row.get("tool", "describe"),
                row["status"],
                row["command"],
                row.get("exit_code"),
                row.get("stderr_excerpt"),
                row["started_at"],
                row["finished_at"],
                row.get("attempt", 1),
            ),
        )

    def upsert_qodo_artifact(self, conn: sqlite3.Connection, row: dict[str, Any]) -> None:
        conn.execute(
            """
            INSERT INTO mr_qodo_artifacts (
              mr_id, project_id, mr_iid, tool, markdown_path, raw_output_path, content_sha256,
              qodo_title, qodo_type, qodo_summary, qodo_sections_json, qodo_labels_json,
              parser_version, quality_status, prompt_leak_count, prompt_leak_markers_json,
              structured_payload_json, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(mr_id, tool) DO UPDATE SET
              project_id=excluded.project_id,
              mr_iid=excluded.mr_iid,
              markdown_path=excluded.markdown_path,
              raw_output_path=excluded.raw_output_path,
              content_sha256=excluded.content_sha256,
              qodo_title=excluded.qodo_title,
              qodo_type=excluded.qodo_type,
              qodo_summary=excluded.qodo_summary,
              qodo_sections_json=excluded.qodo_sections_json,
              qodo_labels_json=excluded.qodo_labels_json,
              parser_version=excluded.parser_version,
              quality_status=excluded.quality_status,
              prompt_leak_count=excluded.prompt_leak_count,
              prompt_leak_markers_json=excluded.prompt_leak_markers_json,
              structured_payload_json=excluded.structured_payload_json,
              updated_at=excluded.updated_at
            """,
            (
                row["mr_id"],
                row["project_id"],
                row["mr_iid"],
                row["tool"],
                row["markdown_path"],
                row.get("raw_output_path"),
                row["content_sha256"],
                row.get("qodo_title"),
                row.get("qodo_type"),
                row.get("qodo_summary"),
                json.dumps(row.get("qodo_sections", {})),
                json.dumps(row.get("qodo_labels", [])),
                row.get("parser_version"),
                row.get("quality_status"),
                int(row.get("prompt_leak_count", 0)),
                json.dumps(row.get("prompt_leak_markers", [])),
                json.dumps(row.get("structured_payload", {})),
                row["updated_at"],
            ),
        )

    def upsert_project_qodo_compaction(self, conn: sqlite3.Connection, row: dict[str, Any]) -> None:
        conn.execute(
            """
            INSERT INTO project_qodo_compaction (
              project_id, compact_markdown_path, overview_mermaid_path, source_mr_count,
              content_sha256, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(project_id) DO UPDATE SET
              compact_markdown_path=excluded.compact_markdown_path,
              overview_mermaid_path=excluded.overview_mermaid_path,
              source_mr_count=excluded.source_mr_count,
              content_sha256=excluded.content_sha256,
              updated_at=excluded.updated_at
            """,
            (
                row["project_id"],
                row["compact_markdown_path"],
                row.get("overview_mermaid_path"),
                row["source_mr_count"],
                row["content_sha256"],
                row["updated_at"],
            ),
        )
