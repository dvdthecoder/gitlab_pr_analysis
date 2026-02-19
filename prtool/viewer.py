from __future__ import annotations

import html
import sqlite3
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, quote_plus, urlparse

COMPLEXITY_COLUMNS = ["Very Low", "Low", "Medium", "High", "Very High"]
DATA_SOURCES = ["production", "test", "all"]

SORT_SQL = {
    "updated_desc": "m.updated_at DESC",
    "complexity_desc": "c.complexity_score DESC, m.updated_at DESC",
    "complexity_asc": "c.complexity_score ASC, m.updated_at DESC",
}


def _open_conn(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _filter_sql(
    project_id: int | None,
    final_type: str | None,
    complexity_level: str | None,
    data_source: str,
) -> tuple[str, tuple[Any, ...]]:
    clauses: list[str] = []
    params: list[Any] = []

    if project_id is not None:
        clauses.append("m.project_id = ?")
        params.append(project_id)
    if final_type:
        clauses.append("c.final_type = ?")
        params.append(final_type)
    if complexity_level:
        clauses.append("c.complexity_level = ?")
        params.append(complexity_level)
    if data_source != "all":
        clauses.append("m.data_source = ?")
        params.append(data_source)

    where = ""
    if clauses:
        where = "WHERE " + " AND ".join(clauses)
    return where, tuple(params)


def get_project_ids(db_path: str, data_source: str = "production") -> list[int]:
    where = ""
    params: tuple[Any, ...] = ()
    if data_source != "all":
        where = "WHERE data_source = ?"
        params = (data_source,)

    with _open_conn(db_path) as conn:
        rows = conn.execute(
            f"""
            SELECT DISTINCT project_id
            FROM merge_requests
            {where}
            ORDER BY project_id ASC
            """,
            params,
        ).fetchall()
    return [int(r["project_id"]) for r in rows]


def get_type_counts(
    db_path: str,
    project_id: int | None = None,
    complexity_level: str | None = None,
    data_source: str = "production",
) -> list[tuple[str, int]]:
    where, params = _filter_sql(
        project_id=project_id,
        final_type=None,
        complexity_level=complexity_level,
        data_source=data_source,
    )
    with _open_conn(db_path) as conn:
        rows = conn.execute(
            f"""
            SELECT c.final_type, COUNT(*) as cnt
            FROM mr_classifications c
            JOIN merge_requests m ON m.id = c.mr_id
            {where}
            GROUP BY c.final_type
            ORDER BY cnt DESC, c.final_type ASC
            """,
            params,
        ).fetchall()
    return [(str(r["final_type"]), int(r["cnt"])) for r in rows]


def get_overview(
    db_path: str,
    project_id: int | None = None,
    final_type: str | None = None,
    complexity_level: str | None = None,
    data_source: str = "production",
) -> dict[str, Any]:
    where, params = _filter_sql(
        project_id=project_id,
        final_type=final_type,
        complexity_level=complexity_level,
        data_source=data_source,
    )

    with _open_conn(db_path) as conn:
        row = conn.execute(
            f"""
            SELECT
              COUNT(*) as total_mrs,
              SUM(CASE WHEN c.is_infra_related = 1 THEN 1 ELSE 0 END) as infra_related,
              AVG(c.complexity_score) as avg_complexity,
              SUM(CASE WHEN c.complexity_level IN ('High','Very High') THEN 1 ELSE 0 END) as high_complexity
            FROM mr_classifications c
            JOIN merge_requests m ON m.id = c.mr_id
            {where}
            """,
            params,
        ).fetchone()

    if row is None:
        return {"total_mrs": 0, "infra_related": 0, "avg_complexity": 0.0, "high_complexity": 0}

    return {
        "total_mrs": int(row["total_mrs"] or 0),
        "infra_related": int(row["infra_related"] or 0),
        "avg_complexity": float(row["avg_complexity"] or 0.0),
        "high_complexity": int(row["high_complexity"] or 0),
    }


def get_heatmap(
    db_path: str,
    project_id: int | None = None,
    final_type: str | None = None,
    data_source: str = "production",
) -> tuple[list[str], dict[str, dict[str, int]], int]:
    where, params = _filter_sql(
        project_id=project_id,
        final_type=final_type,
        complexity_level=None,
        data_source=data_source,
    )

    with _open_conn(db_path) as conn:
        rows = conn.execute(
            f"""
            SELECT c.final_type, c.complexity_level, COUNT(*) as cnt
            FROM mr_classifications c
            JOIN merge_requests m ON m.id = c.mr_id
            {where}
            GROUP BY c.final_type, c.complexity_level
            """,
            params,
        ).fetchall()

    matrix: dict[str, dict[str, int]] = {}
    max_count = 0
    for row in rows:
        ftype = str(row["final_type"])
        level = str(row["complexity_level"])
        cnt = int(row["cnt"])
        matrix.setdefault(ftype, {})[level] = cnt
        max_count = max(max_count, cnt)

    return sorted(matrix.keys()), matrix, max_count


def get_recent_rows(
    db_path: str,
    project_id: int | None = None,
    final_type: str | None = None,
    complexity_level: str | None = None,
    data_source: str = "production",
    limit: int = 200,
    sort_by: str = "complexity_desc",
) -> list[dict[str, Any]]:
    where, params = _filter_sql(
        project_id=project_id,
        final_type=final_type,
        complexity_level=complexity_level,
        data_source=data_source,
    )

    order_sql = SORT_SQL.get(sort_by, SORT_SQL["complexity_desc"])
    with _open_conn(db_path) as conn:
        rows = conn.execute(
            f"""
            SELECT
              m.project_id,
              m.iid,
              m.title,
              m.data_source,
              c.base_type,
              c.final_type,
              c.complexity_level,
              c.complexity_score,
              f.files_changed,
              f.churn,
              f.commit_count,
              f.review_comment_count,
              f.unresolved_thread_count,
              f.pipeline_failed_count,
              f.infra_signal_score,
              m.updated_at
            FROM mr_classifications c
            JOIN merge_requests m ON m.id = c.mr_id
            LEFT JOIN mr_features f ON f.mr_id = c.mr_id
            {where}
            ORDER BY {order_sql}
            LIMIT ?
            """,
            (*params, limit),
        ).fetchall()

    return [dict(r) for r in rows]


def get_enrichment_rows(
    db_path: str,
    project_id: int | None = None,
    final_type: str | None = None,
    complexity_level: str | None = None,
    data_source: str = "production",
    limit: int = 100,
) -> list[dict[str, Any]]:
    where, params = _filter_sql(
        project_id=project_id,
        final_type=final_type,
        complexity_level=complexity_level,
        data_source=data_source,
    )
    with _open_conn(db_path) as conn:
        rows = conn.execute(
            f"""
            SELECT
              m.project_id,
              m.iid,
              q.qodo_title,
              q.qodo_type,
              q.qodo_summary,
              q.markdown_path,
              q.updated_at
            FROM mr_qodo_describe q
            JOIN merge_requests m ON m.id = q.mr_id
            LEFT JOIN mr_classifications c ON c.mr_id = m.id
            {where}
            ORDER BY q.updated_at DESC
            LIMIT ?
            """,
            (*params, limit),
        ).fetchall()
    return [dict(r) for r in rows]


def get_project_compactions(
    db_path: str,
    project_id: int | None = None,
    data_source: str = "production",
    limit: int = 50,
) -> list[dict[str, Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    if project_id is not None:
        clauses.append("m.project_id = ?")
        params.append(project_id)
    if data_source != "all":
        clauses.append("m.data_source = ?")
        params.append(data_source)
    where = ""
    if clauses:
        where = "WHERE " + " AND ".join(clauses)

    with _open_conn(db_path) as conn:
        rows = conn.execute(
            f"""
            SELECT
              p.project_id,
              p.compact_markdown_path,
              p.overview_mermaid_path,
              p.source_mr_count,
              p.updated_at
            FROM project_qodo_compaction p
            JOIN merge_requests m ON m.project_id = p.project_id
            {where}
            GROUP BY p.project_id
            ORDER BY p.updated_at DESC
            LIMIT ?
            """,
            (*params, limit),
        ).fetchall()
    return [dict(r) for r in rows]


def _render_heatmap(rows: list[str], matrix: dict[str, dict[str, int]], max_count: int) -> str:
    if not rows:
        return "<p><small>No heatmap data yet.</small></p>"

    header = "".join([f"<th>{html.escape(c)}</th>" for c in COMPLEXITY_COLUMNS])
    body_rows = []
    for row_name in rows:
        cells = []
        for col in COMPLEXITY_COLUMNS:
            value = matrix.get(row_name, {}).get(col, 0)
            intensity = 0.08 + (0.72 * (value / max_count)) if max_count > 0 else 0.08
            cells.append(f'<td class="heat" style="background: rgba(30,64,175,{intensity:.2f})">{value}</td>')
        body_rows.append(f"<tr><th>{html.escape(row_name)}</th>{''.join(cells)}</tr>")

    return (
        "<div class='tablewrap'>"
        "<table>"
        "<thead><tr><th>Type \\ Complexity</th>" + header + "</tr></thead>"
        "<tbody>" + "".join(body_rows) + "</tbody>"
        "</table>"
        "</div>"
    )


def _html_page(
    db_path: str,
    project_id: int | None,
    final_type: str | None,
    complexity_level: str | None,
    data_source: str,
    limit: int,
    sort_by: str,
) -> str:
    projects = get_project_ids(db_path, data_source=data_source)
    overview = get_overview(
        db_path,
        project_id=project_id,
        final_type=final_type,
        complexity_level=complexity_level,
        data_source=data_source,
    )
    type_counts = get_type_counts(
        db_path,
        project_id=project_id,
        complexity_level=complexity_level,
        data_source=data_source,
    )
    rows = get_recent_rows(
        db_path,
        project_id=project_id,
        final_type=final_type,
        complexity_level=complexity_level,
        data_source=data_source,
        limit=limit,
        sort_by=sort_by,
    )
    heat_rows, heat_matrix, heat_max = get_heatmap(
        db_path,
        project_id=project_id,
        final_type=final_type,
        data_source=data_source,
    )
    enrich_rows = get_enrichment_rows(
        db_path,
        project_id=project_id,
        final_type=final_type,
        complexity_level=complexity_level,
        data_source=data_source,
        limit=min(limit, 300),
    )
    compactions = get_project_compactions(
        db_path,
        project_id=project_id,
        data_source=data_source,
        limit=100,
    )

    project_options = ['<option value="">All</option>']
    for pid in projects:
        selected = " selected" if project_id == pid else ""
        project_options.append(f'<option value="{pid}"{selected}>{pid}</option>')

    type_options = ['<option value="">All</option>']
    for t, _ in type_counts:
        selected = " selected" if final_type == t else ""
        type_options.append(f'<option value="{html.escape(t)}"{selected}>{html.escape(t)}</option>')

    complexity_options = ['<option value="">All</option>']
    for level in COMPLEXITY_COLUMNS:
        selected = " selected" if complexity_level == level else ""
        complexity_options.append(f'<option value="{level}"{selected}>{level}</option>')

    data_source_options = []
    for ds in DATA_SOURCES:
        selected = " selected" if data_source == ds else ""
        data_source_options.append(f'<option value="{ds}"{selected}>{ds}</option>')

    sort_options = []
    for key, label in [
        ("complexity_desc", "Complexity desc"),
        ("complexity_asc", "Complexity asc"),
        ("updated_desc", "Updated desc"),
    ]:
        selected = " selected" if sort_by == key else ""
        sort_options.append(f'<option value="{key}"{selected}>{label}</option>')

    type_badges = "".join([f'<span class="badge">{html.escape(t)}: {count}</span>' for t, count in type_counts])

    table_rows = []
    for r in rows:
        table_rows.append(
            "<tr>"
            f"<td>{r['project_id']}</td>"
            f"<td>{r['iid']}</td>"
            f"<td>{html.escape(str(r['title'] or ''))}</td>"
            f"<td>{html.escape(str(r['base_type']))}</td>"
            f"<td>{html.escape(str(r['final_type']))}</td>"
            f"<td>{html.escape(str(r['complexity_level']))}</td>"
            f"<td>{float(r['complexity_score'] or 0.0):.2f}</td>"
            f"<td>{int(r['files_changed'] or 0)}</td>"
            f"<td>{int(r['churn'] or 0)}</td>"
            f"<td>{int(r['commit_count'] or 0)}</td>"
            f"<td>{int(r['review_comment_count'] or 0)}</td>"
            f"<td>{int(r['unresolved_thread_count'] or 0)}</td>"
            f"<td>{int(r['pipeline_failed_count'] or 0)}</td>"
            f"<td>{html.escape(str(r['updated_at'] or ''))}</td>"
            "</tr>"
        )

    rows_html = "".join(table_rows) if table_rows else "<tr><td colspan='14'>No rows found</td></tr>"
    heatmap_html = _render_heatmap(heat_rows, heat_matrix, heat_max)

    compaction_rows = []
    for row in compactions:
        compact_path = str(row["compact_markdown_path"] or "")
        mermaid_path = str(row["overview_mermaid_path"] or "")
        compact_href = f"/artifact?path={quote_plus(compact_path)}" if compact_path else ""
        mermaid_href = f"/artifact?path={quote_plus(mermaid_path)}" if mermaid_path else ""
        compact_link = f'<a href="{compact_href}" target="_blank">compact.md</a>' if compact_href else "-"
        mermaid_link = f'<a href="{mermaid_href}" target="_blank">overview.mmd</a>' if mermaid_href else "-"
        compaction_rows.append(
            "<tr>"
            f"<td>{int(row['project_id'])}</td>"
            f"<td>{int(row['source_mr_count'] or 0)}</td>"
            f"<td>{html.escape(str(row['updated_at'] or ''))}</td>"
            f"<td>{compact_link}</td>"
            f"<td>{mermaid_link}</td>"
            "</tr>"
        )
    compaction_html = "".join(compaction_rows) if compaction_rows else "<tr><td colspan='5'>No compaction artifacts found</td></tr>"

    enrich_table_rows = []
    for row in enrich_rows:
        md_path = str(row["markdown_path"] or "")
        href = f"/artifact?path={quote_plus(md_path)}" if md_path else ""
        describe_link = f'<a href="{href}" target="_blank">describe.md</a>' if href else "-"
        preview = (row["qodo_summary"] or "")[:300]
        enrich_table_rows.append(
            "<tr>"
            f"<td>{int(row['project_id'])}</td>"
            f"<td>{int(row['iid'])}</td>"
            f"<td>{html.escape(str(row['qodo_type'] or ''))}</td>"
            f"<td>{html.escape(str(row['qodo_title'] or ''))}</td>"
            f"<td>{html.escape(preview)}</td>"
            f"<td>{html.escape(str(row['updated_at'] or ''))}</td>"
            f"<td>{describe_link}</td>"
            "</tr>"
        )
    enrich_html = "".join(enrich_table_rows) if enrich_table_rows else "<tr><td colspan='7'>No enrichment rows found</td></tr>"

    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>PR Analysis Viewer</title>
<style>
body {{ font-family: -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; margin: 20px; background: #f7f8fa; color: #1f2937; }}
.cardwrap {{ display: grid; grid-template-columns: repeat(4, minmax(120px, 1fr)); gap: 12px; margin-bottom: 16px; }}
.card {{ background: white; border: 1px solid #e5e7eb; border-radius: 8px; padding: 12px; }}
.label {{ font-size: 12px; color: #6b7280; }}
.value {{ font-size: 22px; font-weight: 700; }}
form {{ display: flex; gap: 10px; align-items: end; flex-wrap: wrap; margin-bottom: 12px; }}
select, input, button {{ padding: 8px; border-radius: 6px; border: 1px solid #d1d5db; }}
button {{ background: #111827; color: white; border: none; cursor: pointer; }}
.badges {{ margin: 12px 0; display: flex; flex-wrap: wrap; gap: 8px; }}
.badge {{ background: #eef2ff; color: #3730a3; padding: 4px 8px; border-radius: 999px; font-size: 12px; }}
.tablewrap {{ overflow: auto; background: white; border: 1px solid #e5e7eb; border-radius: 8px; margin-bottom: 16px; }}
table {{ width: 100%; border-collapse: collapse; min-width: 1100px; }}
th, td {{ text-align: left; padding: 10px; border-bottom: 1px solid #f1f5f9; font-size: 13px; }}
th {{ background: #f8fafc; position: sticky; top: 0; }}
small {{ color: #6b7280; }}
.heat {{ text-align: center; color: #0b1020; font-weight: 600; }}
</style>
</head>
<body>
<h2>PR Analysis Viewer (Read-only)</h2>
<small>Database: {html.escape(db_path)}</small>

<div class="cardwrap">
  <div class="card"><div class="label">Total PRs</div><div class="value">{overview['total_mrs']}</div></div>
  <div class="card"><div class="label">Infra-related</div><div class="value">{overview['infra_related']}</div></div>
  <div class="card"><div class="label">High/Very High</div><div class="value">{overview['high_complexity']}</div></div>
  <div class="card"><div class="label">Avg complexity</div><div class="value">{overview['avg_complexity']:.2f}</div></div>
</div>

<form method="GET" action="/">
  <div><label>Project</label><br /><select name="project_id">{''.join(project_options)}</select></div>
  <div><label>Final type</label><br /><select name="final_type">{''.join(type_options)}</select></div>
  <div><label>Complexity</label><br /><select name="complexity_level">{''.join(complexity_options)}</select></div>
  <div><label>Data source</label><br /><select name="data_source">{''.join(data_source_options)}</select></div>
  <div><label>Sort</label><br /><select name="sort">{''.join(sort_options)}</select></div>
  <div><label>Rows</label><br /><input type="number" name="limit" value="{limit}" min="1" max="1000" /></div>
  <div><button type="submit">Apply</button></div>
</form>

<div class="badges">{type_badges}</div>

<h3>Type x Complexity Heatmap</h3>
{heatmap_html}

<h3>MR Details</h3>
<div class="tablewrap">
<table>
  <thead>
    <tr>
      <th>Project</th><th>MR IID</th><th>Title</th><th>Base</th><th>Final</th>
      <th>Complexity</th><th>Score</th><th>Files</th><th>Churn</th><th>Commits</th>
      <th>Comments</th><th>Unresolved</th><th>CI Failed</th><th>Updated</th>
    </tr>
  </thead>
  <tbody>{rows_html}</tbody>
</table>
</div>

<h3>Enrichment Artifacts</h3>
<div class="tablewrap">
<table>
  <thead>
    <tr>
      <th>Project</th><th>Source MRs</th><th>Updated</th><th>Compact</th><th>Mermaid</th>
    </tr>
  </thead>
  <tbody>{compaction_html}</tbody>
</table>
</div>

<h3>Enriched MR Describe Output</h3>
<div class="tablewrap">
<table>
  <thead>
    <tr>
      <th>Project</th><th>MR IID</th><th>Type</th><th>Qodo Title</th><th>Preview</th><th>Updated</th><th>Describe</th>
    </tr>
  </thead>
  <tbody>{enrich_html}</tbody>
</table>
</div>
</body>
</html>
"""


def _resolve_artifact_path(raw_path: str) -> Path | None:
    if not raw_path:
        return None
    candidate = Path(raw_path).expanduser().resolve()
    root = Path.cwd().resolve()
    try:
        candidate.relative_to(root)
    except ValueError:
        return None
    if not candidate.is_file():
        return None
    return candidate


def run_viewer(db_path: str, host: str = "127.0.0.1", port: int = 8765) -> None:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            if parsed.path == "/artifact":
                params = parse_qs(parsed.query)
                path_raw = params.get("path", [""])[0]
                resolved = _resolve_artifact_path(path_raw)
                if not resolved:
                    self.send_response(404)
                    self.end_headers()
                    self.wfile.write(b"Artifact not found")
                    return

                body = resolved.read_text(encoding="utf-8", errors="replace")
                body_bytes = body.encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "text/plain; charset=utf-8")
                self.send_header("Content-Length", str(len(body_bytes)))
                self.end_headers()
                self.wfile.write(body_bytes)
                return

            if parsed.path != "/":
                self.send_response(404)
                self.end_headers()
                self.wfile.write(b"Not Found")
                return

            params = parse_qs(parsed.query)
            project_id_raw = params.get("project_id", [""])[0].strip()
            final_type_raw = params.get("final_type", [""])[0].strip()
            complexity_level_raw = params.get("complexity_level", [""])[0].strip()
            data_source_raw = params.get("data_source", ["production"])[0].strip()
            limit_raw = params.get("limit", ["200"])[0].strip()
            sort_raw = params.get("sort", ["complexity_desc"])[0].strip()

            project_id = int(project_id_raw) if project_id_raw else None
            final_type = final_type_raw or None
            complexity_level = complexity_level_raw or None
            data_source = data_source_raw if data_source_raw in DATA_SOURCES else "production"

            try:
                limit = int(limit_raw)
            except ValueError:
                limit = 200
            limit = max(1, min(1000, limit))
            sort_by = sort_raw if sort_raw in SORT_SQL else "complexity_desc"

            body = _html_page(
                db_path=db_path,
                project_id=project_id,
                final_type=final_type,
                complexity_level=complexity_level,
                data_source=data_source,
                limit=limit,
                sort_by=sort_by,
            )
            body_bytes = body.encode("utf-8")

            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body_bytes)))
            self.end_headers()
            self.wfile.write(body_bytes)

        def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
            return

    server = ThreadingHTTPServer((host, port), Handler)
    print(f"Viewer running on http://{host}:{port}")
    print("Press Ctrl+C to stop.")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
