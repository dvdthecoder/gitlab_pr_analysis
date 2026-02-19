from __future__ import annotations

import json
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Iterable

from prtool.config import Settings

try:
    import gitlab  # type: ignore
except Exception:  # pragma: no cover
    gitlab = None


class GitLabSourceClient:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._gl = None
        if gitlab is not None:
            self._gl = gitlab.Gitlab(
                url=self.settings.gitlab_base_url,
                private_token=self.settings.gitlab_token,
                per_page=self.settings.page_size,
            )

    def _request(self, path: str, params: dict[str, Any] | None = None, return_headers: bool = False) -> Any:
        base = f"{self.settings.gitlab_base_url}/api/v4{path}"
        if params:
            query = urllib.parse.urlencode(params)
            url = f"{base}?{query}"
        else:
            url = base

        backoff = self.settings.backoff_ms / 1000
        for attempt in range(self.settings.max_retries + 1):
            req = urllib.request.Request(
                url,
                headers={
                    "PRIVATE-TOKEN": self.settings.gitlab_token,
                    "User-Agent": "prtool/0.1.0",
                },
            )
            try:
                with urllib.request.urlopen(req, timeout=self.settings.request_timeout) as resp:
                    payload = json.loads(resp.read().decode("utf-8"))
                    if return_headers:
                        return payload, dict(resp.headers.items())
                    return payload
            except urllib.error.HTTPError as exc:
                status = exc.code
                if status in (429, 500, 502, 503, 504) and attempt < self.settings.max_retries:
                    time.sleep(backoff)
                    backoff *= 2
                    continue
                raise

        raise RuntimeError("request retry loop ended unexpectedly")

    def _paginated(self, path: str, params: dict[str, Any]) -> Iterable[dict[str, Any]]:
        page = 1
        while True:
            payload = self._request(
                path,
                {
                    **params,
                    "per_page": self.settings.page_size,
                    "page": page,
                },
            )
            if not payload:
                break
            for item in payload:
                yield item
            if len(payload) < self.settings.page_size:
                break
            page += 1

    def list_accessible_project_ids(self) -> list[int]:
        return [int(p["id"]) for p in self.list_accessible_projects()]

    def list_group_project_ids(self, group_ref: str) -> list[int]:
        return [int(p["id"]) for p in self.list_group_projects(group_ref)]

    def list_accessible_projects(self) -> list[dict[str, Any]]:
        if self._gl is not None:
            projects = self._gl.projects.list(archived=False, simple=True, all=True)
            payload = []
            for p in projects:
                payload.append(
                    {
                        "id": int(p.attributes["id"]),
                        "path_with_namespace": p.attributes.get("path_with_namespace") or "",
                        "name": p.attributes.get("name") or "",
                    }
                )
            return sorted(payload, key=lambda x: x["id"])

        params: dict[str, Any] = {
            "archived": False,
            "simple": True,
            "order_by": "id",
            "sort": "asc",
        }
        payload = []
        for p in self._paginated("/projects", params):
            payload.append(
                {
                    "id": int(p["id"]),
                    "path_with_namespace": p.get("path_with_namespace") or "",
                    "name": p.get("name") or "",
                }
            )
        return sorted(payload, key=lambda x: x["id"])

    def list_group_projects(self, group_ref: str) -> list[dict[str, Any]]:
        if self._gl is not None:
            group = self._gl.groups.get(group_ref)
            projects = group.projects.list(include_subgroups=True, archived=False, all=True)
            payload = []
            for p in projects:
                payload.append(
                    {
                        "id": int(p.attributes["id"]),
                        "path_with_namespace": p.attributes.get("path_with_namespace") or "",
                        "name": p.attributes.get("name") or "",
                    }
                )
            return sorted(payload, key=lambda x: x["id"])

        params: dict[str, Any] = {
            "include_subgroups": True,
            "archived": False,
            "simple": True,
            "order_by": "id",
            "sort": "asc",
        }
        encoded_group = urllib.parse.quote(str(group_ref), safe="")
        payload = []
        for p in self._paginated(f"/groups/{encoded_group}/projects", params):
            payload.append(
                {
                    "id": int(p["id"]),
                    "path_with_namespace": p.get("path_with_namespace") or "",
                    "name": p.get("name") or "",
                }
            )
        return sorted(payload, key=lambda x: x["id"])

    def get_project_mr_count_all_states(self, project_id: int) -> int:
        # Use GitLab pagination headers for fast total count retrieval.
        _, headers = self._request(
            f"/projects/{project_id}/merge_requests",
            {
                "state": "all",
                "per_page": 1,
                "page": 1,
            },
            return_headers=True,
        )
        total = headers.get("X-Total") or headers.get("x-total")
        if total is None:
            return 0
        try:
            return int(total)
        except ValueError:
            return 0

    def list_merge_requests(
        self,
        project_id: int,
        updated_after: str | None = None,
        created_after: str | None = None,
    ) -> list[dict[str, Any]]:
        if self._gl is not None:
            project = self._gl.projects.get(project_id)
            kwargs: dict[str, Any] = {
                "scope": "all",
                "order_by": "updated_at",
                "sort": "asc",
                "all": True,
            }
            if updated_after:
                kwargs["updated_after"] = updated_after
            if created_after:
                kwargs["created_after"] = created_after
            return [mr.attributes for mr in project.mergerequests.list(**kwargs)]

        params: dict[str, Any] = {
            "scope": "all",
            "order_by": "updated_at",
            "sort": "asc",
            "with_labels_details": False,
        }
        if updated_after:
            params["updated_after"] = updated_after
        if created_after:
            params["created_after"] = created_after
        return list(self._paginated(f"/projects/{project_id}/merge_requests", params))

    def get_mr_commits(self, project_id: int, mr_iid: int) -> list[dict[str, Any]]:
        if self._gl is not None:
            project = self._gl.projects.get(project_id)
            mr = project.mergerequests.get(mr_iid)
            return [c.attributes for c in mr.commits()]
        return list(self._paginated(f"/projects/{project_id}/merge_requests/{mr_iid}/commits", {}))

    def get_mr_changes(self, project_id: int, mr_iid: int) -> list[dict[str, Any]]:
        payload = self._request(f"/projects/{project_id}/merge_requests/{mr_iid}/changes")
        return payload.get("changes", [])

    def get_mr_discussions(self, project_id: int, mr_iid: int) -> list[dict[str, Any]]:
        if self._gl is not None:
            project = self._gl.projects.get(project_id)
            mr = project.mergerequests.get(mr_iid)
            return [d.attributes for d in mr.discussions.list(all=True)]
        return list(self._paginated(f"/projects/{project_id}/merge_requests/{mr_iid}/discussions", {}))

    def get_mr_approvals(self, project_id: int, mr_iid: int) -> dict[str, Any]:
        return self._request(f"/projects/{project_id}/merge_requests/{mr_iid}/approvals")

    def get_mr_pipelines(self, project_id: int, mr_iid: int) -> list[dict[str, Any]]:
        if self._gl is not None:
            project = self._gl.projects.get(project_id)
            mr = project.mergerequests.get(mr_iid)
            return [p.attributes for p in mr.pipelines.list(all=True)]
        return list(self._paginated(f"/projects/{project_id}/merge_requests/{mr_iid}/pipelines", {}))
