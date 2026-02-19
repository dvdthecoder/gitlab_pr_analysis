from __future__ import annotations

import pytest

from prtool.cli import _slice_project_ids


def test_slice_project_ids_default_all() -> None:
    assert _slice_project_ids([10, 20, 30]) == [10, 20, 30]


def test_slice_project_ids_window() -> None:
    assert _slice_project_ids([10, 20, 30, 40, 50], start_index=2, count=3) == [20, 30, 40]


def test_slice_project_ids_invalid_start() -> None:
    with pytest.raises(ValueError):
        _slice_project_ids([10, 20], start_index=0)


def test_slice_project_ids_invalid_count() -> None:
    with pytest.raises(ValueError):
        _slice_project_ids([10, 20], count=0)


def test_slice_project_ids_empty_selection() -> None:
    with pytest.raises(ValueError):
        _slice_project_ids([10, 20], start_index=5, count=1)
