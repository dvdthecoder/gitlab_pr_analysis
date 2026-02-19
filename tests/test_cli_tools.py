from __future__ import annotations

import pytest

from prtool.cli import _parse_tools


def test_parse_tools_default() -> None:
    assert _parse_tools("") == ("describe",)


def test_parse_tools_dedup() -> None:
    assert _parse_tools("describe,review,describe") == ("describe", "review")


def test_parse_tools_invalid() -> None:
    with pytest.raises(ValueError):
        _parse_tools("describe,foo")


def test_parse_tools_analyse_alias() -> None:
    assert _parse_tools("describe,analyse,review") == ("describe", "improve", "review")


def test_parse_tools_analyze_alias() -> None:
    assert _parse_tools("describe,analyze") == ("describe", "improve")
