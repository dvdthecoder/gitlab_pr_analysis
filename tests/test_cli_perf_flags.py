from __future__ import annotations

import argparse

import pytest

from prtool.cli import _resolve_concurrency


def test_resolve_concurrency_from_arg() -> None:
    args = argparse.Namespace(concurrency=7)
    assert _resolve_concurrency(args) == 7


def test_resolve_concurrency_invalid() -> None:
    args = argparse.Namespace(concurrency=0)
    with pytest.raises(ValueError):
        _resolve_concurrency(args)
