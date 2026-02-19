from __future__ import annotations

import os

from prtool.config import load_dotenv


def test_load_dotenv_sets_missing_vars(tmp_path, monkeypatch) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("FOO=bar\n#comment\nBAZ='qux'\n", encoding="utf-8")

    monkeypatch.delenv("FOO", raising=False)
    monkeypatch.delenv("BAZ", raising=False)

    load_dotenv(str(env_file))
    assert os.environ["FOO"] == "bar"
    assert os.environ["BAZ"] == "qux"


def test_load_dotenv_does_not_override_existing(monkeypatch, tmp_path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("FOO=from_file\n", encoding="utf-8")

    monkeypatch.setenv("FOO", "existing")
    load_dotenv(str(env_file))

    assert os.environ["FOO"] == "existing"
