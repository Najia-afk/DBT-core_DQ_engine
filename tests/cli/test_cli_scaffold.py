from __future__ import annotations

import json

from engine.core.config import load_config


def test_new_referential_writes_valid_config_and_schema(cli, tmp_path):
    out_dir = tmp_path / "out"
    code, out = cli("new-referential", "supplier", "--out", out_dir)
    assert code == 0 and "supplier.yml" in out
    config = out_dir / "supplier.yml"
    text = config.read_text(encoding="utf-8")
    assert text.startswith("# yaml-language-server: $schema=./mdm.schema.json")
    assert "# L'ORDRE de `sources` dans un attribut" in text
    cfg = load_config(config)
    assert cfg.referential == "supplier" and cfg.state.schema_name == "mdm_supplier"
    assert json.loads((out_dir / "mdm.schema.json").read_text(encoding="utf-8"))["properties"]
    assert cli("validate", config)[0] == 0
    assert cli("explain", config)[0] == 0


def test_new_referential_refuses_overwrite_and_bad_names(cli, tmp_path):
    assert cli("new-referential", "supplier", "--out", tmp_path)[0] == 0
    code, out = cli("new-referential", "supplier", "--out", tmp_path)
    assert code == 1 and "already exists" in out
    assert cli("new-referential", "supplier", "--out", tmp_path, "--force")[0] == 0
    code, out = cli("new-referential", "Bad Name", "--out", tmp_path)
    assert code == 1 and "invalid name" in out


def test_scaffold_then_add_source_round_trip(cli, tmp_path):
    cli("new-referential", "supplier", "--out", tmp_path)
    config = tmp_path / "supplier.yml"
    code, out = cli(
        "add-source", config, "--source-id", "erp", "--kind", "postgres", "--location",
        "public.t", "--dsn-env", "ERP_DSN", "--key", "id", "--map", "last_name=nom@1", "email=mail",
    )  # fmt: skip
    assert code == 0, out
    text = config.read_text(encoding="utf-8")
    assert "# L'ORDRE de `sources` dans un attribut" in text and "# - {id: crm" in text
    assert load_config(config).mapping("erp") == {"last_name": "nom", "email": "mail"}


def test_cli_help_and_usage_errors_exit_one(cli, capsys):
    import pytest

    with pytest.raises(SystemExit) as stop:
        cli("validate")
    assert stop.value.code == 1
    with pytest.raises(SystemExit) as stop:
        cli("--help")
    assert stop.value.code == 0
