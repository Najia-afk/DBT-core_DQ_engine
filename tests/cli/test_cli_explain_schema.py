from __future__ import annotations

import json

import pytest
import yaml

from engine.cli.schema import build_schema
from engine.core.config import ReferentialConfig


def test_explain_french_describes_survival_nullable_and_checks(cli, config_path):
    code, out = cli("explain", config_path)
    assert code == 0
    assert "■ last_name : texte, obligatoire (nullable: false)" in out
    assert "1. crm.LASTNAME" in out and "2. erp.nom" in out
    assert "trim (supprime les espaces en début et fin) → lower" in out
    assert "le golden est BLOQUÉ" in out  # last_name non nullable
    assert "l'attribut est publié nul" in out  # email nullable
    assert "reject_record : le nœud source est rejeté : aucun delta, aucune réconciliation" in out
    assert "null_value : la valeur est traitée comme nulle" in out
    assert "longueur ≤ 10" in out
    assert "clé de blocage ln" in out and "comparateur levenshtein" in out
    assert "prefix(last_name, 4)" in out
    assert "Seuils : score ≥ 0.95" in out
    assert "Contradictions : birth_date" in out


def test_explain_english_and_single_attribute(cli, config_path):
    code, out = cli("explain", config_path, "--attribute", "email", "--lang", "en")
    assert code == 0
    assert "■ email : text, optional" in out
    assert "the source record is rejected" not in out  # email check is null_value
    assert "the value is treated as null" in out
    assert "last_name" not in out and "Matching (rules" not in out


def test_explain_most_recent_says_order_is_irrelevant(cli, config_path):
    text = config_path.read_text(encoding="utf-8").replace(
        "  - name: email\n", "  - name: email\n    strategy: most_recent\n"
    )
    config_path.write_text(text, encoding="utf-8")
    code, out = cli("explain", config_path, "--attribute", "email")
    assert code == 0 and "l'ordre n'a pas d'effet" in out


def test_explain_unknown_attribute(cli, config_path):
    code, out = cli("explain", config_path, "--attribute", "emial")
    assert code == 1 and "Did you mean 'email'?" in out


def test_explain_invalid_config_fails(cli, tmp_path):
    path = tmp_path / "x.yml"
    path.write_text("referential: x\n", encoding="utf-8")
    code, out = cli("explain", path)
    assert code == 1 and "ERROR" in out


def test_explain_json(cli_json, config_path):
    code, data = cli_json("explain", config_path)
    assert code == 0
    last = data["attributes"][0]
    assert last["if_all_null"] == "golden_blocked"
    assert [s["source"] for s in last["sources"]] == ["crm", "erp"]
    assert "blocking:ln" in last["matching_roles"]


def test_schema_stdout_is_json_schema(cli):
    code, out = cli("schema")
    assert code == 0
    schema = json.loads(out)
    assert schema["$schema"].endswith("2020-12/schema")
    assert set(schema["properties"]) >= {"referential", "sources", "attributes", "matching"}
    assert schema["properties"]["sources"]["description"]
    assert schema["$defs"]["Attribute"]["additionalProperties"] is False


def test_schema_out_file(cli, tmp_path):
    target = tmp_path / "mdm.schema.json"
    code, _ = cli("schema", "--out", target)
    assert code == 0 and json.loads(target.read_text(encoding="utf-8")) == build_schema()


def test_schema_accepts_the_example_and_rejects_a_typo(config_path):
    jsonschema = pytest.importorskip("jsonschema")
    schema = build_schema()
    raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    jsonschema.validate(raw, schema)
    ReferentialConfig.model_validate(raw)
    raw["attributes"][0]["nullabel"] = True
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(raw, schema)
