from pathlib import Path

import pytest
from pydantic import ValidationError

from engine.core.config import load_config

EXAMPLE = Path(__file__).parents[2] / "configs" / "example" / "customer.yml"


def raw() -> dict:
    import yaml

    return yaml.safe_load(EXAMPLE.read_text(encoding="utf-8"))


def test_example_loads_and_exposes_mapping():
    cfg = load_config(EXAMPLE)
    assert cfg.mapping("crm")["last_name"] == "LASTNAME"
    assert [s.source for s in cfg.attribute("email").sources] == ["web", "crm"]


@pytest.mark.parametrize(
    "mutate",
    [
        lambda r: r["matching"]["comparators"][0].update(attr="nope"),
        lambda r: r["attributes"][0]["sources"].append({"source": "ghost", "column": "x"}),
        lambda r: r["matching"].update(review=0.96),
        lambda r: r["matching"]["comparators"][0].update(weight=0.9),
        lambda r: r["attributes"][1].update(name="golden_id"),
        lambda r: r["attributes"][2].update(checks=[{"rule": "regex", "value": "x"}]),
        lambda r: r["attributes"][0].update(unknown_option=1),
        lambda r: r["sources"][0].pop("dsn_env"),
        lambda r: r["matching"]["comparators"][2].update(method="levenshtein"),
    ],
)
def test_invalid_configs_are_rejected(mutate):
    data = raw()
    mutate(data)
    with pytest.raises(ValidationError):
        from engine.core.config import ReferentialConfig

        ReferentialConfig.model_validate(data)


@pytest.mark.parametrize(
    "name",
    ["revision", "source_id", "source_key", "row_hash", "first_run", "last_change_run", "run_id",
     "node_id", "published_once"],
)
def test_canonical_column_names_are_reserved(name):
    from engine.core.config import ReferentialConfig

    data = raw()
    data["attributes"][4]["name"] = name
    with pytest.raises(ValidationError, match="reserved"):
        ReferentialConfig.model_validate(data)


def test_attribute_provenance_identifier_fits_postgres_limit():
    from engine.core.config import ReferentialConfig

    data = raw()
    data["attributes"][4]["name"] = "a" * 58
    ReferentialConfig.model_validate(data)
    data["attributes"][4]["name"] = "a" * 59
    with pytest.raises(ValidationError, match="58 characters"):
        ReferentialConfig.model_validate(data)
