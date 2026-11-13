from datetime import date, datetime
from decimal import Decimal

import pyarrow as pa
import pytest
from ref_helpers import crm, erp, make_config, run, web

from engine.core.canonical import (
    REQUIRED_TABLES,
    TABLES,
    arrow_type,
    canonical_run_id,
    canonical_schemas,
    canonical_text,
    check_tables,
    empty_tables,
    ordered_pair,
    read_parquet_dir,
    row_hash,
    row_hash_input,
    write_parquet,
)
from engine.core.reference import ReferenceEngine


def test_attribute_types_map_to_arrow_types():
    assert arrow_type("string") == pa.string()
    assert arrow_type("int") == pa.int64()
    assert arrow_type("decimal") == pa.decimal128(18, 4)
    assert arrow_type("date") == pa.date32()
    assert arrow_type("timestamp") == pa.timestamp("us")
    assert arrow_type("bool") == pa.bool_()


def test_schemas_cover_every_table_and_follow_the_config():
    config = make_config()
    schemas = canonical_schemas(config)
    assert set(schemas) == set(TABLES)
    assert set(REQUIRED_TABLES) < set(TABLES)
    golden = schemas["golden"]
    assert golden.names[:3] == ["golden_id", "status", "stale"]
    assert golden.field("birth_date").type == pa.date32()
    assert golden.field("credit_limit").type == pa.decimal128(18, 4)
    assert golden.field("email__src").type == pa.string()
    assert schemas["source_record"].field("revision").type == pa.timestamp("us")
    assert schemas["source_record"].field("last_name").nullable
    assert schemas["match_pair"].names[:4] == [
        "left_source_id",
        "left_key",
        "right_source_id",
        "right_key",
    ]
    assert not schemas["reject"].field("source_key").nullable


def test_provenance_columns_are_optional():
    names = canonical_schemas(make_config(publication__provenance=False))["golden"].names
    assert not [n for n in names if n.endswith("__src")]


def test_ordered_pair_is_canonical():
    assert ordered_pair(("erp", "A"), ("crm", "9")) == (("crm", "9"), ("erp", "A"))
    assert ordered_pair(("crm", "10"), ("crm", "2")) == (("crm", "10"), ("crm", "2"))
    with pytest.raises(ValueError):
        ordered_pair(("crm", "1"), ("crm", "1"))


@pytest.mark.parametrize("good", ["r1", "2024-05-01T10:00:00", "run_1.a:b-c", "x" * 128])
def test_run_id_accepts_opaque_tokens(good):
    assert canonical_run_id(good) == good


@pytest.mark.parametrize("bad", ["", " r1", "r 1", "x" * 129, "é", None, 3])
def test_run_id_rejects_the_rest(bad):
    with pytest.raises(ValueError):
        canonical_run_id(bad)


@pytest.mark.parametrize(
    ("value", "attr_type", "text"),
    [
        ("héllo", "string", "héllo"),
        (42, "int", "42"),
        (Decimal("1.5"), "decimal", "1.5000"),
        (Decimal("-2"), "decimal", "-2.0000"),
        (date(2020, 2, 3), "date", "2020-02-03"),
        (datetime(2020, 2, 3, 4, 5, 6, 7), "timestamp", "2020-02-03T04:05:06.000007"),
        (True, "bool", "true"),
        (False, "bool", "false"),
        (None, "string", None),
    ],
)
def test_canonical_text(value, attr_type, text):
    assert canonical_text(value, attr_type) == text


def test_row_hash_is_defined_on_the_mapped_attributes_of_the_source_only():
    config = make_config()
    values = {
        "last_name": "dupont",
        "first_name": None,
        "birth_date": date(1980, 5, 1),
        "email": "a@b.fr",
        "country": "FR",
        "credit_limit": Decimal("1"),
    }
    text = row_hash_input(config, "web", values)
    # web maps last_name, first_name, email only
    assert text == "last_name\x1f\x01dupont\x1efirst_name\x1f\x00\x1eemail\x1f\x01a@b.fr"
    assert row_hash(config, "web", values) == row_hash(
        config, "web", {**values, "country": "BE", "birth_date": None}
    )
    assert row_hash(config, "web", values) != row_hash(config, "crm", values)
    assert len(row_hash(config, "crm", values)) == 64


def test_engine_export_conforms_and_parquet_roundtrips(tmp_path):
    engine = ReferenceEngine(make_config())
    run(
        engine,
        "r1",
        crm=[crm(1, "dupont", em="e@x.fr")],
        erp=[erp("A", "dupont", plafond="3")],
        web=[web("w", "dupont", mail="e@x.fr")],
    )
    run(engine, "r2", crm=[crm(1, "dupont", em="e@x.fr", fn="paul")])
    tables = engine.export()
    assert check_tables(engine.config, tables) == []
    written = write_parquet(tables, tmp_path / "out")
    assert sorted(p.name for p in written) == sorted(f"{t}.parquet" for t in TABLES)
    back = read_parquet_dir(tmp_path / "out")
    assert set(back) == set(TABLES)
    for name in TABLES:
        assert back[name].to_pylist() == tables[name].to_pylist()
    assert check_tables(engine.config, back) == []


def test_check_tables_reports_problems():
    config = make_config()
    tables = empty_tables(config)
    assert check_tables(config, tables) == []
    del tables["review"]
    tables["reject"] = tables["reject"].drop_columns(["rule"])
    tables["golden_delta"] = tables["golden_delta"].set_column(
        0, "run_id", pa.array([], pa.int64())
    )
    problems = check_tables(config, tables)
    assert "review: table missing" in problems
    assert "reject.rule: column missing" in problems
    assert any(p.startswith("golden_delta.run_id: type") for p in problems)
