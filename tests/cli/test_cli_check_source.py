from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import yaml


def attr(report: dict, name: str) -> dict:
    return next(a for a in report["attributes"] if a["attribute"] == name)


def messages(report: dict, level: str) -> list[str]:
    return [f["message"] for f in report["findings"] if f["level"] == level]


def test_profile_crm_counts(cli_json, config_path):
    code, report = cli_json("check-source", config_path, "--source", "crm")
    assert code == 0 and report["verdict"] == "WARN"
    assert report["rows"] == 100
    assert report["key"]["null_count"] == 0
    assert report["key"]["distinct"] == 98 and report["key"]["duplicate_rows"] == 2
    assert report["revision"]["column"] == "updated_at" and report["revision"]["null_count"] == 0
    assert report["revision"]["min"].startswith("2024-01-01")

    last = attr(report, "last_name")
    assert last["null_count"] == 10 and last["null_rate"] == pytest.approx(0.1)
    assert (last["distinct_before"], last["distinct_after"]) == (3, 2)
    assert last["rank"] == 1 and last["of"] == 2 and last["column"] == "LASTNAME"
    [max_len] = last["checks"]
    assert (max_len["rule"], max_len["failing_rows"], max_len["on_fail"]) == (
        "max_length",
        5,
        "reject_record",
    )
    assert report["rows_rejected_by_checks"] == 5

    email = attr(report, "email")
    [regex] = email["checks"]
    assert regex["failing_rows"] == 6 and regex["on_fail"] == "null_value"

    birth = attr(report, "birth_date")
    assert birth["cast_failures"] == 4 and birth["null_count"] == 8
    assert birth["native_type"] == "VARCHAR"


def test_findings_explain_the_consequences(cli_json, config_path):
    _, report = cli_json("check-source", config_path, "--source", "crm")
    warns = " | ".join(messages(report, "WARN"))
    assert "last_name: check max_length fails on 5 rows (5.0%) → reject_record" in warns
    assert "email: check regex fails on 6 rows" in warns and "null_value" in warns
    assert "birth_date (BIRTHDATE): 4 of 92 non-null values (4.3%) cannot be read as date" in warns
    assert "key 'customer_id': 2 rows share their key" in warns
    assert any("empty strings" in m for m in messages(report, "INFO")) is False


def test_clean_source_is_ok(cli_json, config_path):
    code, report = cli_json("check-source", config_path, "--source", "erp")
    assert code == 0 and report["verdict"] == "OK", report["findings"]
    assert report["key"]["unique"] is True
    assert attr(report, "birth_date")["native_type"] == "DATE"
    assert attr(report, "birth_date")["cast_failures"] == 0


def test_text_report(cli, config_path):
    code, out = cli("check-source", config_path, "--source", "crm")
    assert code == 0
    assert "Verdict : WARN" in out and "Rows    : 100" in out
    assert "last_name" in out and "LASTNAME (1/2)" in out and "3→2" in out
    assert "Findings:" in out and "distinct (estimated)" in out


def test_sample_limits_rows(cli_json, config_path):
    code, report = cli_json("check-source", config_path, "--source", "crm", "--sample", "20")
    assert report["rows"] == 20 and report["sample"] == 20
    assert any("first 20 rows" in m for m in messages(report, "INFO"))


def rewrite_config(path: Path, mutate) -> None:
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    mutate(raw)
    path.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")


def test_typos_in_column_names_get_suggestions(cli_json, config_path):
    def mutate(r):
        r["attributes"][0]["sources"][0]["column"] = "LASTNAM"
        r["attributes"][1]["sources"][0]["column"] = "email"  # casse différente
        r["sources"][0]["key"] = "customer_idx"

    rewrite_config(config_path, mutate)
    code, report = cli_json("check-source", config_path, "--source", "crm")
    assert code == 1 and report["verdict"] == "BLOCKING"
    blocking = messages(report, "BLOCKING")
    assert any("column 'LASTNAM' not found. Did you mean 'LASTNAME'?" in m for m in blocking)
    assert any("column 'email' not found. Did you mean 'EMAIL'?" in m for m in blocking)
    assert any(
        "key: column 'customer_idx' not found. Did you mean 'customer_id'?" in m for m in blocking
    )
    assert report["rows"] == 100  # les colonnes valides sont quand même profilées


def test_incompatible_type_is_blocking(cli_json, config_path):
    rewrite_config(
        config_path, lambda r: r["attributes"][2].update(type="int")
    )  # BIRTHDATE texte → int : lisible ; erp.naissance est un DATE : illisible en int
    code, report = cli_json("check-source", config_path, "--source", "erp")
    assert code == 1
    assert any("is DATE, which cannot be read as int" in m for m in messages(report, "BLOCKING"))


def test_mostly_unreadable_column_is_blocking(cli_json, config_path):
    code, _ = cli_json("check-source", config_path, "--source", "crm")
    rewrite_config(config_path, lambda r: r["attributes"][2].update(type="int"))
    code, report = cli_json("check-source", config_path, "--source", "crm")
    assert code == 1
    assert any("cannot be read as int" in m for m in messages(report, "BLOCKING"))


def test_missing_file_and_unknown_source(cli, cli_json, config_path, tmp_path):
    code, out = cli("check-source", config_path, "--source", "crn")
    assert code == 1 and "unknown source 'crn' (known: crm, erp). Did you mean 'crm'?" in out
    rewrite_config(
        config_path, lambda r: r["sources"][0].update(location=str(tmp_path / "no.parquet"))
    )
    code, report = cli_json("check-source", config_path, "--source", "crm")
    assert code == 1 and "cannot read source" in messages(report, "BLOCKING")[0]


def test_duplicate_full_snapshot_keys_can_be_blocking(cli_json, config_path, data_dir):
    pq.write_table(
        pa.table({"customer_id": [1] * 10, "updated_at": [None] * 10, "LASTNAME": ["a"] * 10,
                  "EMAIL": ["a@b.c"] * 10, "BIRTHDATE": ["1990-01-01"] * 10}),
        data_dir / "crm.parquet",
    )  # fmt: skip
    code, report = cli_json("check-source", config_path, "--source", "crm")
    assert code == 1
    assert any("does not look like a key" in m for m in messages(report, "BLOCKING"))


def test_null_keys_and_revisions_are_reported(cli_json, config_path, data_dir):
    pq.write_table(
        pa.table({
            "customer_id": [1, None, 3, 4], "updated_at": [None, None, None, "2024-01-01"],
            "LASTNAME": ["a", "b", "c", "d"], "EMAIL": ["a@b.c"] * 4,
            "BIRTHDATE": pa.array([None] * 4, pa.string()),
        }),
        data_dir / "crm.parquet",
    )  # fmt: skip
    code, report = cli_json("check-source", config_path, "--source", "crm")
    assert report["key"]["null_count"] == 1
    assert any("1 rows (25.0%) have a null key" in m for m in messages(report, "WARN"))
    assert any("revision 'updated_at' is text" in m for m in messages(report, "WARN"))
    assert any(
        "entirely null: this source contributes nothing" in m for m in messages(report, "WARN")
    )


def test_regex_unsupported_by_engine_is_skipped_not_fatal(cli_json, config_path):
    rewrite_config(
        config_path,
        lambda r: r["attributes"][1]["checks"][0].update(
            value="(?=x)a"
        ),  # lookahead : refusé par RE2
    )
    code, report = cli_json("check-source", config_path, "--source", "crm")
    assert code == 0
    assert any("not supported by DuckDB" in m for m in messages(report, "WARN"))
    assert attr(report, "email")["checks"] == []


def test_blocking_exit_code_for_invalid_config(cli, tmp_path):
    path = tmp_path / "bad.yml"
    path.write_text("nope: 1\n", encoding="utf-8")
    assert cli("check-source", path, "--source", "x")[0] == 1


def test_postgres_without_env_var_is_blocking(cli_json, config_path, monkeypatch):
    monkeypatch.delenv("PG_DSN_MISSING", raising=False)

    def mutate(r):
        r["sources"][0].update(kind="postgres", location="public.crm", dsn_env="PG_DSN_MISSING")

    rewrite_config(config_path, mutate)
    code, report = cli_json("check-source", config_path, "--source", "crm")
    assert code == 1
    assert "environment variable PG_DSN_MISSING is not set" in messages(report, "BLOCKING")[0]


def test_wide_source_is_profiled_in_a_single_query(tmp_path, data_dir):
    from engine.cli import checksource
    from engine.cli.sqlbackends import DuckBackend
    from engine.core.config import ReferentialConfig

    n_attrs = 200
    cols = {"id": list(range(500)), "ts": [None] * 500}
    attrs = []
    for k in range(n_attrs):
        cols[f"c{k}"] = [f" V{i % 7} " for i in range(500)]
        attrs.append({
            "name": f"a{k}", "normalize": ["trim", "lower"],
            "sources": [{"source": "s", "column": f"c{k}"}],
            "checks": [{"rule": "max_length", "value": 3, "on_fail": "warn"}],
        })  # fmt: skip
    path = tmp_path / "wide.parquet"
    pq.write_table(pa.table(cols), path)
    cfg = ReferentialConfig.model_validate({
        "referential": "w", "version": 1, "state": {"dsn_env": "X", "schema": "w"},
        "sources": [{"id": "s", "kind": "parquet", "location": str(path), "key": "id"}],
        "attributes": attrs,
        "matching": {"version": "1", "blocking": [{"name": "b", "parts": [{"attr": "a0"}]}],
                     "comparators": [{"attr": "a0", "method": "exact", "weight": 1}]},
    })  # fmt: skip
    calls = []
    backend = DuckBackend(str(path))
    original = backend.row
    backend.row = lambda sql: calls.append(sql) or original(sql)
    report = checksource.check_source(cfg, "s", backend=backend)
    assert report["rows"] == 500 and len(report["attributes"]) == n_attrs
    assert len(calls) == 1  # une seule passe SQL
    assert report["attributes"][0]["distinct_before"] == 7
