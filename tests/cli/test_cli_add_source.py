from __future__ import annotations

from pathlib import Path

import pytest

from engine.core.config import load_config

ADD = (
    "--source-id", "web", "--kind", "parquet", "--location", "{loc}", "--key", "account_id",
    "--revision", "event_ts",
)  # fmt: skip


@pytest.fixture
def add_args(data_dir):
    loc = (data_dir / "web.parquet").as_posix()
    return [a.format(loc=loc) for a in ADD]


def test_dry_run_prints_diff_and_does_not_write(cli, config_path, add_args):
    before = config_path.read_bytes()
    code, out = cli(
        "add-source", config_path, *add_args, "--map", "last_name=surname@1", "email=contact",
        "--dry-run",
    )  # fmt: skip
    assert code == 0
    assert config_path.read_bytes() == before
    assert "+  - id: web" in out
    assert "dry-run: nothing written" in out


def test_write_preserves_everything_else_and_applies_positions(cli, config_path, add_args):
    before = config_path.read_text(encoding="utf-8")
    code, out = cli(
        "add-source", config_path, *add_args, "--map", "last_name=surname@1", "email=contact@2",
        "birth_date=dob",
    )  # fmt: skip
    assert code == 0 and f"written: {config_path}" in out
    after = config_path.read_text(encoding="utf-8")
    after_lines = after.splitlines()
    start = after_lines.index("  - id: web")
    del after_lines[start : start + 5]  # le bloc de la nouvelle source

    def keep(lines: list[str]) -> list[str]:
        return [x for x in lines if "{source:" not in x]

    assert keep(before.splitlines()) == keep(after_lines)  # le reste est identique à la ligne près
    assert "# l'ERP passe en dernier" in after and "# style bloc" in after

    cfg = load_config(config_path)
    assert [s.id for s in cfg.sources] == ["crm", "erp", "web"]
    assert [s.source for s in cfg.attribute("last_name").sources] == ["web", "crm", "erp"]
    assert [s.source for s in cfg.attribute("email").sources] == ["crm", "web", "erp"]
    assert [s.source for s in cfg.attribute("birth_date").sources] == ["crm", "erp", "web"]
    assert cfg.mapping("web") == {"last_name": "surname", "email": "contact", "birth_date": "dob"}
    assert cfg.source("web").revision == "event_ts"


def test_block_style_attribute_sources_keep_comment_attached_to_next_item(
    cli, config_path, add_args
):
    cli("add-source", config_path, *add_args, "--map", "email=contact@2")
    lines = config_path.read_text(encoding="utf-8").splitlines()
    i = lines.index("      - {source: web, column: contact}")
    assert lines[i - 1] == "      - {source: crm, column: EMAIL}"
    assert lines[i + 1] == "      # l'ERP passe en dernier"


def test_first_position_in_block_sequence_goes_above_item(cli, config_path, add_args):
    cli("add-source", config_path, *add_args, "--map", "email=contact@first")
    lines = config_path.read_text(encoding="utf-8").splitlines()
    i = lines.index("    sources:")  # premier bloc `sources:` d'attribut = email
    assert lines[i + 1] == "      - {source: web, column: contact}"
    assert [s.source for s in load_config(config_path).attribute("email").sources][0] == "web"


def test_new_source_uses_the_style_of_its_neighbours(cli, config_path, add_args):
    cli("add-source", config_path, *add_args, "--map", "last_name=surname")
    # dernier élément de `sources:` = style bloc (erp) => la nouvelle source est en bloc
    lines = config_path.read_text(encoding="utf-8").splitlines()
    i = lines.index("  - id: web")
    assert lines[i + 1].startswith("    kind: parquet") and lines[i + 3] == "    key: account_id"


def test_flow_style_example_gets_a_flow_line_and_other_lines_untouched(cli, tmp_path):
    example = Path(__file__).parents[2] / "configs" / "example" / "customer.yml"
    target = tmp_path / "customer.yml"
    target.write_bytes(example.read_bytes())
    code, _ = cli(
        "add-source", target, "--source-id", "shop", "--kind", "parquet", "--location",
        "/data/shop/*.parquet", "--key", "id", "--revision", "ts", "--map", "last_name=lname@1",
        "email=mail",
    )  # fmt: skip
    assert code == 0
    before, after = example.read_text(encoding="utf-8"), target.read_text(encoding="utf-8")
    new_line = (
        "  - {id: shop, kind: parquet, location: /data/shop/*.parquet, key: id, revision: ts}"
    )
    assert new_line in after

    def untouched(text: str) -> list[str]:
        return [x for x in text.splitlines() if "{source:" not in x and "id: shop" not in x]

    assert untouched(before) == untouched(after)  # seules les lignes de mapping changent
    cfg = load_config(target)
    assert [s.source for s in cfg.attribute("last_name").sources][0] == "shop"
    assert [s.source for s in cfg.attribute("email").sources][-1] == "shop"


def test_invalid_result_is_never_written(cli, config_path, add_args):
    before = config_path.read_bytes()
    code, out = cli("add-source", config_path, *add_args, "--map", "nope=x")
    assert code == 1 and "unknown attribute 'nope'" in out
    assert config_path.read_bytes() == before


@pytest.mark.parametrize(
    ("extra", "message"),
    [
        (["--map", "email=c@9"], "position must be between 1 and 3"),
        (["--map", "email=c@0"], "invalid position"),
        (["--map", "email"], "expected attr=column"),
        (["--map", "email=a", "email=b"], "mapped twice"),
        ([], "at least one --map"),
        (["--map", "emial=c"], "Did you mean 'email'?"),
    ],
)
def test_bad_requests_are_rejected_without_writing(cli, config_path, add_args, extra, message):
    before = config_path.read_bytes()
    code, out = cli("add-source", config_path, *add_args, *extra)
    assert code == 1 and message in out
    assert config_path.read_bytes() == before


def test_duplicate_source_id(cli, config_path, add_args):
    args = ["--source-id", "crm", *add_args[2:]]
    code, out = cli("add-source", config_path, *args, "--map", "email=c")
    assert code == 1 and "source 'crm' already exists" in out


def test_postgres_source_needs_dsn_env_and_result_is_validated(cli, config_path):
    before = config_path.read_bytes()
    args = [
        "--source-id", "pg", "--kind", "postgres", "--location", "public.t", "--key", "id",
        "--map", "email=mail",
    ]  # fmt: skip
    code, out = cli("add-source", config_path, *args)
    assert code == 1 and "dsn_env is required for postgres" in out
    assert "NOT WRITTEN" in out and config_path.read_bytes() == before
    code, _ = cli("add-source", config_path, *args, "--dsn-env", "PG_DSN")
    assert code == 0
    assert load_config(config_path).source("pg").dsn_env == "PG_DSN"


def test_most_recent_attribute_requires_revision_on_new_source(cli, config_path):
    text = config_path.read_text(encoding="utf-8").replace(
        "  - name: email\n", "  - name: email\n    strategy: most_recent\n"
    )
    config_path.write_text(text, encoding="utf-8")
    args = ["--source-id", "pg", "--kind", "parquet", "--location", "/x/*.parquet", "--key", "id"]
    code, out = cli("add-source", config_path, *args, "--map", "email=mail")
    assert code == 1 and "most_recent needs a revision column" in out


def test_special_characters_are_quoted(cli, config_path):
    args = [
        "--source-id", "w", "--kind", "parquet", "--location", "/data/a,b [1]/*.parquet",
        "--key", "id: x", "--map", "email=col#1", "last_name=a, b",
    ]  # fmt: skip
    code, out = cli("add-source", config_path, *args)
    assert code == 0, out
    cfg = load_config(config_path)
    assert cfg.source("w").location == "/data/a,b [1]/*.parquet" and cfg.source("w").key == "id: x"
    assert cfg.mapping("w") == {"email": "col#1", "last_name": "a, b"}


def test_bom_and_crlf_are_preserved(cli, config_path, add_args):
    raw = config_path.read_bytes().decode("utf-8").replace("\n", "\r\n")
    config_path.write_bytes(b"\xef\xbb\xbf" + raw.encode("utf-8"))
    code, _ = cli("add-source", config_path, *add_args, "--map", "last_name=surname")
    assert code == 0
    out = config_path.read_bytes()
    assert out.startswith(b"\xef\xbb\xbf") and out.count(b"\n") == out.count(b"\r\n")
    assert [s.id for s in load_config(config_path).sources] == ["crm", "erp", "web"]


def test_json_output_has_diff_and_new_warnings(cli_json, config_path, add_args):
    code, report = cli_json("add-source", config_path, *add_args, "--map", "email=c", "--dry-run")
    assert code == 0 and report["written"] is False
    assert report["diff"].startswith("--- a/customer.yml")
    assert isinstance(report["new_warnings"], list)


def test_added_source_flows_into_check_source(cli_json, config_path, add_args):
    cli_json("add-source", config_path, *add_args, "--map", "last_name=surname", "email=contact")
    code, report = cli_json("check-source", config_path, "--source", "web")
    assert code == 0 and report["verdict"] in {"OK", "WARN"}
    assert report["rows"] == 30
