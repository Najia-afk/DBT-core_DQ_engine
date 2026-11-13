from __future__ import annotations

from pathlib import Path

import pytest
import yaml

EXAMPLE = Path(__file__).parents[2] / "configs" / "example" / "customer.yml"


@pytest.fixture
def mutated(config_path, tmp_path):
    def make(mutate) -> Path:
        raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
        mutate(raw)
        path = tmp_path / "mutated.yml"
        path.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")
        return path

    return make


def messages(report: dict, key: str = "errors") -> list[tuple[str, str]]:
    return [(e["path"], e["message"]) for e in report[key]]


def codes(report: dict) -> set[str]:
    return {w["code"] for w in report["warnings"]}


def test_example_is_valid_with_warnings(cli_json):
    code, report = cli_json("validate", EXAMPLE)
    assert code == 0 and report["ok"] and report["errors"] == []
    assert {"nonnull-partial-coverage", "blocking-all-nullable"} <= codes(report)


def test_valid_config_text_output(cli, config_path):
    code, out = cli("validate", config_path)
    assert code == 0
    assert out.strip().splitlines()[-1].startswith("OK: 0 error(s)")


def test_unknown_source_has_yaml_path_known_list_and_suggestion(cli_json, mutated):
    path = mutated(lambda r: r["attributes"][1]["sources"][1].update(source="erpp"))
    code, report = cli_json("validate", path)
    assert code == 1
    [(where, message)] = messages(report)
    assert where == "attributes[1].sources[1].source"
    assert message.startswith("unknown source 'erpp' (known: crm, erp)")
    assert "Did you mean 'erp'?" in message
    assert report["errors"][0]["line"] is not None


def test_all_cross_reference_errors_are_reported_at_once(cli_json, mutated):
    def mutate(r):
        r["attributes"][0]["sources"][0]["source"] = "crmm"
        r["matching"]["comparators"][1]["attr"] = "birth_dat"
        r["matching"]["blocking"][0]["parts"][0]["attr"] = "lastname"

    code, report = cli_json("validate", mutated(mutate))
    assert code == 1
    found = dict(messages(report))
    assert "Did you mean 'crm'?" in found["attributes[0].sources[0].source"]
    assert "Did you mean 'birth_date'?" in found["matching.comparators[1].attr"]
    assert "Did you mean 'last_name'?" in found["matching.blocking[0].parts[0].attr"]


def test_comparator_not_valid_for_type_lists_allowed(cli_json, mutated):
    code, report = cli_json(
        "validate", mutated(lambda r: r["matching"]["comparators"][1].update(method="levenshtein"))
    )
    [(where, message)] = messages(report)
    assert where == "matching.comparators[1].method"
    assert "allowed for date: date_year, exact" in message


def test_unknown_option_and_bad_value_suggestions(cli_json, mutated):
    def mutate(r):
        r["attributes"][0]["nullabel"] = False
        r["attributes"][2]["type"] = "dat"

    code, report = cli_json("validate", mutated(mutate))
    found = dict(messages(report))
    assert "unknown option 'nullabel'" in found["attributes[0].nullabel"]
    assert "Did you mean 'nullable'?" in found["attributes[0].nullabel"]
    assert "allowed: string, int, decimal, date, timestamp, bool" in found["attributes[2].type"]
    assert "Did you mean 'date'?" in found["attributes[2].type"]


def test_missing_field_and_bad_identifier(cli_json, mutated):
    def mutate(r):
        del r["state"]["dsn_env"]
        r["attributes"][0]["name"] = "Last Name"

    code, report = cli_json("validate", mutated(mutate))
    found = dict(messages(report))
    assert found["state.dsn_env"] == "required field is missing"
    assert "invalid identifier 'Last Name'" in found["attributes[0].name"]


def test_check_value_union_error_is_collapsed(cli_json, mutated):
    code, report = cli_json(
        "validate", mutated(lambda r: r["attributes"][0]["checks"][0].update(value={"a": 1}))
    )
    assert len(report["errors"]) == 1
    assert report["errors"][0]["path"] == "attributes[0].checks[0].value"


def test_invalid_regex_is_an_error_not_a_crash(cli_json, mutated):
    code, report = cli_json(
        "validate", mutated(lambda r: r["attributes"][1]["checks"][0].update(value="([a-"))
    )
    assert code == 1
    [(where, message)] = messages(report)
    assert where == "attributes[1].checks[0].value"
    assert "invalid regular expression" in message


def test_most_recent_without_revision_explains_the_fix(cli_json, mutated):
    def mutate(r):
        r["sources"][0].pop("revision")
        r["attributes"][1]["strategy"] = "most_recent"

    code, report = cli_json("validate", mutated(mutate))
    [(where, message)] = messages(report)
    assert where == "attributes[1].sources[0].source"
    assert "set sources[0].revision" in message


def test_unmapped_source_and_duplicate_ids(cli_json, mutated):
    def mutate(r):
        r["sources"].append({**r["sources"][0], "id": "orphan"})

    code, report = cli_json("validate", mutated(mutate))
    assert dict(messages(report)) == {
        "sources[2]": "source 'orphan' is not mapped by any attribute"
    }


def test_yaml_syntax_error_reports_line(cli_json, tmp_path):
    path = tmp_path / "bad.yml"
    path.write_text("referential: x\nsources: [oops\nversion: 1\n", encoding="utf-8")
    code, report = cli_json("validate", path)
    assert code == 1 and report["errors"][0]["code"] == "yaml-syntax"
    assert report["errors"][0]["line"] >= 2


def test_errors_carry_the_yaml_line(cli, mutated):
    code, out = cli("validate", mutated(lambda r: r["state"].update(extra=1)))
    assert code == 1
    assert "ERROR mutated.yml:" in out and "state.extra: unknown option 'extra'" in out


def test_missing_file(cli_json, tmp_path):
    code, report = cli_json("validate", tmp_path / "nope.yml")
    assert code == 1 and not report["ok"]


def lint_codes(cli_json, mutated, mutate) -> set[str]:
    code, report = cli_json("validate", mutated(mutate))
    assert code == 0, report
    return codes(report)


def test_lint_nonnull_attribute_not_fed_by_every_source(cli_json, mutated):
    found = lint_codes(cli_json, mutated, lambda r: r["attributes"][0]["sources"].pop())
    assert "nonnull-partial-coverage" in found


def test_lint_source_mapped_by_fewer_than_two_attributes(cli_json, mutated):
    def mutate(r):
        for attr in r["attributes"][1:]:
            attr["sources"] = [s for s in attr["sources"] if s["source"] != "erp"]

    assert "source-few-attributes" in lint_codes(cli_json, mutated, mutate)


def test_lint_blocking_on_sparse_nullable_attribute(cli_json, mutated):
    def mutate(r):
        r["attributes"][2]["sources"].pop()
        r["matching"]["blocking"].append({"name": "bd", "parts": [{"attr": "birth_date"}]})

    assert "blocking-sparse" in lint_codes(cli_json, mutated, mutate)


def test_lint_all_comparators_nullable(cli_json, mutated):
    found = lint_codes(cli_json, mutated, lambda r: None)
    assert "all-comparators-nullable" not in found  # last_name is non-nullable
    found = lint_codes(
        cli_json,
        mutated,
        lambda r: [a.update(nullable=True) for a in r["attributes"]],
    )
    assert "all-comparators-nullable" in found


def test_lint_priority_where_most_recent_is_probably_intended(cli_json, mutated):
    found = lint_codes(cli_json, mutated, lambda r: None)
    assert "maybe-most-recent" in found  # email, 2 sources with revision, strategy priority

    def mutate(r):
        r["attributes"][1]["strategy"] = "most_recent"

    assert "maybe-most-recent" not in lint_codes(cli_json, mutated, mutate)


def test_lint_in_set_not_surviving_normalisation(cli_json, mutated):
    def mutate(r):
        r["attributes"][0]["checks"] = [{"rule": "in_set", "value": ["FR", "be"]}]

    assert "in-set-not-normalized" in lint_codes(cli_json, mutated, mutate)


def test_lint_null_value_on_nonnullable_and_no_revision(cli_json, mutated):
    def mutate(r):
        r["attributes"][0]["checks"][0]["on_fail"] = "null_value"
        r["sources"][1].pop("revision")

    found = lint_codes(cli_json, mutated, mutate)
    assert {"null-on-nonnullable", "source-no-revision"} <= found


def test_lint_many_attributes_note(cli_json, mutated):
    def mutate(r):
        for i in range(200):
            r["attributes"].append(
                {"name": f"extra_{i}", "sources": [{"source": "crm", "column": f"c{i}"}]}
            )

    code, report = cli_json("validate", mutated(mutate))
    assert code == 0
    note = next(w for w in report["warnings"] if w["code"] == "many-attributes")
    assert note["level"] == "info" and "203 attributes" in note["message"]


def test_strict_turns_warnings_into_failure(cli, mutated):
    path = mutated(lambda r: None)
    assert cli("validate", path)[0] == 0
    assert cli("validate", path, "--strict")[0] == 1
