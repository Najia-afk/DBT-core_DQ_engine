"""Point d'entrée : `python -m engine.cli <commande>`. Code de sortie 0 (OK) ou 1 (erreur)."""

from __future__ import annotations

import argparse
import difflib
import json
import os
import sys
from pathlib import Path

import yaml

from engine.cli import checksource, explain, scaffold
from engine.cli.addsource import AddSourceError, add_source, build_source, parse_mapping
from engine.cli.issues import Issue
from engine.cli.lint import lint
from engine.cli.schema import schema_text
from engine.cli.validation import Validation, suggest, validate_text
from engine.cli.yamlio import compose, read_text_file


class Parser(argparse.ArgumentParser):
    def error(self, message: str):
        self.print_usage(sys.stderr)
        self.exit(1, f"{self.prog}: error: {message}\n")


def build_parser() -> argparse.ArgumentParser:
    parser = Parser(
        prog="python -m engine.cli",
        description="Outils pour brancher une source et vérifier un référentiel MDM (YAML).",
    )
    sub = parser.add_subparsers(dest="command", required=True, parser_class=Parser)

    def command(name: str, help_: str) -> argparse.ArgumentParser:
        p = sub.add_parser(name, help=help_, description=help_)
        p.add_argument("--json", action="store_true", help="sortie JSON")
        return p

    p = command("validate", "valide la config (erreurs claires avec chemin YAML) et lint")
    p.add_argument("config")
    p.add_argument("--strict", action="store_true", help="les warnings donnent le code 1")

    p = command("explain", "décrit en langage clair ce que la config fait")
    p.add_argument("config")
    p.add_argument("--attribute", help="un seul attribut")
    p.add_argument("--lang", choices=["fr", "en"], default="fr")

    p = command("schema", "exporte le JSON Schema (auto-complétion de l'éditeur)")
    p.add_argument("--out", help="fichier de sortie (défaut : stdout)")

    p = command("add-source", "ajoute une source et ses mappings (commentaires préservés)")
    p.add_argument("config")
    p.add_argument("--source-id", required=True)
    p.add_argument("--kind", required=True, choices=["postgres", "parquet"])
    p.add_argument("--location", required=True, help="schema.table ou chemin/glob Parquet")
    p.add_argument("--key", required=True, help="colonne source de la clé")
    p.add_argument("--revision", help="colonne source de révision")
    p.add_argument("--dsn-env", help="postgres : variable d'environnement du DSN")
    p.add_argument("--snapshot", choices=["full", "delta"])
    p.add_argument("--full-snapshot-min-ratio", type=float)
    p.add_argument(
        "--map", dest="maps", nargs="+", action="extend", default=[], metavar="ATTR=COL[@POS]",
        help="mapping attribut=colonne ; @POS = priorité (1 = première, défaut : en dernier)",
    )  # fmt: skip
    p.add_argument("--dry-run", action="store_true", help="affiche le diff sans écrire")

    p = command("check-source", "profile la source réelle contre le mapping")
    p.add_argument("config")
    p.add_argument("--source", required=True, help="identifiant de la source")
    p.add_argument("--sample", type=int, help="ne profiler que les N premières lignes")

    p = command("new-referential", "crée une config minimale valide et commentée")
    p.add_argument("name")
    p.add_argument("--out", required=True, help="dossier de sortie")
    p.add_argument("--force", action="store_true", help="écrase un fichier existant")
    return parser


def out(args: argparse.Namespace, payload: dict, text: str) -> None:
    print(json.dumps(payload, indent=2, default=str) if args.json else text.rstrip("\n"))


def fail(args: argparse.Namespace, message: str) -> int:
    out(args, {"ok": False, "error": message}, f"ERROR {message}")
    return 1


def load(args: argparse.Namespace) -> tuple[Validation | None, int]:
    """Valide args.config ; en cas d'erreur, affiche et renvoie le code 1."""
    path = Path(args.config)
    try:
        text = read_text_file(path).text
    except (OSError, UnicodeDecodeError) as error:
        return None, fail(args, f"cannot read {path}: {error}")
    result = validate_text(text)
    if result.errors:
        report = {"ok": False, "file": str(path), "errors": [e.as_dict() for e in result.errors]}
        lines = [e.render(path) for e in result.errors]
        lines.append(f"INVALID: {len(result.errors)} error(s)")
        out(args, report, "\n".join(lines))
        return None, 1
    return result, 0


def cmd_validate(args: argparse.Namespace) -> int:
    result, code = load(args)
    if result is None:
        return code
    path = Path(args.config)
    warnings = lint(result.config, compose(read_text_file(path).text))
    n_warn = sum(w.level == "warning" for w in warnings)
    lines = [w.render(path) for w in warnings]
    lines.append(f"OK: 0 error(s), {n_warn} warning(s), {len(warnings) - n_warn} note(s)")
    payload = {
        "ok": True, "file": str(path), "errors": [], "warnings": [w.as_dict() for w in warnings]
    }  # fmt: skip
    out(args, payload, "\n".join(lines))
    return 1 if args.strict and n_warn else 0


def cmd_explain(args: argparse.Namespace) -> int:
    result, code = load(args)
    if result is None:
        return code
    names = [a.name for a in result.config.attributes]
    if args.attribute and args.attribute not in names:
        return fail(
            args,
            f"unknown attribute '{args.attribute}' (known: {', '.join(names)})."
            f"{suggest(args.attribute, names)}",
        )
    data = explain.explain(result.config, args.attribute)
    out(args, data, explain.render(data, args.lang))
    return 0


def cmd_schema(args: argparse.Namespace) -> int:
    text = schema_text()
    if args.out:
        Path(args.out).write_text(text, encoding="utf-8", newline="\n")
        out(args, {"ok": True, "written": args.out}, f"schema written to {args.out}")
    else:
        sys.stdout.write(text)
    return 0


def cmd_add_source(args: argparse.Namespace) -> int:
    path = Path(args.config)
    result, code = load(args)
    if result is None:
        return code
    file = read_text_file(path)
    source = build_source({
        "id": args.source_id, "kind": args.kind, "location": args.location, "key": args.key,
        "revision": args.revision, "dsn_env": args.dsn_env, "snapshot": args.snapshot,
        "full_snapshot_min_ratio": args.full_snapshot_min_ratio,
    })  # fmt: skip
    try:
        maps = [parse_mapping(m) for m in args.maps]
        new_text, expected = add_source(file.text, result.raw, source, maps)
    except AddSourceError as error:
        return fail(args, str(error))
    check = validate_text(new_text)
    if check.errors or yaml.safe_load(new_text) != expected:
        errors = check.errors or [Issue("error", "internal", "", "text edit differs from intent")]
        lines = [e.render(path) for e in errors] + ["NOT WRITTEN: the result would be invalid"]
        payload = {"ok": False, "written": False, "errors": [e.as_dict() for e in errors]}
        out(args, payload, "\n".join(lines))
        return 1
    diff = "".join(
        difflib.unified_diff(
            file.text.splitlines(True),
            new_text.splitlines(True),
            f"a/{path.name}",
            f"b/{path.name}",
        )
    )
    known = {(w.code, w.path, w.message) for w in lint(result.config, compose(file.text))}
    added = [
        w for w in lint(check.config, compose(new_text)) if (w.code, w.path, w.message) not in known
    ]
    if not args.dry_run:
        tmp = path.with_name(f"{path.name}.{os.getpid()}.tmp")
        try:
            tmp.write_bytes(file.restore(new_text))
            os.replace(tmp, path)
        finally:
            tmp.unlink(missing_ok=True)
    status = "dry-run: nothing written" if args.dry_run else f"written: {path}"
    lines = [diff.rstrip("\n"), "", *(w.render(path) for w in added), status]
    payload = {
        "ok": True, "written": not args.dry_run, "diff": diff,
        "new_warnings": [w.as_dict() for w in added],
    }  # fmt: skip
    out(args, payload, "\n".join(lines))
    return 0


def cmd_check_source(args: argparse.Namespace) -> int:
    result, code = load(args)
    if result is None:
        return code
    ids = [s.id for s in result.config.sources]
    if args.source not in ids:
        return fail(
            args,
            f"unknown source '{args.source}' (known: {', '.join(ids)}).{suggest(args.source, ids)}",
        )
    if args.sample is not None and args.sample < 1:
        return fail(args, "--sample must be >= 1")
    report = checksource.check_source(result.config, args.source, args.sample)
    out(args, report, checksource.render(report))
    return 1 if report["verdict"] == "BLOCKING" else 0


def cmd_new_referential(args: argparse.Namespace) -> int:
    try:
        written = scaffold.scaffold(args.name, Path(args.out), args.force)
    except (ValueError, FileExistsError) as error:
        return fail(args, str(error))
    text = "\n".join(f"written: {p}" for p in written)
    out(args, {"ok": True, "written": [str(p) for p in written]}, text)
    return 0


COMMANDS = {
    "validate": cmd_validate,
    "explain": cmd_explain,
    "schema": cmd_schema,
    "add-source": cmd_add_source,
    "check-source": cmd_check_source,
    "new-referential": cmd_new_referential,
}


def main(argv: list[str] | None = None) -> int:
    for stream in (sys.stdout, sys.stderr):
        if not stream.isatty() and hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8")  # sortie redirigée : UTF-8, pas la page de code
    args = build_parser().parse_args(argv)
    try:
        return COMMANDS[args.command](args)
    except BrokenPipeError:
        return 0
