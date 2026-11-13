"""Génération de docs/bench/RESULTS.md : tableaux (JSON des runs) entre deux marqueurs."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .project import render_projection

START = "<!-- BENCH:GENERATED:START -->"
END = "<!-- BENCH:GENERATED:END -->"
SKELETON = f"# Résultats du banc MDM\n\n{START}\n{END}\n"


def fmt(value: Any, digits: int = 1) -> str:
    if value is None:
        return "–"
    if isinstance(value, float):
        return f"{value:,.{digits}f}".replace(",", " ")
    if isinstance(value, int) and not isinstance(value, bool):
        return f"{value:,}".replace(",", " ")
    return str(value)


def table(header: list[str], rows: list[list[Any]]) -> str:
    lines = ["| " + " | ".join(header) + " |", "|" + "|".join("---" for _ in header) + "|"]
    lines += ["| " + " | ".join(fmt(c) for c in row) + " |" for row in rows]
    return "\n".join(lines)


def load_runs(out_dir: Path) -> list[dict[str, Any]]:
    runs = []
    for path in sorted((out_dir / "runs").glob("*.json")):
        if path.name.startswith(("smoke", "test")):
            continue
        runs.append(json.loads(path.read_text(encoding="utf-8")))
    return runs


def run_title(run: dict[str, Any]) -> str:
    c = run["config"]
    return (
        f"{run['name']} — {run['lane']} {c['nodes']:,} nœuds × {c['attributes']} attributs, "
        f"{c['days']} jour(s) delta"
    ).replace(",", " ")


def overview_table(runs: list[dict[str, Any]]) -> str:
    rows = []
    for r in runs:
        c, t = r["config"], r["totals"]
        rows.append(
            [
                r["name"],
                r["lane"],
                r["machine"]["date"][:16].replace("T", " "),
                r["status"],
                c["nodes"],
                c["attributes"],
                c["days"],
                t["phases_wall_s"],
                round(t["total_peak_mb"] / 1024, 1),
                round((t.get("pg_db_mb_final") or 0) / 1024, 2),
                round((t.get("pgdata_peak_mb") or 0) / 1024, 2),
                c["hash"],
                c["seed"],
            ]
        )
    header = ["run", "voie", "date", "statut", "nœuds", "attr.", "jours delta", "Σ phases (s)",
              "pic RAM client+PG (Go)", "base PG finale (Go)", "pic PGDATA (Go)", "hash cfg",
              "graine"]  # fmt: skip
    return table(header, rows)


def phase_table(run: dict[str, Any]) -> str:
    rows = []
    for p in run["phases"]:
        rows.append(
            [
                p["day"],
                p["phase"],
                p["wall_s"],
                p.get("rows"),
                p.get("rows_per_s"),
                p["rss_peak_mb"],
                p.get("container_mem_peak_mb"),
                p.get("container_cpu_peak_pct"),
                p["pg_db_mb"],
                p.get("pgdata_mb_peak", p["pgdata_mb"]),
                p["wal_mb"],
                p.get("workdir_mb_peak", p.get("workdir_mb")),
                p["status"],
            ]
        )
    header = ["jour", "phase", "mur (s)", "lignes", "lignes/s", "RSS client pic (Mo)",
              "RAM PG pic (Mo)", "CPU PG pic (%)", "base PG (Mo)", "PGDATA pic (Mo)", "WAL (Mo)",
              "workdir pic (Mo)", "statut"]  # fmt: skip
    return table(header, rows)


def deltas_text(deltas: list[dict[str, float]]) -> str:
    return "; ".join(
        f"upd {x['update_pct']}% new {x['new_pct']}% del {x['delete_pct']}% brk {x['broken_pct']}%"
        for x in deltas
    )


def run_details(run: dict[str, Any]) -> str:
    c, m, d = run["config"], run["machine"], run.get("dataset") or {}
    versions = (
        f"python {m['python']}, duckdb {m['duckdb']}, pyarrow {m['pyarrow']}, "
        f"psycopg {m['psycopg']}, dbt-core {m['dbt-core']}, dbt-duckdb {m['dbt-duckdb']}, "
        f"docker {m['docker_server']}"
    )
    info = table(
        ["clé", "valeur"],
        [
            ["date", m["date"]],
            ["machine", f"{m['cpu_threads']} threads, {m['ram_gb']} Go RAM, {m['os']}"],
            ["versions", versions],
            ["PostgreSQL", f"{run.get('pg_server')} ({run['pg_image']})"],
            [
                "config",
                f"hash {c['hash']}, graine {c['seed']}, sources {c['kinds']} / {c['snapshots']}, "
                f"ingest {c['ingest_mode']}, batch_rows {c['batch_rows']}",
            ],
            ["deltas (jour 1..N)", deltas_text(c["deltas"])],
            ["réglages PG", json.dumps(c["pg_tuning"])],
            [
                "client",
                f"ELT mem {c['elt_memory_limit']} threads {c['elt_threads']} ; "
                f"ETL workers {c['etl_workers']}",
            ],
            [
                "jeu de données",
                f"{fmt(d.get('bytes', 0) / 1024**3, 2)} Go, génération {d.get('gen_wall_s')} s, "
                f"RSS pic {d.get('gen_rss_peak_mb')} Mo",
            ],
            [
                "pic hôte (psutil used)",
                f"{run['totals'].get('host_used_peak_gb')} Go (base avant run "
                f"{run['totals'].get('host_used_baseline_gb')} Go)",
            ],
        ],
    )
    status = f"Statut : **{run['status']}**"
    if run.get("abort_reason"):
        status += f" — {run['abort_reason']}"
    lines = [
        f"### {run_title(run)}", "", status, "", info, "", phase_table(run), "",
        "<details><summary>commandes exactes</summary>", "", "```text",
        *run.get("commands", []), "```", "</details>",
    ]  # fmt: skip
    return "\n".join(lines)


def equivalence_section(out_dir: Path) -> str:
    folder = out_dir / "equivalence"
    if not folder.exists():
        return ""
    parts = ["## Équivalence", ""]
    for path in sorted(folder.glob("*.json")):
        data = json.loads(path.read_text(encoding="utf-8"))
        reports: dict[str, dict] = {}
        parts.append(f"### {path.stem}")
        if "lanes" in data:
            rows = []
            for lane, entry in data["lanes"].items():
                for day, rep in entry.get("days", {}).items():
                    reports[f"{lane}/jour {day}"] = rep
                    rows.append([lane, day, "égal" if rep.get("ok") else "DIVERGE",
                                 json.dumps(rep.get("totals") or {}), rep.get("seconds"),
                                 rep.get("rss_peak_mb")])  # fmt: skip
                if entry["status"] != "ok":
                    rows.append([lane, "–", entry["status"], entry.get("abort_reason"), None, None])
            header = ["voie", "jour", "résultat", "écarts (catégorie → nb)", "s", "RSS Mo"]
            parts += ["", table(header, rows), "", f"Référence : {json.dumps(data['reference'])}"]
        else:
            rows = []
            for pair, rep in data["pairs"].items():
                reports[pair] = rep
                rows.append([pair, "égal" if rep.get("ok") else "DIVERGE",
                             json.dumps(rep.get("totals") or {}), rep.get("seconds"),
                             rep.get("rss_peak_mb")])  # fmt: skip
            parts += ["", table(["paire", "résultat", "écarts", "s", "RSS Mo"], rows)]
        for key, rep in reports.items():
            if rep.get("details"):
                parts += [
                    "",
                    f"Détails {key} (20 premiers) :",
                    "",
                    "```text",
                    *rep["details"],
                    "```",
                ]
        parts.append("")
    return "\n".join(parts)


def concurrency_section(out_dir: Path) -> str:
    folder = out_dir / "concurrency"
    if not folder.exists():
        return ""
    parts = ["## Concurrence via engine.runtime", ""]
    for path in sorted(folder.glob("*.json")):
        data = json.loads(path.read_text(encoding="utf-8"))
        parts += [
            f"### {path.stem}",
            "",
            "```json",
            json.dumps(data["summary"], indent=1),
            "```",
            "",
        ]
    return "\n".join(parts)


def generated_block(out_dir: Path) -> str:
    runs = load_runs(out_dir)
    parts = ["## Runs mesurés (généré par `python -m engine.bench report`)", ""]
    if runs:
        parts += [overview_table(runs), ""]
        parts += [render_projection(runs), ""]
        parts += [run_details(r) + "\n" for r in runs]
    parts += [equivalence_section(out_dir), concurrency_section(out_dir)]
    return "\n".join(parts)


def splice(text: str, block: str) -> str:
    if START not in text or END not in text:
        text = text.rstrip() + f"\n\n{START}\n{END}\n"
    head, rest = text.split(START, 1)
    _, tail = rest.split(END, 1)
    return f"{head}{START}\n{block}\n{END}{tail}"


def write_results(out_dir: Path) -> Path:
    path = out_dir / "RESULTS.md"
    text = path.read_text(encoding="utf-8") if path.exists() else SKELETON
    path.write_text(splice(text, generated_block(out_dir)), encoding="utf-8")
    return path
