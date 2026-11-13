"""Fausse lane pour les tests du runtime.

Respecte le contrat MDM §6 (CLI + une ligne JSON de synthèse sur stdout, code ≠ 0 en échec).
Comportement piloté par FAKE_LANE_DIR/scenario.json : clés `default`, `<step>` ou
`<referentiel>/<step>` (step = `ingest:crm`, `reconcile`...), valeurs possibles :
  sleep (s), fail_times (échoue aux N premières tentatives), exit_code, retryable (bool),
  alloc_mb (alloue et touche la mémoire), hang (ne termine jamais), warnings (int).
Chaque tentative est tracée dans events/ ; l'« effet » final (un fichier dans effects/)
n'est volontairement pas idempotent : deux tentatives qui terminent = deux fichiers.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path


def append(path: Path, line: str) -> None:
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(line + "\n")


def record(directory: Path, line: str) -> None:
    """Un fichier par enregistrement : les ajouts concurrents au même fichier se perdent sous
    Windows (O_APPEND émulé), ce qui rendrait les tests instables."""
    directory.mkdir(exist_ok=True)
    name = f"{time.time_ns()}-{os.getpid()}"
    (directory / name).write_text(line, encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("command")
    parser.add_argument("--config", required=True)
    parser.add_argument("--source")
    parser.add_argument("--out")
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()

    base = Path(os.environ["FAKE_LANE_DIR"])
    referential = Path(args.config).stem
    step = args.command + (f":{args.source}" if args.source else "")
    scenario = json.loads((base / "scenario.json").read_text(encoding="utf-8"))
    spec = {**scenario.get("default", {}), **scenario.get(step, {})}
    spec.update(scenario.get(f"{referential}/{step}", {}))

    (base / "attempts").mkdir(exist_ok=True)
    tag = f"{args.run_id}__{step.replace(':', '_')}"
    append(base / "attempts" / tag, "x")
    attempt = len((base / "attempts" / tag).read_text().splitlines())

    def log_event(ev: str, **extra: object) -> None:
        event = {
            "ev": ev,
            "t": time.time(),
            "run_id": args.run_id,
            "step": step,
            "referential": referential,
            "attempt": attempt,
            "pid": os.getpid(),
            **extra,
        }
        record(base / "events", json.dumps(event))

    log_event("start")
    if attempt <= int(spec.get("fail_times", 0)):
        error = {"error": f"scripted failure {attempt}", "retryable": spec.get("retryable", True)}
        print(json.dumps(error))
        print("fake lane: scripted failure", file=sys.stderr)
        log_event("end", ok=False)
        return int(spec.get("exit_code", 1))

    ballast = None
    if spec.get("alloc_mb"):
        ballast = bytearray(int(spec["alloc_mb"]) * 1024 * 1024)
        for offset in range(0, len(ballast), 4096):
            ballast[offset] = 1
    if spec.get("hang"):
        while True:
            time.sleep(1)
    deadline = time.time() + float(spec.get("sleep", 0))
    while time.time() < deadline:
        time.sleep(min(0.05, max(0.0, deadline - time.time())))

    record(base / "effects", f"{args.run_id} {step} attempt={attempt}")
    log_event("end", ok=True)
    summary = {
        "lane": "fake",
        "command": args.command,
        "step": step,
        "attempt": attempt,
        "warnings": int(spec.get("warnings", 0)),
        "alloc_mb": len(ballast) // (1024 * 1024) if ballast else 0,
    }
    summary.update(spec.get("summary", {}))
    print(json.dumps(summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
