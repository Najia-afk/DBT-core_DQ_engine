"""Fixtures communes : sources Parquet synthétiques et config YAML commentée."""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from engine.cli.main import main

CONFIG = """\
# Référentiel de test : les commentaires doivent survivre à add-source.
referential: customer
version: 1

state:
  dsn_env: MDM_STATE_DSN
  schema: mdm_customer

sources:
  # export CRM
  - {{id: crm, kind: parquet, location: "{crm}", key: customer_id, revision: updated_at}}
  - id: erp            # style bloc
    kind: parquet
    location: "{erp}"
    key: client_no
    revision: modified_at

attributes:
  - name: last_name      # nom de famille
    nullable: false
    normalize: [trim, lower, strip_accents]
    sources: [{{source: crm, column: LASTNAME}}, {{source: erp, column: nom}}]
    checks:
      - {{rule: max_length, value: 10, on_fail: reject_record}}

  - name: email
    normalize: [trim, lower]
    sources:
      - {{source: crm, column: EMAIL}}
      # l'ERP passe en dernier
      - {{source: erp, column: mail}}
    checks: [{{rule: regex, value: '^[^@\\s]+@[^@\\s]+$', on_fail: null_value}}]

  - name: birth_date
    type: date
    sources: [{{source: crm, column: BIRTHDATE}}, {{source: erp, column: naissance}}]

matching:
  version: t-1
  blocking:
    - name: ln
      parts: [{{attr: last_name, transform: prefix, n: 4}}]
  comparators:
    - {{attr: last_name, method: levenshtein, weight: 0.6}}
    - {{attr: birth_date, method: exact, weight: 0.4}}
  contradictions:
    - {{attr: birth_date}}
"""


def crm_rows() -> dict[str, list]:
    n = 100
    base = datetime(2024, 1, 1)
    ids = list(range(1, 99)) + [98, 98]  # 2 lignes en doublon de clé
    last, email, birth = [], [], []
    for i in range(1, n + 1):
        if i % 10 == 0:
            last.append(None)  # 10 nuls
        elif i % 20 == 1:
            last.append("Abcdefghijklmnop")  # 5 lignes de plus de 10 caractères
        elif i % 2:
            last.append("  Élise ")
        else:
            last.append("ELISE")
        email.append("not-an-email" if i % 15 == 0 else f"  U{i}@X.FR ")  # 6 invalides
        if i % 25 == 0:
            birth.append("31/12/1999")  # 4 invalides
        elif i % 10 == 5:
            birth.append(None)  # 10 nuls
        else:
            birth.append(f"19{70 + i % 30}-01-15")
    return {
        "customer_id": ids,
        "updated_at": [base + timedelta(days=i) for i in range(n)],
        "LASTNAME": last,
        "EMAIL": email,
        "BIRTHDATE": birth,
    }


@pytest.fixture
def data_dir(tmp_path: Path) -> Path:
    d = tmp_path / "data"
    d.mkdir()
    pq.write_table(pa.table(crm_rows()), d / "crm.parquet")
    erp = {
        "client_no": [f"C{i}" for i in range(50)],
        "modified_at": [datetime(2024, 2, 1) + timedelta(days=i) for i in range(50)],
        "nom": [f"Nom{i}" for i in range(50)],
        "mail": [f"c{i}@erp.fr" for i in range(50)],
        "naissance": [datetime(1980, 1, 1).date() for _ in range(50)],
    }
    pq.write_table(pa.table(erp), d / "erp.parquet")
    web = {
        "account_id": list(range(1, 31)),
        "event_ts": [datetime(2024, 3, 1) + timedelta(hours=i) for i in range(30)],
        "surname": [f"Web{i}" for i in range(30)],
        "contact": [f"w{i}@web.fr" for i in range(30)],
    }
    pq.write_table(pa.table(web), d / "web.parquet")
    return d


@pytest.fixture
def config_path(tmp_path: Path, data_dir: Path) -> Path:
    path = tmp_path / "customer.yml"
    text = CONFIG.format(
        crm=(data_dir / "crm.parquet").as_posix(), erp=(data_dir / "erp.parquet").as_posix()
    )
    path.write_text(text, encoding="utf-8", newline="\n")
    return path


@pytest.fixture
def cli(capsys):
    def run(*args: str) -> tuple[int, str]:
        code = main([str(a) for a in args])
        return code, capsys.readouterr().out

    return run


@pytest.fixture
def cli_json(cli):
    def run(*args: str) -> tuple[int, dict]:
        code, out = cli(*args, "--json")
        return code, json.loads(out)

    return run
