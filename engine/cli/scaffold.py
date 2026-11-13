"""`new-referential` : squelette de config valide et commenté."""

from __future__ import annotations

import re
from pathlib import Path

from engine.cli.schema import schema_text
from engine.cli.validation import validate_text
from engine.core.config import IDENT

SCHEMA_FILE = "mdm.schema.json"

TEMPLATE = """\
# yaml-language-server: $schema=./{schema_file}
# Référentiel {name}. Complétez les zones CHANGE_ME puis lancez :
#   python -m engine.cli validate {name}.yml
#   python -m engine.cli explain {name}.yml
#   python -m engine.cli check-source {name}.yml --source src_a
referential: {name}
version: 1

# Base PostgreSQL de l'état et de la publication (le DSN est dans la variable d'environnement).
state:
  dsn_env: MDM_STATE_DSN
  schema: {schema}

# Une ligne par source. Pour en ajouter une avec ses mappings :
#   python -m engine.cli add-source {name}.yml --source-id web --kind parquet ... --map attr=col
sources:
  - {{id: src_a, kind: parquet, location: /data/CHANGE_ME/*.parquet, key: id,
     revision: updated_at, snapshot: full}}
  # - {{id: crm, kind: postgres, location: public.crm_customers, dsn_env: CRM_DSN,
  #    key: customer_id, revision: updated_at}}

# L'ORDRE de `sources` dans un attribut = priorité de survie (la première valeur non nulle gagne).
# nullable: false => si toutes les sources sont nulles, le golden est BLOQUÉ.
attributes:
  - name: last_name
    nullable: false
    normalize: [trim, lower, strip_accents, collapse_spaces]
    sources: [{{source: src_a, column: last_name}}]
    checks:
      - {{rule: max_length, value: 120, on_fail: reject_record}}

  - name: birth_date
    type: date
    sources: [{{source: src_a, column: birth_date}}]

  - name: email
    normalize: [trim, lower]
    sources: [{{source: src_a, column: email}}]
    strategy: priority            # ou most_recent (nécessite `revision` sur chaque source)
    checks:
      - {{rule: regex, value: '^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$', on_fail: null_value}}

# Rapprochement : blocage (candidats), comparateurs (score), seuils.
matching:
  version: {name}-match-1
  blocking:
    - name: ln_year
      parts: [{{attr: last_name, transform: prefix, n: 4}}, {{attr: birth_date, transform: year}}]
    - name: email
      parts: [{{attr: email}}]
  comparators:                    # la somme des poids doit valoir 1
    - {{attr: last_name, method: levenshtein, weight: 0.5}}
    - {{attr: birth_date, method: exact, weight: 0.3}}
    - {{attr: email, method: exact, weight: 0.2}}
  contradictions:
    - {{attr: birth_date}}
  accept: 0.95                    # >= accept : fusion automatique
  review: 0.80                    # entre review et accept : revue manuelle
  min_evidence: 0.5

publication:
  provenance: true
"""


def render(name: str) -> str:
    if not re.match(IDENT, name):
        raise ValueError(
            f"invalid name {name!r}: use lowercase letters, digits and underscores, "
            "starting with a letter (max 63 characters)"
        )
    schema = f"mdm_{name}" if len(name) <= 59 else name
    text = TEMPLATE.format(name=name, schema=schema, schema_file=SCHEMA_FILE)
    result = validate_text(text)
    if result.errors:
        raise ValueError("internal error: scaffold is invalid: " + result.errors[0].message)
    return text


def scaffold(name: str, out_dir: Path, force: bool = False) -> list[Path]:
    text = render(name)
    config, schema_path = out_dir / f"{name}.yml", out_dir / SCHEMA_FILE
    if config.exists() and not force:
        raise FileExistsError(f"{config} already exists (use --force to overwrite)")
    out_dir.mkdir(parents=True, exist_ok=True)
    config.write_text(text, encoding="utf-8", newline="\n")
    schema_path.write_text(schema_text(), encoding="utf-8", newline="\n")
    return [config, schema_path]
