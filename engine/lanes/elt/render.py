"""Génération du projet dbt depuis la config : gabarits `dbt_project/` -> répertoire de travail.

Deux niveaux de Jinja : le moteur de rendu de ce module (délimiteurs `<< >>` / `<% %>`) déroule la
config (boucles sur les attributs, expressions SQL générées) ; ce qui est produit contient encore
le Jinja natif de dbt (`{{ ref() }}`, `{{ config() }}`, macros) évalué ensuite par dbt.
"""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any

from jinja2 import Environment, StrictUndefined

from . import sqlgen
from .settings import Settings, format_memory
from .sqlgen import Spec

TEMPLATES = Path(__file__).parent / "dbt_project"


def _environment(spec: Spec) -> Environment:
    env = Environment(
        variable_start_string="<<",
        variable_end_string=">>",
        block_start_string="<%",
        block_end_string="%>",
        comment_start_string="<#",
        comment_end_string="#>",
        trim_blocks=True,
        lstrip_blocks=True,
        undefined=StrictUndefined,
        keep_trailing_newline=True,
    )
    env.globals.update(
        spec=spec,
        S=spec.schema_name,
        q=sqlgen.q,
        lit=sqlgen.lit,
        normalize_sql=sqlgen.normalize_sql,
        canon=sqlgen.canonical_text_sql,
        check_pass=sqlgen.check_pass_sql,
        push=lambda name, **kw: push_config(spec, name, **kw),
    )
    return env


def push_config(spec: Spec, name: str, index: str | None = None, analyze: bool = True) -> str:
    """`config()` dbt : la table est publiée dans PostgreSQL (table de travail) après sa création.

    Les hooks ne sont pas transactionnels : DuckDB refuse d'écrire dans deux bases dans la même
    transaction."""
    target = f"pg.{spec.schema_name}.{name}"
    hooks = [f"create or replace table {target} as select * from {{{{ this }}}}"]
    if index:
        hooks.append(
            f"call postgres_execute('pg', 'create index on {spec.schema_name}.{name} ({index})')"
        )
    if analyze:
        hooks.append(f"call postgres_execute('pg', 'analyze {spec.schema_name}.{name}')")
    items = ", ".join("{'sql': " + repr(h) + ", 'transaction': False}" for h in hooks)
    return "{{ config(materialized='table', post_hook=[" + items + "]) }}"


def render_project(
    spec: Spec,
    settings: Settings,
    dest: Path,
    kind: str,
    context: dict[str, Any],
    scratch_dir: Path,
) -> Path:
    """Écrit un projet dbt complet (`kind` = ingest | reconcile) dans `dest`."""
    if dest.exists():
        shutil.rmtree(dest)
    (dest / "models").mkdir(parents=True)
    shutil.copytree(TEMPLATES / "macros", dest / "macros")
    shutil.copy(TEMPLATES / "dbt_project.yml", dest / "dbt_project.yml")
    env = _environment(spec)
    profile_ctx = {
        "dbt_threads": 2,
        "threads": settings.threads,
        "memory_limit": format_memory(settings.memory_bytes),
        "temp_directory": (scratch_dir / "spill").as_posix(),
        "module_path": Path(__file__).resolve().parents[3].as_posix(),
    }
    profile = env.from_string((TEMPLATES / "profiles.yml").read_text(encoding="utf-8"))
    (dest / "profiles.yml").write_text(profile.render(**profile_ctx), encoding="utf-8")
    for template in sorted((TEMPLATES / "models" / kind).glob("*.sql")):
        try:
            text = env.from_string(template.read_text(encoding="utf-8")).render(**context)
        except Exception as error:
            raise RuntimeError(f"template {template.name}: {error}") from error
        (dest / "models" / template.name).write_text(text, encoding="utf-8")
    return dest
