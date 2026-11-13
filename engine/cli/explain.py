"""`explain` : décrit en langage clair ce que la config fait réellement."""

from __future__ import annotations

from engine.cli import normalizers
from engine.cli.lint import roles
from engine.core.config import Attribute, Check, ReferentialConfig

TYPE_NAMES = {
    "fr": {"string": "texte", "int": "entier", "decimal": "décimal", "date": "date",
           "timestamp": "horodatage", "bool": "booléen"},
    "en": {"string": "text", "int": "integer", "decimal": "decimal", "date": "date",
           "timestamp": "timestamp", "bool": "boolean"},
}  # fmt: skip

TEXT = {
    "fr": {
        "header": "Référentiel {name} (version {version}) : {n_src} sources, {n_attr} attributs",
        "source": "  - {id} : {kind} {location} | clé {key} | révision {revision} | "
        "snapshot {snapshot}",
        "no_revision": "(aucune)",
        "required": "obligatoire (nullable: false)",
        "optional": "optionnel (nullable: true)",
        "normalize": "    Normalisation : {steps}",
        "no_normalize": "    Normalisation : aucune (valeur brute)",
        "sources_priority": "    Sources par ordre de survie (1 = gagne en premier) :",
        "sources_recent": "    Sources (l'ordre n'a pas d'effet : la révision la plus récente "
        "gagne) :",
        "source_line": "      {rank}. {source}.{column}",
        "priority": "    Stratégie priority : la première source de la liste dont la valeur "
        "est non nulle et valide gagne.",
        "most_recent": "    Stratégie most_recent : la valeur de la version la plus récente "
        "(colonne revision de chaque source) gagne ; à égalité, révision la plus grande puis "
        "(source_id, clé).",
        "null_ok": "    Si toutes les sources sont nulles : l'attribut est publié nul.",
        "null_blocked": "    Si toutes les sources sont nulles : le golden est BLOQUÉ (non publié, "
        "ancienne version conservée et marquée périmée, anomalie visible).",
        "checks": "    Contrôles (appliqués après normalisation, à l'ingestion) :",
        "no_checks": "    Contrôles : aucun",
        "check_line": "      - {desc} → {on_fail} : {effect}",
        "reject_record": "le nœud source est rejeté : aucun delta, aucune réconciliation, la "
        "dernière version valide reste active",
        "null_value": "la valeur est traitée comme nulle (une autre source peut alors gagner)",
        "warn": "la valeur est conservée, seule une anomalie est tracée",
        "roles": "    Rôle dans le rapprochement : {roles}",
        "no_roles": "    Rôle dans le rapprochement : aucun (publié seulement)",
        "role_blocking": "clé de blocage {name}",
        "role_comparator": "comparateur {name}",
        "role_contradiction": "contradiction (jamais de fusion automatique si valeurs différentes)",
        "rule_regex": "doit correspondre à /{value}/",
        "rule_in_set": "doit appartenir à {value}",
        "rule_min": "doit être ≥ {value}",
        "rule_max": "doit être ≤ {value}",
        "rule_max_length": "longueur ≤ {value}",
        "matching": "Rapprochement (règles {version})",
        "blocking": "  Clés de blocage (union des candidats ; bloc > {size} : non expansé, "
        "anomalie oversized_block) :",
        "block_line": "    - {name} : {parts}",
        "comparators": "  Comparateurs (score = somme pondérée des comparateurs actifs / somme de "
        "leurs poids ; valeur nulle d'un côté = comparateur ignoré) :",
        "comp_line": "    - {attr} : {method}, poids {weight}",
        "thresholds": "  Seuils : score ≥ {accept} et un seul candidat sans contradiction → "
        "fusion ; entre {review} et {accept} (ou plusieurs candidats) → revue manuelle ; en "
        "dessous → aucune décision. Poids actif minimal pour décider : {min_evidence}.",
        "contradictions": "  Contradictions : {attrs} (deux valeurs non nulles différentes = "
        "jamais de fusion automatique)",
        "no_contradictions": "  Contradictions : aucune",
    },
    "en": {
        "header": "Referential {name} (version {version}): {n_src} sources, {n_attr} attributes",
        "source": "  - {id}: {kind} {location} | key {key} | revision {revision} | "
        "snapshot {snapshot}",
        "no_revision": "(none)",
        "required": "required (nullable: false)",
        "optional": "optional (nullable: true)",
        "normalize": "    Normalisation: {steps}",
        "no_normalize": "    Normalisation: none (raw value)",
        "sources_priority": "    Sources in survival order (1 = wins first):",
        "sources_recent": "    Sources (order has no effect: the most recent revision wins):",
        "source_line": "      {rank}. {source}.{column}",
        "priority": "    Strategy priority: the first listed source whose value is non-null and "
        "valid wins.",
        "most_recent": "    Strategy most_recent: the value of the most recent version (each "
        "source's revision column) wins; ties: greatest revision, then (source_id, key).",
        "null_ok": "    If all sources are null: the attribute is published as null.",
        "null_blocked": "    If all sources are null: the golden is BLOCKED (not published, "
        "previous version kept and marked stale, anomaly visible).",
        "checks": "    Checks (applied after normalisation, at ingestion):",
        "no_checks": "    Checks: none",
        "check_line": "      - {desc} → {on_fail}: {effect}",
        "reject_record": "the source record is rejected: no delta, no reconcile, the last valid "
        "version stays active",
        "null_value": "the value is treated as null (another source may then win)",
        "warn": "the value is kept, only an anomaly is logged",
        "roles": "    Role in matching: {roles}",
        "no_roles": "    Role in matching: none (published only)",
        "role_blocking": "blocking key {name}",
        "role_comparator": "comparator {name}",
        "role_contradiction": "contradiction (never auto-merged when values differ)",
        "rule_regex": "must match /{value}/",
        "rule_in_set": "must be in {value}",
        "rule_min": "must be ≥ {value}",
        "rule_max": "must be ≤ {value}",
        "rule_max_length": "length ≤ {value}",
        "matching": "Matching (rules {version})",
        "blocking": "  Blocking keys (union of candidates; block > {size}: not expanded, anomaly "
        "oversized_block):",
        "block_line": "    - {name}: {parts}",
        "comparators": "  Comparators (score = weighted sum of active comparators / sum of their "
        "weights; a null on either side = comparator ignored):",
        "comp_line": "    - {attr}: {method}, weight {weight}",
        "thresholds": "  Thresholds: score ≥ {accept} with a single candidate and no contradiction "
        "→ merge; between {review} and {accept} (or several candidates) → manual review; below → "
        "no decision. Minimum active weight to decide: {min_evidence}.",
        "contradictions": "  Contradictions: {attrs} (two different non-null values = never "
        "auto-merged)",
        "no_contradictions": "  Contradictions: none",
    },
}


def _check_dict(chk: Check) -> dict:
    return {"rule": chk.rule, "value": chk.value, "on_fail": chk.on_fail}


def _attribute(attr: Attribute, role_map: dict[str, list[str]]) -> dict:
    return {
        "name": attr.name,
        "type": attr.type,
        "nullable": attr.nullable,
        "normalize": list(attr.normalize),
        "strategy": attr.strategy,
        "sources": [
            {"rank": rank, "source": sc.source, "column": sc.column}
            for rank, sc in enumerate(attr.sources, 1)
        ],
        "if_all_null": "null" if attr.nullable else "golden_blocked",
        "checks": [_check_dict(c) for c in attr.checks],
        "matching_roles": role_map.get(attr.name, []),
    }


def explain(cfg: ReferentialConfig, attribute: str | None = None) -> dict:
    role_map = roles(cfg)
    attrs = [a for a in cfg.attributes if attribute in (None, a.name)]
    m = cfg.matching
    matching = {
        "version": m.version,
        "max_block_size": m.max_block_size,
        "blocking": [
            {
                "name": k.name,
                "parts": [{"attr": p.attr, "transform": p.transform, "n": p.n} for p in k.parts],
            }
            for k in m.blocking
        ],
        "comparators": [c.model_dump() for c in m.comparators],
        "contradictions": [c.attr for c in m.contradictions],
        "accept": m.accept,
        "review": m.review,
        "min_evidence": m.min_evidence,
    }
    return {
        "referential": cfg.referential,
        "version": cfg.version,
        "sources": [s.model_dump() for s in cfg.sources],
        "attributes": [_attribute(a, role_map) for a in attrs],
        "matching": None if attribute else matching,
    }


def _part(part: dict) -> str:
    if part["transform"] == "prefix":
        return f"prefix({part['attr']}, {part['n']})"
    if part["transform"] == "year":
        return f"year({part['attr']})"
    return part["attr"]


def _check_desc(t: dict, chk: dict) -> str:
    value = chk["value"]
    if chk["rule"] == "in_set":
        value = "{" + ", ".join(value) + "}"
    return t[f"rule_{chk['rule']}"].format(value=value)


def render(data: dict, lang: str = "fr") -> str:
    t, types = TEXT[lang], TYPE_NAMES[lang]
    descriptions = normalizers.DESCRIPTIONS_FR if lang == "fr" else normalizers.DESCRIPTIONS_EN
    lines: list[str] = []
    if data["matching"] is not None:
        lines.append(
            t["header"].format(
                name=data["referential"],
                version=data["version"],
                n_src=len(data["sources"]),
                n_attr=len(data["attributes"]),
            )
        )
        for s in data["sources"]:
            lines.append(t["source"].format(**{**s, "revision": s["revision"] or t["no_revision"]}))
        lines.append("")
    for a in data["attributes"]:
        state = t["required"] if not a["nullable"] else t["optional"]
        lines.append(f"■ {a['name']} : {types[a['type']]}, {state}")
        if a["normalize"]:
            steps = " → ".join(f"{n} ({descriptions[n]})" for n in a["normalize"])
            lines.append(t["normalize"].format(steps=steps))
        else:
            lines.append(t["no_normalize"])
        header = "sources_priority" if a["strategy"] == "priority" else "sources_recent"
        lines.append(t[header])
        lines += [t["source_line"].format(**s) for s in a["sources"]]
        lines.append(t[a["strategy"]])
        lines.append(t["null_ok"] if a["nullable"] else t["null_blocked"])
        if a["checks"]:
            lines.append(t["checks"])
            for c in a["checks"]:
                desc = _check_desc(t, c)
                lines.append(
                    t["check_line"].format(desc=desc, on_fail=c["on_fail"], effect=t[c["on_fail"]])
                )
        else:
            lines.append(t["no_checks"])
        if a["matching_roles"]:
            parts = []
            for role in a["matching_roles"]:
                kind, _, name = role.partition(":")
                parts.append(t[f"role_{kind}"].format(name=name))
            lines.append(t["roles"].format(roles=", ".join(parts)))
        else:
            lines.append(t["no_roles"])
        lines.append("")
    m = data["matching"]
    if m is not None:
        lines.append(t["matching"].format(version=m["version"]))
        lines.append(t["blocking"].format(size=m["max_block_size"]))
        for k in m["blocking"]:
            parts = " + ".join(_part(p) for p in k["parts"])
            lines.append(t["block_line"].format(name=k["name"], parts=parts))
        lines.append(t["comparators"])
        for c in m["comparators"]:
            lines.append(t["comp_line"].format(**c))
        lines.append(
            t["thresholds"].format(
                accept=m["accept"], review=m["review"], min_evidence=m["min_evidence"]
            )
        )
        if m["contradictions"]:
            lines.append(t["contradictions"].format(attrs=", ".join(m["contradictions"])))
        else:
            lines.append(t["no_contradictions"])
    return "\n".join(lines).rstrip() + "\n"
