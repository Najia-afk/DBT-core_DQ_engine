"""Normaliseurs : définition Python (explain, lint) et traduction SQL (check-source).

Le contrat ne fixe que la liste des noms ; les définitions exactes ci-dessous sont celles de
l'outillage CLI (ASCII pour les espaces et les chiffres, Unicode pour alnum).
"""

from __future__ import annotations

import re
import unicodedata

WS = " \t\n\r"

DESCRIPTIONS_FR = {
    "trim": "supprime les espaces en début et fin",
    "lower": "met en minuscules",
    "upper": "met en majuscules",
    "strip_accents": "retire les accents, é devient e",
    "collapse_spaces": "remplace toute suite d'espaces par un seul espace",
    "digits_only": "ne garde que les chiffres 0-9",
    "alnum_only": "ne garde que les lettres et chiffres",
}
DESCRIPTIONS_EN = {
    "trim": "strips leading/trailing whitespace",
    "lower": "lower-cases",
    "upper": "upper-cases",
    "strip_accents": "removes accents, é becomes e",
    "collapse_spaces": "replaces any run of whitespace by a single space",
    "digits_only": "keeps digits 0-9 only",
    "alnum_only": "keeps letters and digits only",
}


def strip_accents(value: str) -> str:
    decomposed = unicodedata.normalize("NFKD", value)
    return "".join(c for c in decomposed if not unicodedata.combining(c))


def apply_one(name: str, value: str) -> str:
    if name == "trim":
        return value.strip(WS)
    if name == "lower":
        return value.lower()
    if name == "upper":
        return value.upper()
    if name == "strip_accents":
        return strip_accents(value)
    if name == "collapse_spaces":
        return re.sub(r"\s+", " ", value, flags=re.ASCII)
    if name == "digits_only":
        return re.sub(r"[^0-9]", "", value)
    if name == "alnum_only":
        return "".join(c for c in value if c.isalnum())
    raise ValueError(f"unknown normalizer {name!r}")


def apply(names: list[str], value: str) -> str:
    for name in names:
        value = apply_one(name, value)
    return value


def _accent_table() -> tuple[str, str]:
    src, dst = [], []
    for code in range(0xC0, 0x250):
        char = chr(code)
        base = strip_accents(char)
        if base != char and base.isascii() and len(base) == 1:
            src.append(char)
            dst.append(base)
    return "".join(src), "".join(dst)


ACCENTS_FROM, ACCENTS_TO = _accent_table()
