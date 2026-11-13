"""Pools de chaînes précalculés (mots, variantes bruitées) : toute la génération est du `take`."""

from __future__ import annotations

import unicodedata
from dataclasses import dataclass
from functools import lru_cache

import numpy as np
import pyarrow as pa

CONS = list("bcdfghjklmnprstvz")
VOW = list("aeiou")
ACCENTED = {"a": "à", "e": "é", "i": "ï", "o": "ô", "u": "ü"}
N_VARIANTS = 8  # 0 = forme canonique, 1..7 = bruits
DOMAINS = [
    "example.com",
    "mail.example.org",
    "corp.example.net",
    "post.example.fr",
    "web.example.be",
    "inbox.example.de",
    "demo.example.es",
    "test.example.it",
]


@dataclass(frozen=True)
class NamePool:
    size: int
    variants: int
    arr: pa.Array  # taille size * variants ; indice = mot * variants + variante


def _words(rng: np.random.Generator, size: int, accents: bool) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    while len(out) < size:
        n_syl = int(rng.integers(2, 5))
        parts = [
            CONS[int(rng.integers(len(CONS)))] + VOW[int(rng.integers(5))] for _ in range(n_syl)
        ]
        if rng.random() < 0.4:
            parts.append(CONS[int(rng.integers(len(CONS)))])
        word = "".join(parts)
        if word in seen:
            continue
        seen.add(word)
        if accents and rng.random() < 0.25:
            vowels = [i for i, ch in enumerate(word) if ch in ACCENTED]
            i = vowels[int(rng.integers(len(vowels)))]
            word = word[:i] + ACCENTED[word[i]] + word[i + 1 :]
        out.append(word)
    return out


def strip_accents(text: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", text) if not unicodedata.combining(c))


def _variants(word: str) -> list[str]:
    cap = word[0].upper() + word[1:]
    plain = strip_accents(cap)
    mid = len(cap) // 2
    swap = cap[0] + cap[2] + cap[1] + cap[3:]
    sub = cap[0] + ("q" if cap[1] != "q" else "x") + cap[2:]
    forms = [
        cap,
        cap.upper(),
        cap.lower(),
        plain if plain != cap else cap[0] + cap[2:],  # sans accent, sinon suppression
        cap[:mid] + cap[mid + 1 :],  # suppression
        sub,  # substitution
        swap if swap != cap else cap + "x",  # transposition
        f"  {cap} ",  # espaces parasites
    ]
    return [f if f != cap or i == 0 else cap + "x" for i, f in enumerate(forms)]


@lru_cache(maxsize=16)
def name_pool(kind: str, size: int, seed: int) -> NamePool:
    salt = {"last_name": 1, "first_name": 2}[kind]
    rng = np.random.default_rng([seed, 0x5EED, salt])
    flat: list[str] = []
    for w in _words(rng, size, accents=True):
        flat.extend(_variants(w))
    return NamePool(size=size, variants=N_VARIANTS, arr=pa.array(flat, type=pa.string()))


@lru_cache(maxsize=4)
def generic_pool(seed: int, size: int = 5000) -> pa.Array:
    rng = np.random.default_rng([seed, 0x5EED, 3])
    return pa.array([w.capitalize() for w in _words(rng, size, accents=False)], pa.string())


@lru_cache(maxsize=1)
def letters_pool() -> pa.Array:
    letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    return pa.array([a + b for a in letters for b in letters], pa.string())


@lru_cache(maxsize=1)
def digits_pool() -> pa.Array:
    return pa.array([f"{i:04d}" for i in range(10_000)], pa.string())


@lru_cache(maxsize=1)
def domains_pool() -> pa.Array:
    return pa.array(DOMAINS, pa.string())
