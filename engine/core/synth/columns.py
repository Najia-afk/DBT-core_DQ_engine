"""Génération vectorisée des colonnes : valeurs = fonctions pures de (seed, entité, attr, version).

Aucune boucle par ligne : hachage splitmix64 en numpy, puis `take` dans des pools Arrow.
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

from .params import SynthParams
from .plan import AttrSpec, Plan, SourceSpec
from .pools import DOMAINS, digits_pool, domains_pool, generic_pool, letters_pool, name_pool

U = np.uint64
MASK64 = (1 << 64) - 1
K1, K2, K3, K4, K5, K6 = (
    0xD6E8FEB86659FD93,
    0xA0761D6478BD642F,
    0xE7037ED1A0B428DB,
    0x8EBC6AF09C88C6E3,
    0x589965CC75374CC3,
    0x1D8E4E27C47D124F,
)
MASK21 = U((1 << 21) - 1)
PRECISION = 1 << 21
GENERIC_POOL = 5000


def u(value: int) -> np.uint64:
    return U(value & MASK64)


def mix(x: np.ndarray) -> np.ndarray:
    with np.errstate(over="ignore"):
        x = x + U(0x9E3779B97F4A7C15)
        x = (x ^ (x >> U(30))) * U(0xBF58476D1CE4E5B9)
        x = (x ^ (x >> U(27))) * U(0x94D049BB133111EB)
        return x ^ (x >> U(31))


def seed_salt(seed: int, tag: int) -> np.uint64:
    return mix(np.array([(seed * 0x100000001B3 + tag) & MASK64], dtype=np.uint64))[0]


def thr(rate: float) -> int:
    return int(round(rate * PRECISION))


def flag(h: np.ndarray, shift: int, threshold: Any) -> np.ndarray | None:
    if isinstance(threshold, int) and threshold <= 0:
        return None
    return ((h >> U(shift)) & MASK21) < threshold


def key_strings(src: SourceSpec, keys: np.ndarray) -> pa.Array:
    text = pc.cast(pa.array(keys, pa.int64()), pa.string())
    if not src.key_is_str:
        return text
    padded = pc.utf8_lpad(text, 9, "0")
    return pc.binary_join_element_wise(pa.scalar(src.key_prefix), padded, pa.scalar(""))


class Block:
    """Lignes d'une partie (arrays d'état) et hachages partagés par toutes les colonnes."""

    def __init__(self, plan: Plan, src: SourceSpec, rows: dict[str, np.ndarray], day: int):
        self.plan = plan
        self.p: SynthParams = plan.params
        self.src = src
        self.rows = rows
        self.key = rows["key"]
        self.n = len(self.key)
        seed = self.p.seed
        with np.errstate(over="ignore"):
            self.key_h = mix(self.key.astype(np.uint64) + seed_salt(seed, 100 + src.idx))
        ent_salt = seed_salt(seed, 1)
        self.ent_h = mix(rows["ent"].astype(np.uint64) + ent_salt)
        self.home_h = mix(rows["home"].astype(np.uint64) + ent_salt)
        self.dup = rows["dup"]
        self._ent_str: pa.Array | None = None
        self.nonident = plan.nonidentity
        self.pos = {a: i for i, a in enumerate(self.nonident)}
        self.thr_change = thr(self.p.attr_change_rate)
        self.upd_days: list[tuple[int, np.ndarray, np.ndarray]] = []
        upd = rows["upd"]
        for d in range(1, day + 1):
            sel = np.flatnonzero((upd >> U(d)) & U(1))
            if sel.size:
                forced = mix(self.key_h[sel] + u(d * K6)) % U(len(self.nonident))
                self.upd_days.append((d, sel, forced))

    @property
    def ent_str(self) -> pa.Array:
        if self._ent_str is None:
            self._ent_str = pc.cast(pa.array(self.rows["ent"], pa.int64()), pa.string())
        return self._ent_str

    def versions(self, a: AttrSpec) -> np.ndarray | None:
        """Nombre de mises à jour ayant touché cet attribut pour chaque ligne (None = jamais)."""
        if a.identity or not self.upd_days:
            return None
        ver = np.zeros(self.n, dtype=np.uint64)
        ca = u((a.idx + 1) * K4)
        for d, sel, forced in self.upd_days:
            hm = mix(self.key_h[sel] + ca + u(d * K5))
            chg = ((hm & MASK21) < self.thr_change) | (forced == U(self.pos[a.idx]))
            ver[sel] += chg.astype(np.uint64)
        return ver


def _take(pool: pa.Array, idx: np.ndarray, null: np.ndarray | None) -> pa.Array:
    return pool.take(pa.array(idx.astype(np.int64), mask=null))


def _idx(h: np.ndarray, modulus: int, off: np.ndarray | None, mult: int = 1) -> np.ndarray:
    base = h % U(modulus)
    if off is not None:
        base = (base + off * U(mult)) % U(modulus)
    return base.astype(np.int64)


def _decimal(cents: np.ndarray, null: np.ndarray | None) -> pa.Array:
    n = len(cents)
    raw = np.empty((n, 2), dtype=np.int64)
    raw[:, 0] = cents
    raw[:, 1] = cents >> 63
    validity = None
    nulls = 0
    if null is not None and null.any():
        validity = pa.py_buffer(np.packbits(~null, bitorder="little"))
        nulls = int(null.sum())
    return pa.Array.from_buffers(
        pa.decimal128(18, 2), n, [validity, pa.py_buffer(raw)], null_count=nulls
    )


def _violate_string(arr: pa.Array, a: AttrSpec, viol: np.ndarray, cidx: np.ndarray) -> pa.Array:
    for j, chk in enumerate(a.checks):
        m = viol & (cidx == j)
        if not m.any():
            continue
        if chk["rule"] == "max_length":
            text = "V" * (int(chk["value"]) + 5)
        elif chk["rule"] == "regex":
            text = "not-an-email" if a.kind == "email" else "bad-value"
        else:
            text = "ZZ"
        arr = pc.if_else(pa.array(m), pa.scalar(text), arr)
    return arr


def _violate_numeric(
    vals: np.ndarray, a: AttrSpec, viol: np.ndarray, cidx: np.ndarray, extra: np.ndarray, scale: int
) -> np.ndarray:
    for j, chk in enumerate(a.checks):
        m = viol & (cidx == j)
        if not m.any():
            continue
        bound = int(chk["value"]) * scale
        vals[m] = bound - 1 - extra[m] if chk["rule"] == "min" else bound + 1 + extra[m]
    return vals


def build_column(blk: Block, a: AttrSpec) -> tuple[pa.Array, np.ndarray | None, np.ndarray | None]:
    """Retourne (colonne, masque des valeurs violant un contrôle, indice du contrôle violé)."""
    p = blk.p
    bh = blk.ent_h if a.identity else blk.home_h
    h0 = mix(bh + u((a.idx + 1) * K1))
    ver = blk.versions(a)
    kh = blk.key_h + u((a.idx + 1) * K2)
    if ver is not None:
        kh = kh + ver * u(K3)
    hc = mix(kh)
    hc2 = mix(hc)

    null = flag(hc, 0, thr(a.null_rate)) if a.nullable else None
    viol = cidx = None
    if a.checks:
        viol = flag(hc, 42, thr(p.violation_rate))
        broken = blk.rows["broken"] == a.idx
        if broken.any():
            viol = broken if viol is None else viol | broken
            if null is not None:
                null = null & ~broken
        if viol is not None and null is not None:
            viol = viol & ~null
        if viol is not None:
            cidx = ((hc2 >> U(50)) % U(len(a.checks))).astype(np.int64)
            reject = [j for j, c in enumerate(a.checks) if c["on_fail"] == "reject_record"]
            if reject and broken.any():
                cidx[broken] = reject[0]
    extra = ((hc2 >> U(24)) % U(1000)).astype(np.int64)

    kind = a.kind
    conf = None
    if kind in ("birth_date", "email"):
        conf = flag(hc2, 0, thr(p.contradiction_rate))
    elif not a.identity:
        conf = flag(hc2, 0, thr(p.conflict_rate))
    off = None
    if ver is not None or conf is not None:
        off = ver if ver is not None else np.zeros(blk.n, dtype=np.uint64)
        if conf is not None and not a.identity:
            off = off + conf.astype(np.uint64)

    if kind in ("last_name", "first_name"):
        pool = name_pool(kind, p.name_pool, p.seed)
        noise_thr = np.where(blk.dup, thr(p.dup_noise_rate), thr(p.noise_rate)).astype(np.uint64)
        noise = flag(hc, 21, noise_thr)
        variant = np.where(noise, 1 + ((hc2 >> U(24)) % U(pool.variants - 1)).astype(np.int64), 0)
        word = (h0 % U(pool.size)).astype(np.int64)
        arr = _take(pool.arr, word * pool.variants + variant, null)
    elif kind == "email":
        gp = generic_pool(p.seed, GENERIC_POOL)
        loc_off = conf.astype(np.uint64) if conf is not None else None
        local = _take(gp, _idx(h0, GENERIC_POOL, loc_off), null)
        domain = _take(domains_pool(), _idx(h0 >> U(32), len(DOMAINS), None), None)
        arr = pc.binary_join_element_wise(
            local, pa.scalar("."), blk.ent_str, pa.scalar("@"), domain, pa.scalar("")
        )
        noise_thr = np.where(blk.dup, thr(p.dup_noise_rate), thr(p.noise_rate)).astype(np.uint64)
        noise = flag(hc, 21, noise_thr)
        if noise is not None and noise.any():
            arr = pc.if_else(pa.array(noise), pc.utf8_upper(arr), arr)
    elif kind == "birth_date":
        days = a.lo + (h0 % U(a.span)).astype(np.int64)
        if conf is not None:
            shift = 1 + ((hc2 >> U(24)) % U(400)).astype(np.int64)
            sign = np.where((hc2 >> U(23)) & U(1), 1, -1)
            days = days + np.where(conf, sign * shift, 0)
        arr = pa.array(days.astype(np.int32), type=pa.int32(), mask=null).cast(pa.date32())
    elif kind == "word":
        arr = _take(generic_pool(p.seed, GENERIC_POOL), _idx(h0, GENERIC_POOL, off), null)
    elif kind == "phrase":
        gp = generic_pool(p.seed, GENERIC_POOL)
        first = _take(gp, _idx(h0, GENERIC_POOL, off), null)
        second = _take(gp, _idx(h0 >> U(24), GENERIC_POOL, off, 3), None)
        arr = pc.binary_join_element_wise(first, second, pa.scalar(" "))
    elif kind == "code":
        letters = _take(letters_pool(), _idx(h0, 676, off), null)
        digits = _take(digits_pool(), _idx(h0 >> U(24), 10_000, off, 7), None)
        arr = pc.binary_join_element_wise(letters, digits, pa.scalar("-"))
    elif kind in ("category", "country"):
        values = pa.array(list(a.values), pa.string())
        arr = _take(values, _idx(h0, len(a.values), off), null)
    elif kind == "int":
        vals = a.lo + _idx(h0, a.span, off)
        if viol is not None:
            vals = _violate_numeric(vals, a, viol, cidx, extra, 1)
        return pa.array(vals, type=pa.int64(), mask=null), viol, cidx
    elif kind == "decimal":
        cents = _idx(h0, a.span * 100, off, 7919)
        if viol is not None:
            cents = _violate_numeric(cents, a, viol, cidx, extra, 100)
        return _decimal(cents, null), viol, cidx
    elif kind == "date":
        days = a.lo + _idx(h0, a.span, off, 13)
        arr = pa.array(days.astype(np.int32), type=pa.int32(), mask=null).cast(pa.date32())
    elif kind == "timestamp":
        secs = a.lo + _idx(h0, a.span, off, 17)
        arr = pa.array(secs * 1_000_000, type=pa.int64(), mask=null).cast(pa.timestamp("us"))
    elif kind == "bool":
        bits = ((h0 >> U(7)) + (off if off is not None else U(0))) & U(1)
        arr = pa.array(bits.astype(bool), type=pa.bool_(), mask=null)
    else:
        raise ValueError(f"unknown attribute kind {kind}")

    if a.type == "string" and viol is not None:
        arr = _violate_string(arr, a, viol, cidx)
    return arr, viol, cidx
