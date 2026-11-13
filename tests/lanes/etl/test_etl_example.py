"""Example customer config (crm/erp postgres + web parquet) with hand-made dirty data.

Covers what the synthetic generator does not: accents, unbounded numeric, timestamptz, duplicate
keys across batches, null keys, DQ actions of the example, `most_recent`, several batches.
"""

from __future__ import annotations

import random
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import psycopg
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import yaml
from etl_support import Lab

from engine.core.canonical import check_tables

EXAMPLE = Path(__file__).resolve().parents[3] / "configs" / "example" / "customer.yml"
FIRST = [
    "Jean", "Marie", "\u00c9lo\u00efse", "Zo\u00eb", "H\u00e9l\u00e8ne", "J\u00f6rg",
    "S\u00f8ren", "Anne-Marie", "O'Neil", "Luc", "Lucie", "Emma",
]  # fmt: skip
LAST = ["Dupont", "Durand", "M\u00fcller", "Lef\u00e8vre", "Garc\u00eda", "Rossi", "Smith",
        "N'Diaye", "\u0141ukasz", "Van der Berg", "Martin", "Bernard"]  # fmt: skip


def noisy(rng: random.Random, text: str | None) -> str | None:
    if text is None:
        return None
    r = rng.random()
    if r < 0.15:
        return f"  {text.upper()} "
    if r < 0.25:
        return text.lower()
    if r < 0.30:
        return text.replace("\u00e9", "e").replace("\u00e8", "e").replace("\u00fc", "u")
    if r < 0.34 and len(text) > 3:
        i = rng.randrange(len(text))
        return text[:i] + text[i + 1 :]
    if r < 0.38:
        return text.replace(" ", "   ")
    return text


class World:
    """Persons observed by three sources; each day mutates the source rows."""

    def __init__(self, seed: int, persons: int, root: Path) -> None:
        self.rng = random.Random(seed)
        self.root = root
        self.day = 0
        self.next_key = 1
        self.crm: dict[int, dict[str, Any]] = {}
        self.erp: dict[str, dict[str, Any]] = {}
        self.web: dict[str, dict[str, Any]] = {}
        self.web_changed: list[dict[str, Any]] = []
        for _ in range(persons):
            self.add_person()

    def person(self) -> dict[str, Any]:
        r = self.rng
        email = f"{r.choice(FIRST)}.{r.choice(LAST)}{r.randrange(40)}@mail.test".lower().replace(
            " ", ""
        )
        return {
            "ln": r.choice(LAST),
            "fn": r.choice(FIRST),
            "bd": date(1960 + r.randrange(30), 1 + r.randrange(12), 1 + r.randrange(28)),
            "email": email if r.random() < 0.8 else None,
            "country": r.choice(["FR", "BE", "DE", "ES", "IT", "GB"]),
            "limit": Decimal(r.randrange(0, 500000)) / 100,
        }

    def stamp(self) -> datetime:
        return datetime(2025, 1, 1) + timedelta(days=self.day, seconds=self.rng.randrange(86400))

    def crm_row(self, p: dict[str, Any], key: int | None = None) -> dict[str, Any]:
        r = self.rng
        key = key or self.next_key
        self.next_key += 1
        return {
            "customer_id": key, "LASTNAME": noisy(r, p["ln"]), "FIRSTNAME": noisy(r, p["fn"]),
            "BIRTHDATE": p["bd"], "EMAIL": noisy(r, p["email"]), "COUNTRY": p["country"],
            "updated_at": self.stamp(),
        }  # fmt: skip

    def erp_row(self, p: dict[str, Any], key: str | None = None) -> dict[str, Any]:
        r = self.rng
        key = key or f"E{self.next_key}"
        self.next_key += 1
        limit = p["limit"] if r.random() > 0.05 else Decimal("0.12345")
        return {
            "client_no": key, "nom": noisy(r, p["ln"]), "prenom": noisy(r, p["fn"]),
            "date_naissance": p["bd"] if r.random() > 0.03 else p["bd"] + timedelta(days=400),
            "pays": noisy(r, p["country"]), "plafond": limit,
            "modified_at": self.stamp().replace(tzinfo=UTC),
        }  # fmt: skip

    def web_row(self, p: dict[str, Any], key: str | None = None) -> dict[str, Any]:
        r = self.rng
        key = key or f"W{self.next_key}"
        self.next_key += 1
        return {
            "account_id": key, "surname": noisy(r, p["ln"]), "given_name": noisy(r, p["fn"]),
            "email": noisy(r, p["email"]), "event_ts": self.stamp(),
        }  # fmt: skip

    def add_person(self) -> None:
        p = self.person()
        r = self.rng.random()
        if r < 0.7:
            row = self.crm_row(p)
            self.crm[row["customer_id"]] = row
        if r > 0.3:
            row = self.erp_row(p)
            self.erp[row["client_no"]] = row
        if 0.15 < r < 0.85:
            row = self.web_row(p)
            self.web[row["account_id"]] = row
            self.web_changed.append(row)

    def evolve(self) -> None:
        self.day += 1
        r = self.rng
        self.web_changed = []
        for rows, mutate in ((self.crm, "LASTNAME"), (self.erp, "nom"), (self.web, "surname")):
            keys = sorted(rows, key=str)
            for key in r.sample(keys, k=len(keys) // 12):
                row = rows[key]
                row[mutate] = noisy(r, r.choice(LAST))
                stamp_col = {"LASTNAME": "updated_at", "nom": "modified_at"}.get(mutate, "event_ts")
                row[stamp_col] = self.stamp().replace(tzinfo=UTC if mutate == "nom" else None)
                if rows is self.web:
                    self.web_changed.append(row)
            if rows is not self.web:
                for key in r.sample(keys, k=len(keys) // 25):
                    del rows[key]
        for _ in range(len(self.crm) // 20):
            self.add_person()
        # dirty values
        for row in r.sample(sorted(self.crm.values(), key=lambda x: x["customer_id"]), k=8):
            row["EMAIL"] = "not-an-email"
        for row in r.sample(sorted(self.erp.values(), key=lambda x: x["client_no"]), k=6):
            row["plafond"] = Decimal("-5")
        for row in r.sample(sorted(self.erp.values(), key=lambda x: x["client_no"]), k=4):
            row["pays"] = "ZZ"
        row = r.choice(sorted(self.crm.values(), key=lambda x: x["customer_id"]))
        row["LASTNAME"] = "x" * 130

    # ---- publication of the day to the sources
    def web_rows(self) -> list[dict[str, Any]]:
        rows = list(self.web_changed)
        if rows:  # duplicate keys (older revision first and last), a null key, an empty key
            first, last = rows[0], rows[-1]
            rows.append(
                {
                    **first,
                    "surname": "Old Version",
                    "event_ts": first["event_ts"] - timedelta(days=9),
                }
            )
            rows.insert(
                0,
                {
                    **last,
                    "surname": "Older Version",
                    "event_ts": last["event_ts"] - timedelta(days=3),
                },
            )
            rows.append({**first, "account_id": None})
            rows.append({**first, "account_id": ""})
        return rows

    def publish(self, dsn: str) -> None:
        with psycopg.connect(dsn, autocommit=True) as conn:
            conn.execute("DROP TABLE IF EXISTS public.crm_customers")
            conn.execute(
                'CREATE TABLE public.crm_customers (customer_id bigint, "LASTNAME" text,'
                ' "FIRSTNAME" text, "BIRTHDATE" date, "EMAIL" text, "COUNTRY" text,'
                " updated_at timestamp)"
            )
            conn.cursor().executemany(
                "INSERT INTO public.crm_customers VALUES (%s, %s, %s, %s, %s, %s, %s)",
                [list(r.values()) for r in self.crm.values()],
            )
            conn.execute("DROP TABLE IF EXISTS public.erp_clients")
            conn.execute(
                "CREATE TABLE public.erp_clients (client_no text, nom text, prenom text,"
                " date_naissance date, pays text, plafond numeric, modified_at timestamptz)"
            )
            conn.cursor().executemany(
                "INSERT INTO public.erp_clients VALUES (%s, %s, %s, %s, %s, %s, %s)",
                [list(r.values()) for r in self.erp.values()],
            )
        web_dir = self.root / "web"
        for old in web_dir.glob("*.parquet"):
            old.unlink()
        web_dir.mkdir(exist_ok=True)
        rows = self.web_rows()
        schema = pa.schema(
            [("account_id", pa.string()), ("surname", pa.string()), ("given_name", pa.string()),
             ("email", pa.string()), ("event_ts", pa.timestamp("us"))]
        )  # fmt: skip
        pq.write_table(pa.Table.from_pylist(rows, schema=schema), web_dir / "part-0.parquet")
        self.published = {
            "crm": list(self.crm.values()),
            "erp": list(self.erp.values()),
            "web": rows,
        }


def adapted_config(tmp: Path) -> Path:
    raw = yaml.safe_load(EXAMPLE.read_text(encoding="utf-8"))
    raw["state"]["schema"] = "mdm_customer_t"
    raw["runtime"]["batch_rows"] = 1000
    for source in raw["sources"]:
        if source["kind"] == "parquet":
            source["location"] = (tmp / "web" / "*.parquet").as_posix()
        source["full_snapshot_min_ratio"] = 0.7
    path = tmp / "customer.yml"
    path.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")
    return path


def test_example_customer_three_days(
    tmp_path: Path, pg_db: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    world = World(seed=7, persons=1700, root=tmp_path)
    adapted_config(tmp_path)
    lab = Lab(tmp_path, pg_db, monkeypatch, tmp_path / "out", "customer.yml")
    lab.lane("init")
    for day in range(3):
        if day:
            world.evolve()
        world.publish(pg_db)
        for source in ("crm", "erp", "web"):
            lab.lane("ingest", "--source", source, "--run-id", f"d{day}")
            lab.ref.ingest(source, world.published[source], f"d{day}")
        lab.lane("reconcile", "--run-id", f"d{day}")
        lab.ref.reconcile(f"d{day}")
        report = lab.compare()
        assert report.ok, f"day {day}: {report.format()}"
    tables = lab.export("final")
    assert check_tables(lab.cfg, tables) == []
    rules = {(r["rule"], r["action"]) for r in tables["reject"].to_pylist()}
    assert {("duplicate_key", "reject_record"), ("key_null", "reject_record"),
            ("regex", "null_value"), ("min", "reject_record"), ("in_set", "warn"),
            ("max_length", "reject_record")} <= rules  # fmt: skip
    assert lab.last["counts"]["tables"]["golden"] > 500
    assert lab.last["counts"]["tables"]["review"] > 0
