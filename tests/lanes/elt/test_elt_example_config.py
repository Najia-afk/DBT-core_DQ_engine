"""Config d'exemple (configs/example/customer.yml) adaptée au banc de test : sources PostgreSQL
(noms de colonnes en MAJUSCULES, types natifs) + Parquet, trois jours, comparés à la référence."""

from __future__ import annotations

import random
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import psycopg
from elt_helpers import RowsHarness, write_rows
from psycopg.rows import dict_row

from engine.core.config import ReferentialConfig, load_config

EXAMPLE = Path("configs/example/customer.yml")
NAMES = ["Dupont", "Durand", "Martin", "Bernard", "Lefèvre", "Müller", "O'Brien", "ÉMILE", "Nuñez"]
FIRST = ["Jean", "Marie", "Léa", "José", "Zoë", "Anne-Claire", None]
COUNTRIES = ["FR", "BE", "DE", "ES", "IT", "GB", "US", " fr ", None]
DDL = [
    "CREATE TABLE public.crm_customers (customer_id bigint PRIMARY KEY, updated_at timestamp,"
    ' "LASTNAME" text, "FIRSTNAME" text, "BIRTHDATE" date, "EMAIL" text, "COUNTRY" text)',
    "CREATE TABLE public.erp_clients (client_no text PRIMARY KEY, modified_at timestamp,"
    " nom text, prenom text, date_naissance date, pays text, plafond numeric(12, 2))",
]


def adapted_config(web_dir: Path) -> ReferentialConfig:
    raw = load_config(EXAMPLE).model_dump(by_alias=True, mode="json", exclude_none=True)
    raw["state"]["schema"] = "mdm_customer_example"
    for src in raw["sources"]:
        if src["id"] == "web":
            src["location"] = (web_dir / "*.parquet").as_posix()
    return ReferentialConfig.model_validate(raw)


class Person:
    def __init__(self, rng: random.Random) -> None:
        self.rng = rng
        self.last = rng.choice(NAMES)
        self.first = rng.choice(FIRST)
        self.birth = date(1950, 1, 1) + timedelta(days=rng.randrange(0, 30000, 37))
        self.email = f"{self.first or 'x'}.{self.last}@mail.fr".lower().replace("'", "")
        self.country = rng.choice(COUNTRIES)

    def crm(self, key: int, stamp: datetime) -> dict:
        r = self.rng
        return {
            "customer_id": key,
            "updated_at": stamp,
            "LASTNAME": self.last if r.random() < 0.9 else "x" * 130,
            "FIRSTNAME": self.first,
            "BIRTHDATE": self.birth,
            "EMAIL": self.email if r.random() < 0.85 else "not-an-email",
            "COUNTRY": self.country,
        }

    def erp(self, key: str, stamp: datetime) -> dict:
        r = self.rng
        return {
            "client_no": key,
            "modified_at": stamp,
            "nom": self.last.upper(),
            "prenom": self.first,
            "date_naissance": self.birth,
            "pays": self.country,
            "plafond": r.choice([Decimal("500.00"), Decimal("0"), Decimal("-3.50"), None]),
        }

    def web(self, key: int, stamp: datetime) -> dict:
        return {
            "account_id": key,
            "event_ts": stamp,
            "surname": self.last,
            "given_name": self.first,
            "email": self.email.upper(),
        }


def fill(conn: psycopg.Connection, table: str, rows: list[dict]) -> None:
    conn.execute(f"TRUNCATE {table}")
    cols = list(rows[0])
    names = ", ".join(f'"{c}"' for c in cols)
    marks = ", ".join(["%s"] * len(cols))
    with conn.cursor() as cur:
        cur.executemany(
            f"INSERT INTO {table} ({names}) VALUES ({marks})",
            [[r[c] for c in cols] for r in rows],
        )
    conn.commit()


def fetch(conn: psycopg.Connection, table: str) -> list[dict]:
    with conn.cursor(row_factory=dict_row) as cur:
        return list(cur.execute(f"SELECT * FROM {table}"))


async def test_example_customer_config_matches_reference(elt_env, pg_dsn, work_dir):
    web_dir = work_dir / "web"
    cfg = adapted_config(web_dir)
    rng = random.Random(21)
    people = [Person(rng) for _ in range(45)]
    crm_rows = {i: people[i].crm(1000 + i, datetime(2024, 1, 1, 8)) for i in range(0, 40)}
    erp_rows = {i: people[i].erp(f"C{i:04d}", datetime(2024, 1, 2, 9)) for i in range(10, 45)}
    web_rows = [people[i].web(5000 + i, datetime(2024, 1, 3, 10)) for i in range(20, 45)]
    with psycopg.connect(pg_dsn) as conn:
        for statement in DDL:
            conn.execute(statement)
        conn.commit()
    h = RowsHarness(cfg, work_dir / "unused")
    columns = h.columns["web"]

    async def day(run: str) -> None:
        with psycopg.connect(pg_dsn) as conn:
            fill(conn, "public.crm_customers", list(crm_rows.values()))
            fill(conn, "public.erp_clients", list(erp_rows.values()))
            crm_in, erp_in = fetch(conn, "public.crm_customers"), fetch(conn, "public.erp_clients")
        write_rows(web_dir, web_rows, columns)
        ids = ["crm", "erp", "web"]
        out = await h.lane.ingest(ids, run)
        for sid, rows in zip(ids, (crm_in, erp_in, web_rows), strict=True):
            r = h.ref.ingest(sid, rows, run)
            c = out["counters"][sid]
            assert (c["inserted"], c["updated"], c["deleted"], c["rejected"], c["blocked"]) == (
                r.inserted,
                r.updated,
                r.deleted,
                r.rejected,
                r.blocked,
            ), (run, sid)
        rec = await h.lane.reconcile(run)
        want = h.ref.reconcile(run)
        assert rec["counters"]["golden_changes"] == want.golden_changes
        assert rec["counters"]["merges"] == want.merges and rec["counters"]["splits"] == want.splits
        h.assert_equivalent()

    await day("d0")
    exported = h.export()
    assert exported["golden"].num_rows > 20
    assert {"reject_record", "null_value", "warn"} >= {
        r["action"] for r in exported["reject"].to_pylist()
    }

    # jour 1 : mises à jour (nom, pays, plafond), retraits (snapshot complet > 90 %), nouveaux nœuds
    for i in (3, 7, 12):
        crm_rows[i] = people[i].crm(1000 + i, datetime(2024, 2, 1, 8))
        crm_rows[i]["LASTNAME"] = "Changé-" + str(i)
    for i in (11, 13):
        del erp_rows[i]
    erp_rows[44]["pays"] = "BE"
    erp_rows[44]["modified_at"] = datetime(2024, 2, 1, 9)
    extra = Person(rng)
    crm_rows[99] = extra.crm(1099, datetime(2024, 2, 1, 8))
    erp_rows[99] = extra.erp("C0099", datetime(2024, 2, 1, 9))
    web_rows = [extra.web(5099, datetime(2024, 2, 1, 10))]
    await day("d1")

    # jour 2 : réapparition d'un retiré, fusion de deux personnes (même identité saisie deux fois)
    erp_rows[11] = people[11].erp("C0011", datetime(2024, 3, 1, 9))
    twin = people[5]
    crm_rows[50] = twin.crm(1050, datetime(2024, 3, 1, 8))
    web_rows = [twin.web(5050, datetime(2024, 3, 1, 10))]
    await day("d2")
    final = h.export()
    assert final["staging_event"].num_rows > 0
    assert "retired" in {r["status"] for r in final["source_record"].to_pylist()}
    assert "golden_blocked" in {r["kind"] for r in final["anomaly"].to_pylist()}
