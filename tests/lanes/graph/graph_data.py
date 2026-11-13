"""Deterministic small datasets for the graph lane tests (hand-built customer-like data)."""

from __future__ import annotations

import random
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any

SURNAMES = [
    "Dupont",
    "Martin",
    "Bernard",
    "Durand",
    "Lefebvre",
    "Moreau",
    "Laurent",
    "Simon",
    "Michel",
    "Garcia",
    "Roux",
    "Fournier",
    "Girard",
    "Andre",
    "Mercier",
    "Blanc",
    "Guerin",
    "Muller",
    "Henry",
    "Perrin",
    "Morin",
    "Clement",
    "Gauthier",
    "Dumas",
    "Fontaine",
    "Chevalier",
    "Robin",
    "Masson",
    "Sanchez",
    "Gerard",
    "Nicolas",
    "Lemaire",
    "Dubois",
    "Lambert",
    "Bonnet",
    "Francois",
    "Martinez",
    "Legrand",
    "Garnier",
    "Faure",
    "Rousseau",
    "Blanchard",
    "Guillaume",
    "Muñoz",
    "Éloï",
    "Núñez",
    "O'Neil",
    "Zoë",
]
FIRSTS = [
    "Jean",
    "Marie",
    "Pierre",
    "Anne",
    "Luc",
    "Sophie",
    "Paul",
    "Julie",
    "Marc",
    "Claire",
    "Éric",
    "Hélène",
    "Zoé",
    "Noël",
    "Anaïs",
]
COUNTRIES = ["FR", "BE", "DE", "ES", "IT", "GB", "US"]
EPOCH = datetime(2024, 1, 1)


def typo(rng: random.Random, text: str) -> str:
    kind = rng.randrange(6)
    if kind == 0 and len(text) > 3:
        i = rng.randrange(len(text) - 1)
        return text[:i] + text[i + 1] + text[i] + text[i + 2 :]
    if kind == 1:
        return text.upper()
    if kind == 2:
        return f"  {text}  "
    if kind == 3:
        return text.replace("e", "é", 1) if "e" in text else text + "e"
    if kind == 4 and len(text) > 3:
        return text[:-1]
    return text.lower()


class Customers:
    """Entities observed by crm (full), erp (full) and web (delta); mutated day after day."""

    def __init__(self, seed: int, n: int = 220) -> None:
        self.rng = random.Random(seed)
        self.clock = 0
        self.entities: dict[int, dict[str, Any]] = {}
        self.rows: dict[str, dict[Any, dict[str, Any]]] = {"crm": {}, "erp": {}, "web": {}}
        self.next_key = {"crm": 1000, "erp": 5000, "web": 9000}
        for i in range(n):
            self.new_entity(i)
        for i in range(n):
            for src, p in (("crm", 0.8), ("erp", 0.6), ("web", 0.5)):
                if self.rng.random() < p:
                    self.observe(src, i)

    def stamp(self) -> datetime:
        self.clock += 1
        return EPOCH + timedelta(minutes=self.clock)

    def new_entity(self, i: int) -> None:
        r = self.rng
        last, first = r.choice(SURNAMES), r.choice(FIRSTS)
        self.entities[i] = {
            "last": last,
            "first": first,
            "birth": date(1950 + r.randrange(50), 1 + r.randrange(12), 1 + r.randrange(28)),
            "email": f"{first}.{last}.{i}@mail.com".lower().replace("'", "").replace("é", "e"),
            "country": r.choice(COUNTRIES),
            "limit": Decimal(r.randrange(0, 90000)) / 4,
        }

    def observe(self, src: str, i: int, noisy: float = 0.25) -> Any:
        r, e = self.rng, self.entities[i]
        key = self.next_key[src]
        self.next_key[src] += 1
        last = typo(r, e["last"]) if r.random() < noisy else e["last"]
        first = typo(r, e["first"]) if r.random() < noisy else e["first"]
        email = e["email"] if r.random() > 0.05 else r.choice(["broken", "a@b", "@x.fr", ""])
        birth = (
            e["birth"] if r.random() > 0.03 else e["birth"] + timedelta(days=r.randrange(1, 400))
        )
        row = self.build(src, key, last, first, birth, email, e["country"], e["limit"])
        self.rows[src][key] = row
        return key

    def build(self, src, key, last, first, birth, email, country, limit) -> dict[str, Any]:
        r = self.rng
        if r.random() < 0.02:
            last = None
        if r.random() < 0.02:
            first = None
        if r.random() < 0.01:
            last = "x" * 130
        if src == "crm":
            return {
                "customer_id": key,
                "LASTNAME": last,
                "FIRSTNAME": first,
                "BIRTHDATE": birth,
                "EMAIL": email,
                "COUNTRY": f" {country.lower()} " if r.random() < 0.1 else country,
                "updated_at": self.stamp(),
            }
        if src == "erp":
            neg = -limit - 1 if r.random() < 0.01 else limit
            return {
                "client_no": f"C{key:05d}",
                "nom": last,
                "prenom": first,
                "date_naissance": birth,
                "pays": country,
                "plafond": neg,
                "modified_at": self.stamp(),
            }
        return {
            "account_id": key,
            "surname": last,
            "given_name": first,
            "email": email,
            "event_ts": self.stamp(),
        }

    # ---------------------------------------------------------------- evolution

    def day(self, update: float = 0.08, delete: float = 0.04, new: float = 0.05) -> dict[str, Any]:
        """Mutate the sources; returns {source: (rows, replace)} to load (web = delta only)."""
        r = self.rng
        changed_web: list[Any] = []
        for src in ("crm", "erp", "web"):
            keys = sorted(self.rows[src], key=str)
            for key in keys:
                x = r.random()
                row = self.rows[src][key]
                if x < update:
                    self.mutate(src, row)
                    if src == "web":
                        changed_web.append(key)
                elif x < update + delete and src != "web":
                    del self.rows[src][key]
            for _ in range(max(1, int(len(keys) * new))):
                ent = r.randrange(len(self.entities) + 5)
                if ent >= len(self.entities):
                    self.new_entity(ent)
                key = self.observe(src, ent)
                if src == "web":
                    changed_web.append(key)
        out: dict[str, Any] = {}
        for src in ("crm", "erp"):
            rows = list(self.rows[src].values())
            if rows and r.random() < 0.9:
                dup = dict(r.choice(rows))
                dup["updated_at" if src == "crm" else "modified_at"] = self.stamp()
                rows.append(dup)  # same key twice in the batch: latest revision wins
            out[src] = (rows, True)
        out["web"] = ([self.rows["web"][k] for k in changed_web if k in self.rows["web"]], False)
        return out

    def mutate(self, src: str, row: dict[str, Any]) -> None:
        r = self.rng
        fields = {
            "crm": ("LASTNAME", "FIRSTNAME", "EMAIL", "BIRTHDATE", "updated_at"),
            "erp": ("nom", "prenom", "pays", "date_naissance", "modified_at"),
            "web": ("surname", "given_name", "email", "event_ts"),
        }[src]
        *data, rev = fields
        name = r.choice(data)
        if name in ("LASTNAME", "nom", "surname"):
            row[name] = r.choice(SURNAMES)  # identity change: merges / splits
        elif name in ("FIRSTNAME", "prenom", "given_name"):
            row[name] = r.choice(FIRSTS)
        elif name in ("BIRTHDATE", "date_naissance"):
            row[name] = date(1950 + r.randrange(50), 1 + r.randrange(12), 1 + r.randrange(28))
        elif name in ("pays",):
            row[name] = r.choice(COUNTRIES)
        else:
            row[name] = f"changed{r.randrange(1000)}@mail.com"
        row[rev] = self.stamp()

    def initial(self) -> dict[str, Any]:
        return {
            "crm": (list(self.rows["crm"].values()), True),
            "erp": (list(self.rows["erp"].values()), True),
            "web": (list(self.rows["web"].values()), False),
        }
