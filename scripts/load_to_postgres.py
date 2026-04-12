"""
Initialisation et chargement de la base PostgreSQL LogiStore.

Usage :
    python scripts/load_to_postgres.py --init         # Créer les tables
    python scripts/load_to_postgres.py --load-catalogue data/inbox/catalogue/catalogue_small.csv
    python scripts/load_to_postgres.py --load-movements data/inbox/movements/movements_small.csv
"""
import argparse
import os
import sys
from pathlib import Path

import psycopg2

import pandas as pd
from pydantic import TypeAdapter, ValidationError
from psycopg2.extras import execute_values


ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from contracts.catalogue_contract import get_catalogue_contract  # noqa: E402
from contracts.movement_contract import MovementRecordV1  # noqa: E402

DSN = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
    "dbname": os.getenv("POSTGRES_DB", "logistore"),
    "user": os.getenv("POSTGRES_USER", "logistore"),
    "password": os.getenv("POSTGRES_PASSWORD", "logistore"),
}

CREATE_PRODUCTS = """
CREATE TABLE IF NOT EXISTS products (
    sku         VARCHAR(20)  PRIMARY KEY,
    label       VARCHAR(200) NOT NULL,
    category    VARCHAR(20)  NOT NULL,
    unit        VARCHAR(10)  NOT NULL,
    min_stock   INTEGER      NOT NULL DEFAULT 0,
    supplier_id VARCHAR(50),
    published_at TIMESTAMP   NOT NULL,
    inserted_at  TIMESTAMP   DEFAULT NOW()
);
"""

CREATE_MOVEMENTS = """
CREATE TABLE IF NOT EXISTS movements (
    movement_id   VARCHAR(36)  PRIMARY KEY,
    sku           VARCHAR(20)  NOT NULL REFERENCES products(sku),
    movement_type VARCHAR(10)  NOT NULL,
    quantity      INTEGER      NOT NULL,
    reason        TEXT,
    occurred_at   TIMESTAMP    NOT NULL,
    inserted_at   TIMESTAMP    DEFAULT NOW()
);
"""

CREATE_REJECTED_MOVEMENTS = """
CREATE TABLE IF NOT EXISTS rejected_movements (
    id              SERIAL       PRIMARY KEY,
    movement_id     VARCHAR(36),
    sku             VARCHAR(20)  NOT NULL,
    movement_type   VARCHAR(10),
    quantity        INTEGER,
    reason          TEXT,
    occurred_at     TIMESTAMP,
    rejection_reason TEXT        NOT NULL,
    rejected_at     TIMESTAMP    DEFAULT NOW(),
    status          VARCHAR(20)  DEFAULT 'PENDING'  -- PENDING | REPLAYED | ABANDONED
);
"""


def get_conn():
    return psycopg2.connect(**DSN)


def init_db():
    """Crée les tables si elles n'existent pas."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_PRODUCTS)
            cur.execute(CREATE_MOVEMENTS)
            cur.execute(CREATE_REJECTED_MOVEMENTS)
        conn.commit()
    print("✅ Tables créées (ou déjà existantes) : products, movements, rejected_movements")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--init", action="store_true", help="Créer les tables")
    parser.add_argument("--load-catalogue", metavar="FILE", help="Charger un CSV catalogue")
    parser.add_argument("--load-movements", metavar="FILE", help="Charger un CSV mouvements")
    args = parser.parse_args()

    if args.init:
        init_db()

    if args.load_catalogue:
        df = pd.read_csv(args.load_catalogue, dtype={"schema_version": "string"})
        if "supplier_id" not in df.columns:
            df["supplier_id"] = None
        values = []
        rejected = 0
        for _, group in df.groupby("schema_version", sort=False):
            records = group.where(pd.notna(group), None).to_dict(orient="records")
            version = str(records[0]["schema_version"])
            try:
                contract = get_catalogue_contract(version)
                validated_rows = TypeAdapter(list[contract]).validate_python(records)
                for row in validated_rows:
                    values.append((
                        row.sku,
                        row.label,
                        row.category.value,
                        row.unit.value,
                        row.min_stock,
                        getattr(row, "supplier_id", None),
                        row.published_at,
                    ))
                continue
            except ValueError:
                pass
            except ValidationError:
                pass

            # Fallback ligne par ligne uniquement si le lot échoue
            for row_idx, raw in group.iterrows():
                raw_record = raw.where(pd.notna(raw), None).to_dict()
                try:
                    contract = get_catalogue_contract(str(raw_record.get("schema_version")))
                    parsed = contract.model_validate(raw_record)
                except (ValueError, ValidationError) as exc:
                    rejected += 1
                    print(f"❌ Ligne {row_idx + 2} rejetée: {exc}")
                    continue

                values.append((
                    parsed.sku,
                    parsed.label,
                    parsed.category.value,
                    parsed.unit.value,
                    parsed.min_stock,
                    getattr(parsed, "supplier_id", None),
                    parsed.published_at,
                ))

        if values:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    execute_values(
                        cur,
                        """
                        INSERT INTO products (
                            sku, label, category, unit, min_stock, supplier_id, published_at
                        )
                        VALUES %s
                        ON CONFLICT (sku) DO UPDATE
                        SET
                            label = EXCLUDED.label,
                            category = EXCLUDED.category,
                            unit = EXCLUDED.unit,
                            min_stock = EXCLUDED.min_stock,
                            supplier_id = EXCLUDED.supplier_id,
                            published_at = EXCLUDED.published_at
                        """,
                        values,
                        page_size=5000,
                    )
                conn.commit()

        print(f"✅ Catalogue chargé: {len(values)} lignes valides / {rejected} rejetées")

    if args.load_movements:
        df = pd.read_csv(args.load_movements, dtype={"schema_version": "string"})
        valid_values = []
        invalid_rows = 0

        for _, group in df.groupby("schema_version", sort=False):
            records = group.where(pd.notna(group), None).to_dict(orient="records")
            version = str(records[0].get("schema_version"))

            if version == "1.0":
                try:
                    validated_rows = TypeAdapter(list[MovementRecordV1]).validate_python(records)
                    valid_values.extend([
                        (
                            row.movement_id,
                            row.sku,
                            row.movement_type.value,
                            row.quantity,
                            row.reason,
                            row.occurred_at,
                        )
                        for row in validated_rows
                    ])
                    continue
                except ValidationError:
                    pass

            for row_idx, raw in group.iterrows():
                raw_record = raw.where(pd.notna(raw), None).to_dict()
                try:
                    parsed = MovementRecordV1.model_validate(raw_record)
                    valid_values.append((
                        parsed.movement_id,
                        parsed.sku,
                        parsed.movement_type.value,
                        parsed.quantity,
                        parsed.reason,
                        parsed.occurred_at,
                    ))
                except ValidationError as exc:
                    invalid_rows += 1
                    print(f"❌ Ligne {row_idx + 2} rejetée (schéma): {exc}")

        inserted = 0
        rejected_unknown_sku = 0

        if valid_values:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        CREATE TEMP TABLE staging_movements (
                            movement_id   VARCHAR(36),
                            sku           VARCHAR(20),
                            movement_type VARCHAR(10),
                            quantity      INTEGER,
                            reason        TEXT,
                            occurred_at   TIMESTAMP
                        ) ON COMMIT DROP
                        """
                    )

                    execute_values(
                        cur,
                        """
                        INSERT INTO staging_movements (
                            movement_id, sku, movement_type, quantity, reason, occurred_at
                        )
                        VALUES %s
                        """,
                        valid_values,
                        page_size=10000,
                    )

                    cur.execute(
                        """
                        INSERT INTO movements (
                            movement_id, sku, movement_type, quantity, reason, occurred_at
                        )
                        SELECT
                            s.movement_id, s.sku, s.movement_type, s.quantity, s.reason, s.occurred_at
                        FROM staging_movements s
                        JOIN products p ON p.sku = s.sku
                        ON CONFLICT (movement_id) DO NOTHING
                        """
                    )
                    inserted = cur.rowcount

                    cur.execute(
                        """
                        INSERT INTO rejected_movements (
                            movement_id, sku, movement_type, quantity, reason, occurred_at, rejection_reason
                        )
                        SELECT
                            s.movement_id, s.sku, s.movement_type, s.quantity, s.reason, s.occurred_at, 'unknown_sku'
                        FROM staging_movements s
                        LEFT JOIN products p ON p.sku = s.sku
                        WHERE p.sku IS NULL
                        """
                    )
                    rejected_unknown_sku = cur.rowcount
                conn.commit()

        print(
            f"✅ Mouvements chargés: {inserted} insérés / "
            f"{rejected_unknown_sku} rejetés (SKU inconnu) / "
            f"{invalid_rows} rejetés (schéma)"
        )


if __name__ == "__main__":
    main()
