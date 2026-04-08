"""
DAG 4 — Rejeu des mouvements rejetés

Déclenché MANUELLEMENT par un opérateur, après :
1. Vérification que de nouveaux produits ont été intégrés dans le catalogue.
2. Identification des mouvements rejetés dont le SKU est maintenant connu.

Ce DAG :
1. Lit la table rejected_movements (status='PENDING') depuis PostgreSQL
2. Vérifie quels SKUs sont désormais présents dans products
3. Réinsère ces mouvements dans la table movements
4. Met à jour leur statut en 'REPLAYED'
5. Génère un rapport de rejeu
"""
from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

import pandas as pd
import psycopg2
from airflow.decorators import dag, task
from psycopg2.extras import execute_values

DATA_CURATED = Path("/opt/airflow/data/curated")

DSN = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
    "dbname": os.getenv("POSTGRES_DB", "logistore"),
    "user": os.getenv("POSTGRES_USER", "logistore"),
    "password": os.getenv("POSTGRES_PASSWORD", "logistore"),
}

def get_conn():
    return psycopg2.connect(**DSN)


@dag(
    dag_id="replay_rejected_movements",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["logistore", "replay", "rejected"],
    doc_md="""## DAG 4 — Rejeu des mouvements rejetés\n\n
Déclencher ce DAG manuellement après avoir intégré de nouveaux produits
dans le catalogue.""",
)
def replay_rejected_movements():

    @task
    def fetch_pending_rejected() -> list[dict]:
        """Récupère tous les mouvements avec status='PENDING'."""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        id, movement_id, sku, movement_type,
                        quantity, reason, occurred_at,
                        rejection_reason, status
                    FROM rejected_movements
                    WHERE status = 'PENDING'
                """)
                cols = [desc[0] for desc in cur.description]
                rows = [dict(zip(cols, row)) for row in cur.fetchall()]
        print(f"Mouvements PENDING trouvés : {len(rows)}")
        return rows

    @task
    def filter_now_known_skus(rejected: list[dict]) -> dict:
        """Filtre ceux dont le SKU est maintenant présent dans products."""
        if not rejected:
            print("Aucun mouvement en attente de rejeu.")
            return {"replayable": [], "still_pending": [], "counts": {}}

        pending_skus = list({r["sku"] for r in rejected})

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT sku FROM products WHERE sku = ANY(%s)",
                    (pending_skus,)
                )
                known_skus = {row[0] for row in cur.fetchall()}

        replayable = [r for r in rejected if r["sku"] in known_skus]
        still_pending = [r for r in rejected if r["sku"] not in known_skus]

        print(f"Rejouables : {len(replayable)} | Encore en attente : {len(still_pending)}")
        return {
            "replayable": replayable,
            "still_pending": still_pending,
            "counts": {
                "replayable": len(replayable),
                "still_pending": len(still_pending),
            }
        }

    @task
    def replay_movements(result: dict) -> dict:
        """Réinsère les mouvements rejouables et met à jour le Parquet."""
        replayable = result.get("replayable", [])
        if not replayable:
            print("Aucun mouvement à rejouer.")
            return {"replayed": 0, "still_pending": len(result.get("still_pending", []))}

        # 1. INSERT dans movements
        values = [
            (
                r["movement_id"], r["sku"], r["movement_type"],
                r["quantity"], r["reason"], r["occurred_at"],
            )
            for r in replayable
        ]
        insert_sql = """
            INSERT INTO movements (
                movement_id, sku, movement_type, quantity, reason, occurred_at
            )
            VALUES %s
            ON CONFLICT (movement_id) DO NOTHING
        """

        # 2. UPDATE statut → REPLAYED
        replay_ids = [r["movement_id"] for r in replayable]
        update_sql = """
            UPDATE rejected_movements
            SET status = 'REPLAYED'
            WHERE movement_id = ANY(%s)
        """

        with get_conn() as conn:
            with conn.cursor() as cur:
                execute_values(cur, insert_sql, values)
                cur.execute(update_sql, (replay_ids,))
            conn.commit()

        # 3. Append dans movements_history.parquet
        DATA_CURATED.mkdir(parents=True, exist_ok=True)
        history_path = DATA_CURATED / "movements_history.parquet"
        new_df = pd.DataFrame([
            {
                "movement_id": r["movement_id"],
                "sku": r["sku"],
                "movement_type": r["movement_type"],
                "quantity": r["quantity"],
                "reason": r["reason"],
                "occurred_at": r["occurred_at"],
            }
            for r in replayable
        ])
        if history_path.exists():
            old_df = pd.read_parquet(history_path)
            full_df = pd.concat([old_df, new_df], ignore_index=True).drop_duplicates("movement_id")
        else:
            full_df = new_df
        full_df.to_parquet(history_path, index=False)

        stats = {
            "replayed": len(replayable),
            "still_pending": len(result.get("still_pending", []))
        }
        print(f"Rejeu terminé : {stats}")
        return stats

    pending = fetch_pending_rejected()
    filtered = filter_now_known_skus(pending)
    replay_movements(filtered)


replay_rejected_movements()