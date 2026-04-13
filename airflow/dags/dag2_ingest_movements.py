"""
DAG 2 — Ingestion du flux MOUVEMENT

Ce DAG est déclenché automatiquement par le Dataset Airflow
publié par DAG 1 (catalogue_snapshot.parquet).

Ce DAG :
1. Charge le fichier mouvements depuis data/inbox/movements/
2. Valide le schéma Pydantic (MovementRecordV1)
3. Vérifie l'existence du SKU dans la table products (PostgreSQL)
   - SKU connu → insertion dans movements
   - SKU inconnu → insertion dans rejected_movements + rapport de rejet
4. Exporte les mouvements valides en Parquet
5. Génère un rapport JSON de rejet si nécessaire
"""
from __future__ import annotations

from datetime import datetime
import json
import os
from pathlib import Path

import pandas as pd
import psycopg2
from airflow.datasets import Dataset
from airflow.decorators import dag, task
from psycopg2.extras import execute_values

from contracts.movement_contract import MovementRecordV1

CATALOGUE_DATASET = Dataset("file:///opt/airflow/data/curated/catalogue_snapshot.parquet")
MOVEMENTS_DATASET = Dataset("file:///opt/airflow/data/curated/movements_history.parquet")

DATA_INBOX = Path("/opt/airflow/data/inbox/movements")
DATA_CURATED = Path("/opt/airflow/data/curated")
DATA_REJECTED = Path("/opt/airflow/data/rejected")

# Paramètres de connexion PostgreSQL.
DSN = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
    "dbname": os.getenv("POSTGRES_DB", "logistore"),
    "user": os.getenv("POSTGRES_USER", "logistore"),
    "password": os.getenv("POSTGRES_PASSWORD", "logistore"),
}


def get_conn():
    """Ouvre une connexion PostgreSQL avec le DSN du projet."""
    return psycopg2.connect(**DSN)


@dag(
    dag_id="ingest_movements",
    schedule=[CATALOGUE_DATASET],  # Déclenché par DAG 1
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["logistore", "movements", "ingestion"],
    doc_md="""## DAG 2 — Ingestion Mouvements\n\nIngère les mouvements de stock, rejette ceux sur SKU inconnus.""",
)
def ingest_movements():

    @task
    def load_movements_file() -> str | None:
        """Retourne le chemin du dernier fichier mouvements disponible."""
        # On prend le dernier fichier déposé dans l'inbox.
        files = sorted(DATA_INBOX.glob("*.csv"), key=lambda f: f.stat().st_mtime, reverse=True)
        if not files:
            print("Aucun fichier mouvements trouvé.")
            return None
        return str(files[0])

    @task
    def validate_schema(filepath: str | None) -> dict:
        """
        1. Charger le fichier CSV
        2. Valider chaque ligne avec MovementRecordV1
        3. Retourner {valid_rows: [...], invalid_rows: [...], filepath: filepath}
        """
        if not filepath:
            return {"valid_rows": [], "invalid_rows": [], "filepath": None}

        # Forcer schema_version en string pour respecter le Literal["1.0"] du contrat.
        df = pd.read_csv(filepath, dtype={"schema_version": "string"})
        valid_rows: list[dict] = []
        invalid_rows: list[dict] = []

        # Validation stricte du contrat MovementRecordV1 ligne par ligne.
        for idx, row in df.iterrows():
            payload = {k: (None if pd.isna(v) else v) for k, v in row.to_dict().items()}
            if payload.get("schema_version") is not None:
                payload["schema_version"] = str(payload["schema_version"]).strip()
            try:
                validated = MovementRecordV1(**payload)
                valid_rows.append(validated.model_dump())
            except Exception as exc:
                invalid_rows.append(
                    {
                        **payload,
                        "line_number": int(idx) + 2,
                        "rejection_reason": f"schema_error: {exc}",
                    }
                )

        print(
            f"Validation schéma terminée — total={len(df)} valid={len(valid_rows)} "
            f"invalid={len(invalid_rows)}"
        )
        return {"valid_rows": valid_rows, "invalid_rows": invalid_rows, "filepath": filepath}

    @task
    def check_sku_and_route(validation_result: dict) -> dict:
        """
        1. Pour chaque ligne valide (schema OK), vérifier que le SKU existe dans products
        2. Lignes avec SKU connu → accepted_rows
        3. Lignes avec SKU inconnu → rejected_rows avec rejection_reason='unknown_sku'
        4. Insérer rejected_rows dans la table rejected_movements
        5. Retourner {accepted: [...], rejected_count: N, total: M}

        C'est le coeur du projet : gérer l'asynchronisme entre les deux flux.
        """
        valid_rows = validation_result.get("valid_rows", [])
        invalid_rows = validation_result.get("invalid_rows", [])
        if not valid_rows and not invalid_rows:
            return {
                "accepted": [],
                "unknown_sku_rejected": [],
                "schema_rejected": [],
                "rejected_count": 0,
                "total": 0,
            }

        # Récupération des SKUs produits connus.
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT sku FROM products")
                known_skus = {row[0] for row in cur.fetchall()}

        accepted_rows: list[dict] = []
        unknown_sku_rejected: list[dict] = []

        # Routage fonctionnel : connu => accepté, inconnu => rejet DLQ.
        for row in valid_rows:
            if row["sku"] in known_skus:
                accepted_rows.append(row)
            else:
                unknown_sku_rejected.append({**row, "rejection_reason": "unknown_sku"})

        # Persistance des rejets de type unknown_sku dans rejected_movements.
        if unknown_sku_rejected:
            values = [
                (
                    r.get("movement_id"),
                    r.get("sku"),
                    r.get("movement_type"),
                    r.get("quantity"),
                    r.get("reason"),
                    r.get("occurred_at"),
                    r.get("rejection_reason"),
                )
                for r in unknown_sku_rejected
            ]
            insert_rejected_sql = """
                INSERT INTO rejected_movements (
                    movement_id, sku, movement_type, quantity, reason, occurred_at, rejection_reason
                )
                VALUES %s
            """
            with get_conn() as conn:
                with conn.cursor() as cur:
                    execute_values(cur, insert_rejected_sql, values)
                conn.commit()

        total = len(valid_rows) + len(invalid_rows)
        rejected_count = len(unknown_sku_rejected) + len(invalid_rows)
        print(
            f"Routing terminé — total={total} accepted={len(accepted_rows)} "
            f"unknown_sku={len(unknown_sku_rejected)} schema_rejected={len(invalid_rows)}"
        )
        return {
            "accepted": accepted_rows,
            "unknown_sku_rejected": unknown_sku_rejected,
            "schema_rejected": invalid_rows,
            "rejected_count": rejected_count,
            "total": total,
        }

    @task(outlets=[MOVEMENTS_DATASET])
    def persist_valid_movements(routing_result: dict) -> dict:
        """
        1. Insérer les mouvements acceptés dans la table movements (PostgreSQL)
        2. Append dans data/curated/movements_history.parquet
        3. Générer le rapport de rejet JSON si rejected_count > 0
        4. Le décorateur outlets=[MOVEMENTS_DATASET] déclenche DAG 3
        """
        accepted = routing_result.get("accepted", [])
        unknown_sku_rejected = routing_result.get("unknown_sku_rejected", [])
        schema_rejected = routing_result.get("schema_rejected", [])

        inserted = 0
        if accepted:
            values = [
                (
                    r.get("movement_id"),
                    r.get("sku"),
                    r.get("movement_type"),
                    r.get("quantity"),
                    r.get("reason"),
                    r.get("occurred_at"),
                )
                for r in accepted
            ]
            insert_movements_sql = """
                INSERT INTO movements (
                    movement_id, sku, movement_type, quantity, reason, occurred_at
                )
                VALUES %s
                ON CONFLICT (movement_id) DO NOTHING
            """
            with get_conn() as conn:
                with conn.cursor() as cur:
                    execute_values(cur, insert_movements_sql, values)
                conn.commit()
            inserted = len(accepted)

            # Historisation analytique en Parquet (append logique via concat puis rewrite).
            DATA_CURATED.mkdir(parents=True, exist_ok=True)
            history_path = DATA_CURATED / "movements_history.parquet"
            new_df = pd.DataFrame(accepted)
            if history_path.exists():
                old_df = pd.read_parquet(history_path)
                full_df = pd.concat([old_df, new_df], ignore_index=True)
            else:
                full_df = new_df
            full_df.to_parquet(history_path, index=False)
            print(f"Parquet mis à jour : {history_path} ({len(full_df)} lignes)")

        rejected_total = len(unknown_sku_rejected) + len(schema_rejected)
        if rejected_total > 0:
            DATA_REJECTED.mkdir(parents=True, exist_ok=True)
            reports_dir = DATA_REJECTED / "reports"
            reports_dir.mkdir(parents=True, exist_ok=True)
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            report_path = reports_dir / f"movements_rejections_{ts}.json"
            report = {
                "generated_at": datetime.now().isoformat(),
                "summary": {
                    "accepted": len(accepted),
                    "rejected_total": rejected_total,
                    "rejected_unknown_sku": len(unknown_sku_rejected),
                    "rejected_schema": len(schema_rejected),
                    "total": routing_result.get("total", len(accepted) + rejected_total),
                },
                "unknown_sku_rejected": unknown_sku_rejected,
                "schema_rejected": schema_rejected,
            }
            with report_path.open("w", encoding="utf-8") as f:
                json.dump(report, f, ensure_ascii=False, indent=2, default=str)
            print(f"Rapport de rejets généré : {report_path}")

        stats = {
            "inserted": inserted,
            "accepted": len(accepted),
            "rejected": rejected_total,
        }
        print(f"Persistance mouvements terminée : {stats}")
        return stats

    filepath = load_movements_file()
    validation = validate_schema(filepath)
    routing = check_sku_and_route(validation)
    persist_valid_movements(routing)


ingest_movements()
