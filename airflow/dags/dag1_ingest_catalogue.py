"""
DAG 1 — Ingestion du flux CATALOGUE

Ce DAG :
1. Détecte un nouveau fichier catalogue dans data/inbox/catalogue/
2. Valide chaque ligne avec le contrat CatalogueRecordV1 (ou V2)
3. Insère les produits valides dans PostgreSQL (UPSERT)
4. Exporte le catalogue complet en Parquet (data/curated/catalogue_snapshot.parquet)
5. Publie le Dataset Airflow pour déclencher automatiquement DAG 2

TODO étudiant : implémenter les fonctions marquées TODO ci-dessous.
"""
from __future__ import annotations

from datetime import datetime
import os
from pathlib import Path

import pandas as pd
import psycopg2
from airflow.datasets import Dataset
from airflow.decorators import dag, task
from psycopg2.extras import execute_values

from contracts.catalogue_contract import get_catalogue_contract

# Dataset Airflow partagé avec DAG 2
CATALOGUE_DATASET = Dataset("file:///opt/airflow/data/curated/catalogue_snapshot.parquet")

DATA_INBOX = Path("/opt/airflow/data/inbox/catalogue")
DATA_CURATED = Path("/opt/airflow/data/curated")
DATA_REJECTED = Path("/opt/airflow/data/rejected/catalogue")

# Paramètres de connexion PostgreSQL.
# Dans Airflow Docker, l'hôte est "postgres" (nom du service docker-compose).
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
    dag_id="ingest_catalogue",
    schedule="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["logistore", "catalogue", "ingestion"],
    doc_md="""## DAG 1 — Ingestion Catalogue\n\nIngère les fichiers CSV catalogue et publie le Dataset pour DAG 2.""",
)
def ingest_catalogue():

    @task
    def detect_new_catalogue_file() -> str | None:
        """Retourne le chemin du dernier fichier catalogue disponible, ou None."""
        # On trie par date de modification décroissante pour prendre le fichier le plus récent.
        files = sorted(DATA_INBOX.glob("*.csv"), key=lambda f: f.stat().st_mtime, reverse=True)
        if not files:
            print("Aucun fichier catalogue trouvé.")
            return None
        path = str(files[0])
        print(f"Fichier détecté : {path}")
        return path

    @task
    def validate_and_upsert_catalogue(filepath: str | None) -> dict:
        """
        TODO étudiant :
        1. Charger le fichier CSV filepath avec pandas
        2. Pour chaque ligne, déterminer la version du contrat (schema_version)
           et valider avec get_catalogue_contract(version)
        3. Séparer les lignes valides des lignes invalides
        4. Insérer/mettre à jour (UPSERT) les lignes valides dans PostgreSQL
        5. Sauvegarder les lignes invalides dans data/rejected/catalogue/
        6. Retourner un dict de stats : {valid: N, rejected: M}
        """
        if not filepath:
            return {"valid": 0, "rejected": 0, "skipped": True}

        # Lecture brute du CSV catalogue.
        df = pd.read_csv(filepath)
        valid_rows: list[dict] = []
        rejected_rows: list[dict] = []

        # Validation ligne par ligne selon la version du contrat (V1 ou V2).
        for idx, row in df.iterrows():
            # Conversion NaN -> None pour que Pydantic gère proprement les champs optionnels.
            payload = {k: (None if pd.isna(v) else v) for k, v in row.to_dict().items()}
            version = str(payload.get("schema_version", "")).strip()
            try:
                model_class = get_catalogue_contract(version)
                validated = model_class(**payload)
                valid_rows.append(validated.model_dump())
            except Exception as exc:
                # On conserve la ligne rejetée avec sa raison précise pour audit/rejeu manuel.
                rejected_rows.append(
                    {
                        **payload,
                        "line_number": int(idx) + 2,  # +2: header CSV + index 0
                        "rejection_reason": str(exc),
                    }
                )

        if valid_rows:
            # Préparation du lot pour insertion en masse.
            values = [
                (
                    r["sku"],
                    r["label"],
                    r["category"],
                    r["unit"],
                    r["min_stock"],
                    r.get("supplier_id"),
                    r["published_at"],
                )
                for r in valid_rows
            ]
            # UPSERT : INSERT si nouveau SKU, UPDATE si SKU déjà existant.
            upsert_sql = """
                INSERT INTO products (
                    sku, label, category, unit, min_stock, supplier_id, published_at
                )
                VALUES %s
                ON CONFLICT (sku)
                DO UPDATE SET
                    label = EXCLUDED.label,
                    category = EXCLUDED.category,
                    unit = EXCLUDED.unit,
                    min_stock = EXCLUDED.min_stock,
                    supplier_id = EXCLUDED.supplier_id,
                    published_at = EXCLUDED.published_at
            """
            with get_conn() as conn:
                with conn.cursor() as cur:
                    # execute_values permet une insertion batch performante.
                    execute_values(cur, upsert_sql, values)
                conn.commit()

        if rejected_rows:
            # Persistance locale des rejets (traçabilité des erreurs de contrat).
            DATA_REJECTED.mkdir(parents=True, exist_ok=True)
            rejected_df = pd.DataFrame(rejected_rows)
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            rejected_file = DATA_REJECTED / f"catalogue_rejected_{ts}.csv"
            rejected_df.to_csv(rejected_file, index=False)
            print(f"Lignes rejetées sauvegardées : {rejected_file}")

        stats = {"valid": len(valid_rows), "rejected": len(rejected_rows), "skipped": False}
        print(f"Ingestion catalogue terminée : {stats}")
        return stats

    @task(outlets=[CATALOGUE_DATASET])
    def export_catalogue_to_parquet(stats: dict) -> None:
        """
        TODO étudiant :
        1. Lire tous les produits depuis PostgreSQL
        2. Exporter en Parquet dans data/curated/catalogue_snapshot.parquet
        3. Le décorateur outlets=[CATALOGUE_DATASET] déclenche automatiquement DAG 2
        """
        DATA_CURATED.mkdir(parents=True, exist_ok=True)
        print(f"Stats ingestion : {stats}")
        out_path = DATA_CURATED / "catalogue_snapshot.parquet"

        # Snapshot complet du catalogue opérationnel depuis PostgreSQL.
        with get_conn() as conn:
            snapshot_df = pd.read_sql_query(
                """
                SELECT
                    sku,
                    label,
                    category,
                    unit,
                    min_stock,
                    supplier_id,
                    published_at,
                    inserted_at
                FROM products
                ORDER BY sku
                """,
                conn,
            )

        # Export Parquet pour la couche analytique et publication Dataset Airflow (outlets).
        snapshot_df.to_parquet(out_path, index=False)
        print(f"Snapshot catalogue exporté : {out_path} ({len(snapshot_df)} lignes)")

    # Chaînage des tâches
    filepath = detect_new_catalogue_file()
    stats = validate_and_upsert_catalogue(filepath)
    export_catalogue_to_parquet(stats)


ingest_catalogue()
