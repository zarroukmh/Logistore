"""
DAG 3 — Calcul des KPIs d'inventaire

Déclenché automatiquement par le Dataset publié par DAG 2.

Ce DAG :
1. Lit l'historique des mouvements depuis Parquet (DuckDB)
2. Calcule le stock courant par SKU
3. Identifie les SKUs en rupture ou sous le seuil minimum
4. Produit un rapport CSV dans data/reports/

TODO étudiant : compléter les requêtes analytiques et
explorer la comparaison des performances SQL vs Parquet.
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path


import duckdb


from airflow.datasets import Dataset
from airflow.decorators import dag, task

MOVEMENTS_DATASET = Dataset("file:///opt/airflow/data/curated/movements_history.parquet")

DATA_CURATED = Path("/opt/airflow/data/curated")
DATA_REPORTS = Path("/opt/airflow/data/reports")


@dag(
    dag_id="inventory_analytics",
    schedule=[MOVEMENTS_DATASET],  # Déclenché par DAG 2
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["logistore", "analytics"],
)
def inventory_analytics():

    @task
    def compute_current_stock() -> str:
        """
        Calcule le stock courant par SKU depuis Parquet via DuckDB.
        Retourne le chemin du rapport généré.
        """
        # Import lazy pour éviter un Broken DAG si duckdb n'est pas encore installé
        # dans le conteneur Airflow. L'erreur éventuelle devient une erreur de tâche,
        # pas une erreur d'import du DAG.
        import duckdb

        movements_file = str(DATA_CURATED / "movements_history.parquet")
        if not Path(movements_file).exists():
            print("Pas encore de fichier Parquet mouvements. Rien à calculer.")
            return ""

        conn = duckdb.connect()

        # TODO étudiant : enrichir cette requête
        # Ajouter la jointure avec le catalogue pour obtenir min_stock
        # et calculer le stock_status (OK / WARNING / ALERT)

        catalogue_file = str(DATA_CURATED / "catalogue_snapshot.parquet")

        stock_df = conn.execute(f"""
            SELECT
                m.sku,
                SUM(m.quantity)                          AS current_stock,
                p.min_stock,
                p.label,
                p.category,
                CASE
                    WHEN SUM(m.quantity) <= 0            THEN 'ALERT'
                    WHEN SUM(m.quantity) < p.min_stock   THEN 'WARNING'
                    ELSE                                      'OK'
                END                                      AS stock_status
            FROM read_parquet('{movements_file}') AS m
            JOIN read_parquet('{catalogue_file}') AS p ON m.sku = p.sku
            GROUP BY m.sku, p.min_stock, p.label, p.category
            ORDER BY current_stock ASC
        """).df()

        DATA_REPORTS.mkdir(parents=True, exist_ok=True)
        report_date = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_path = str(DATA_REPORTS / f"inventory_report_{report_date}.csv")
        stock_df.to_csv(out_path, index=False)
        print(f"Rapport généré : {out_path} ({len(stock_df)} SKUs)")
        return out_path

    compute_current_stock()


inventory_analytics()
