"""
Benchmark comparatif SQL (PostgreSQL) vs Parquet (DuckDB).

Usage :
    python scripts/benchmark_queries.py --palier small
    python scripts/benchmark_queries.py --palier small --ci   # mode CI : sorties réduites
"""
import argparse
import os
import time
from pathlib import Path

import duckdb
import psycopg2

DSN = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
    "dbname": os.getenv("POSTGRES_DB", "logistore"),
    "user": os.getenv("POSTGRES_USER", "logistore"),
    "password": os.getenv("POSTGRES_PASSWORD", "logistore"),
}

QUERIES_PARQUET = {
    "stock_by_sku": """
        SELECT sku, SUM(quantity) AS current_stock
        FROM read_parquet('{movements_file}')
        GROUP BY sku
        ORDER BY current_stock DESC
    """,
    "movements_by_month": """
        SELECT strftime(occurred_at, '%Y-%m') AS month,
               movement_type, COUNT(*) AS nb, SUM(quantity) AS total_qty
        FROM read_parquet('{movements_file}')
        GROUP BY 1, 2
        ORDER BY 1
    """,
}

QUERIES_SQL = {
    "stock_by_sku": """
        SELECT sku, SUM(quantity) AS current_stock
        FROM movements
        GROUP BY sku
        ORDER BY current_stock DESC
    """,
    "movements_by_month": """
        SELECT TO_CHAR(occurred_at, 'YYYY-MM') AS month,
               movement_type, COUNT(*) AS nb, SUM(quantity) AS total_qty
        FROM movements
        GROUP BY 1, 2
        ORDER BY 1
    """,
}


def run_parquet_benchmark(movements_file: str, ci_mode: bool = False):
    """Exécute les requêtes DuckDB sur un fichier Parquet et mesure le temps."""
    if not Path(movements_file).exists():
        print(f"⚠️  Fichier Parquet introuvable : {movements_file}")
        print("   Générez d'abord les données avec generate_flows.py puis exportez en Parquet.")
        return {}

    conn = duckdb.connect()
    results = {}
    for name, query in QUERIES_PARQUET.items():
        q = query.format(movements_file=movements_file)
        start = time.perf_counter()
        df = conn.execute(q).df()
        elapsed = (time.perf_counter() - start) * 1000
        results[name] = {"engine": "DuckDB/Parquet", "rows": len(df), "ms": round(elapsed, 2)}
        if not ci_mode:
            print(f"  [{name}] DuckDB/Parquet : {elapsed:.2f} ms ({len(df)} lignes)")
    return results


def run_sql_benchmark(ci_mode: bool = False):
    """Exécute les requêtes sur PostgreSQL et mesure le temps."""
    try:
        conn = psycopg2.connect(**DSN)
    except Exception as e:
        print(f"⚠️  Connexion PostgreSQL impossible : {e}")
        return {}

    results = {}
    with conn:
        with conn.cursor() as cur:
            for name, query in QUERIES_SQL.items():
                start = time.perf_counter()
                cur.execute(query)
                rows = cur.fetchall()
                elapsed = (time.perf_counter() - start) * 1000
                results[name] = {"engine": "PostgreSQL", "rows": len(rows), "ms": round(elapsed, 2)}
                if not ci_mode:
                    print(f"  [{name}] PostgreSQL      : {elapsed:.2f} ms ({len(rows)} lignes)")
    conn.close()
    return results


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--palier", choices=["small", "medium", "large"], default="small")
    parser.add_argument("--ci", action="store_true", help="Mode CI : skip si fichiers absents")
    args = parser.parse_args()

    movements_parquet = "data/curated/movements_history.parquet"

    print(f"\n📊 Benchmark — palier '{args.palier}'")
    print("-" * 50)

    parquet_results = run_parquet_benchmark(movements_parquet, ci_mode=args.ci)
    sql_results = run_sql_benchmark(ci_mode=args.ci)

    if parquet_results and sql_results:
        print("\n📈 Comparaison DuckDB/Parquet vs PostgreSQL :")
        print(f"  {'Requête':<25} {'DuckDB (ms)':>12} {'PostgreSQL (ms)':>16} {'Ratio':>8}")
        print("  " + "-" * 65)
        for name in parquet_results:
            p = parquet_results[name]["ms"]
            s = sql_results.get(name, {}).get("ms", 0)
            ratio = f"{s/p:.1f}x" if p > 0 else "N/A"
            print(f"  {name:<25} {p:>12.2f} {s:>16.2f} {ratio:>8}")

    if parquet_results:
        print("\n✅ Benchmark Parquet/DuckDB terminé.")
    elif args.ci:
        print("ℹ️  Mode CI : fichiers Parquet absents, benchmark ignoré.")


if __name__ == "__main__":
    main()