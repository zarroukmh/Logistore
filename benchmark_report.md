# Rapport de Benchmark — SQL vs Parquet
## Projet LogiStore — Livrable L2

> Comparaison des performances entre PostgreSQL (SQL relationnel) et DuckDB/Parquet (stockage columnar) sur 3 paliers de volume.

---

## Contexte

Le pipeline LogiStore stocke les mouvements de stock dans deux formats :
- **PostgreSQL** : stockage opérationnel ligne-par-ligne, optimisé pour les écritures et la cohérence transactionnelle.
- **Parquet + DuckDB** : stockage analytique columnar, optimisé pour les lectures et les agrégations sur grands volumes.

Les requêtes benchmarkées sont représentatives des usages analytiques réels :
- **stock_by_sku** : agrégation simple (`SUM`) sur l'ensemble des mouvements, groupée par SKU.
- **movements_by_month** : agrégation temporelle avec double `GROUP BY` (mois + type de mouvement).

---

## Résultats par palier

### Palier Small (200 produits, ~10 000 mouvements)

| Requête | DuckDB/Parquet (ms) | PostgreSQL (ms) | Ratio |
|---|---|---|---|
| stock_by_sku | 11.65 | 23.54 | **2.0x** |
| movements_by_month | 9.15 | 45.74 | **5.0x** |

### Palier Medium (500 produits, ~100 000 mouvements)

| Requête | DuckDB/Parquet (ms) | PostgreSQL (ms) | Ratio |
|---|---|---|---|
| stock_by_sku | 11.50 | 22.06 | **1.9x** |
| movements_by_month | 9.03 | 42.70 | **4.7x** |

### Palier Large (1 000 produits, ~1 000 000 mouvements)

| Requête | DuckDB/Parquet (ms) | PostgreSQL (ms) | Ratio |
|---|---|---|---|
| stock_by_sku | 12.70 | 21.61 | **1.7x** |
| movements_by_month | 10.02 | 47.19 | **4.7x** |

---

## Analyse

### DuckDB/Parquet est systématiquement plus rapide

Sur les deux requêtes et les trois paliers, DuckDB/Parquet surpasse PostgreSQL :
- Entre **1.7x et 2.0x** plus rapide sur l'agrégation simple (`stock_by_sku`).
- Entre **4.7x et 5.0x** plus rapide sur l'agrégation temporelle (`movements_by_month`).

### Stabilité de DuckDB face à la montée en volume

Les temps DuckDB restent stables entre 9 et 13 ms quel que soit le palier. Cela s'explique par deux mécanismes du format Parquet :
- **Predicate pushdown** : DuckDB ne lit que les colonnes nécessaires à la requête, pas toute la ligne.
- **Compression columnar** : les colonnes de même type sont compressées ensemble, réduisant les I/O.

### PostgreSQL se dégrade sur les agrégations temporelles

`movements_by_month` passe de 45.74 ms (small) à 47.19 ms (large) sur PostgreSQL — une légère dégradation liée au coût croissant du `GROUP BY` sur des données non partitionnées. Sans index sur `occurred_at`, PostgreSQL fait un scan séquentiel complet à chaque fois.

### Nuance : le contexte d'usage différencie les deux approches

| Critère | PostgreSQL | DuckDB/Parquet |
|---|---|---|
| Lectures analytiques | Lent (row-based) | Rapide (columnar) |
| Écritures transactionnelles | Rapide, ACID | Non adapté |
| Cohérence des données | Garantie (FK, contraintes) | Aucune contrainte |
| Gestion des rejets/rejeu | Native (SQL) | Impossible |
| Coût infrastructure | Simple | Double stockage |

---

## Conclusion

Le double stockage PostgreSQL + Parquet est justifié dans LogiStore :
- **PostgreSQL** reste indispensable pour l'ingestion, la validation des contrats, la gestion des rejets et le rejeu (DAGs 1, 2, 4).
- **DuckDB/Parquet** est le bon choix pour toute requête analytique (DAG 3, benchmark, reporting).

Le compromis principal est la **complexité de maintien de la cohérence** entre les deux stores : tout mouvement inséré dans PostgreSQL doit être répliqué dans le Parquet — c'est le rôle des `outlets` Dataset Airflow dans le pipeline.

---

*Benchmark exécuté le 08/04/2026 — environnement : Docker, Python 3.12, DuckDB 1.x, PostgreSQL 15.*
