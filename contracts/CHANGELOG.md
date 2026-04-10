# CHANGELOG — Contrats d'interface LogiStore

Ce fichier trace toutes les évolutions des contrats de données du projet.
Toute modification d'un contrat existant ou ajout d'un nouveau contrat doit être documentée ici.

---

## v1.0.0 — 2026-04-07 (scaffold initial)

### CatalogueRecordV1
- Champs : `schema_version`, `sku`, `label`, `category`, `unit`, `min_stock`, `published_at`
- Contraintes : `schema_version` doit être `'1.0'`, `sku` au format `SKU-XXXXX`, `min_stock >= 0`
- Catégories acceptées : `FOOD`, `ELEC`, `TOOL`, `CLOTH`, `OTHER`

### MovementRecordV1
- Champs : `schema_version`, `movement_id`, `sku`, `movement_type`, `quantity`, `reason`, `occurred_at`
- Contraintes : `movement_type` IN (`IN`, `OUT`), quantité positive pour `IN`, négative pour `OUT`, non nulle

---

## v1.1.0 — 2026-04-07 (extension catalogue)

### CatalogueRecordV2
- Ajout du champ optionnel `supplier_id` (str, nullable)
- Rétrocompatible avec V1 — `supplier_id` absent = valide
- Registre de contrats `ContractVersionRegistry` ajouté pour router V1/V2 dynamiquement

---

## Fix — 2026-04-08

### DAG1 — lecture CSV
- Problème : pandas interprétait `schema_version = 1.0` comme un float, rejeté par le contrat Pydantic (`Literal['1.0']`)
- Correction : ajout de `dtype={"schema_version": "string"}` dans `pd.read_csv()`
- Impact : 0 lignes valides → 500 lignes valides sur le palier medium