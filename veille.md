Veille technologique – Pipeline LogiStore
1. Flux EDI (Electronic Data Interchange)

Un EDI (Electronic Data Interchange) est un système permettant l’échange automatisé de documents entre organisations ou systèmes informatiques, dans un format structuré et standardisé, sans intervention humaine.

Types de flux EDI

Les flux EDI peuvent transporter différents types de documents :

commandes (purchase orders)

factures

avis d’expédition

catalogues produits

confirmations de commande

Les formats EDI les plus courants sont :

EDIFACT

ANSI X12

XML

CSV (version simplifiée)

Dans le projet LogiStore, les fichiers CSV représentent une forme simplifiée d’EDI, où chaque fichier correspond à un document échangé entre systèmes.

Différence avec API et RPC
Technologie	Principe	Mode
EDI	échange de documents structurés	asynchrone
API REST	communication entre services via HTTP	synchrone
RPC	appel direct de fonction distante	synchrone

Différences principales :

EDI : échanges par fichiers ou messages, souvent batch.

API : requêtes en temps réel.

RPC : communication directe entre programmes.

L’EDI est donc plus adapté aux flux de données volumineux et asynchrones.

2. Contrats d’interface (Data Contracts)

Un data contract définit la structure et les règles d’un échange de données entre un producteur et un consommateur.

Il décrit notamment :

le schéma des données

les types de champs

les contraintes (nullable, format, etc.)

les règles de validation

Différence avec une simple documentation
Documentation	Data Contract
description théorique	validation automatique
peut être ignorée	doit être respecté
non exécutable	exécutable dans le code

Un data contract est donc une documentation "vivante" qui peut être automatiquement vérifiée.

Exemples industriels

Confluent Schema Registry : gestion des schémas pour Kafka

Avro : format de données avec schéma

Pydantic : validation de données en Python

Solution adaptée au projet

Dans un contexte Python sans infrastructure lourde, la solution la plus adaptée est Pydantic car elle permet :

validation automatique des données

définition claire des modèles

gestion des erreurs

Exemple :

from pydantic import BaseModel

class Order(BaseModel):
    order_id: int
    product_id: int
    quantity: int
    price: float
3. Dead Letter Queue (DLQ) et rejeu

Dans les systèmes de messagerie comme :

Kafka

RabbitMQ

AWS SQS

certains messages peuvent échouer lors du traitement (données invalides, service indisponible, erreur de format).

Dead Letter Queue

Une DLQ (Dead Letter Queue) est une file spéciale où sont envoyés les messages qui ne peuvent pas être traités.

Objectifs :

éviter de bloquer le pipeline

conserver les messages problématiques

permettre leur analyse

Rejeu des messages

Le rejeu consiste à retraiter les messages une fois le problème corrigé.

Exemple :

message invalide → envoyé en DLQ

correction du problème

réinjection du message dans le pipeline

Application à un pipeline CSV

Dans le projet :

fichiers CSV invalides → déplacés dans un dossier dead_letter/

journalisation de l’erreur

possibilité de rejouer le traitement après correction

Structure possible :

data/
incoming/
processed/
dead_letter/
4. Orchestration avec Airflow

Airflow est un outil permettant d’orchestrer et planifier des pipelines de données.

Le pipeline est représenté sous forme de DAG (Directed Acyclic Graph).

DAG

Un DAG est un graphe de tâches dépendantes.

Exemple :

ingestion -> validation -> transformation -> stockage

Chaque étape est une task.

Caractéristiques :

pas de cycles

ordre d’exécution défini

monitoring possible

Dataset API (Airflow 2.4)

La Dataset API permet de déclencher un DAG lorsqu’une donnée spécifique est mise à jour.

Exemple :

DAG ingestion → produit dataset "orders_data"

DAG analytics → se déclenche quand dataset "orders_data" est mis à jour

Avantages :

découplage entre pipelines

meilleure modularité

orchestration par données plutôt que par tâches

Comparaison avec d'autres méthodes
Méthode	Principe	Limite
TriggerDagRunOperator	déclenche un DAG	couplage fort
ExternalTaskSensor	attend la fin d'une tâche	dépendance rigide
Dataset API	déclenchement basé sur la donnée	plus flexible

La Dataset API est la solution la plus moderne et découplée.

5. Stockage columnar et format Parquet

Le format Parquet est un format de stockage columnar (par colonne) optimisé pour l’analyse de données.

Différence avec stockage ligne
Stockage ligne (CSV, SQL)	Stockage colonne (Parquet)
lecture ligne complète	lecture colonnes nécessaires
inefficace pour analytics	très efficace pour analytics

Exemple :

Si une table possède 50 colonnes et qu’une requête utilise seulement 3 colonnes :

CSV → lit les 50 colonnes

Parquet → lit seulement les 3

Optimisations
Projection Pushdown

Le moteur lit uniquement les colonnes nécessaires.

Predicate Pushdown

Les filtres sont appliqués directement au niveau du stockage.

Exemple :

SELECT * FROM orders WHERE price > 100

Seules les partitions pertinentes sont lues.

DuckDB vs PostgreSQL
DuckDB	PostgreSQL
optimisé analytics	optimisé transactions
lecture Parquet native	nécessite import
très rapide localement	serveur nécessaire

DuckDB est donc très adapté pour les analyses sur fichiers Parquet.

6. Scalabilité d’un pipeline de données

La scalabilité permet de traiter de grands volumes de données efficacement.

Stratégies classiques
Traitement par chunks

Lecture des données par blocs :

10000 lignes -> traitement -> chargement -> bloc suivant

Permet d’éviter la saturation mémoire.

Parallélisme

Traitement simultané de plusieurs tâches.

Exemple :

plusieurs workers

multiprocessing

tâches Airflow parallèles

Partitionnement

Les données sont divisées selon un critère :

/data/year=2025/month=03/
/data/year=2025/month=04/

Cela permet :

traitement parallèle

requêtes plus rapides

Scalabilité verticale vs horizontale
Type	Principe
verticale	augmenter puissance machine
horizontale	ajouter plusieurs machines

Les architectures modernes privilégient la scalabilité horizontale.

Coût écologique du double stockage (SQL + Parquet)

Stocker les données dans :

une base SQL

des fichiers Parquet

permet :

transactions (SQL)

analytics rapides (Parquet)

Mais cela implique :

plus de stockage

duplication des données

coût énergétique supplémentaire

Il faut donc trouver un équilibre entre performance et éco-conception.