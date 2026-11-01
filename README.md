# Artefact — Test technique Data Engineer
## Chicago Taxi Trips | Pipeline de données Medallion

Ce dépôt contient ma solution au test technique Data Engineer proposé
par Artefact.

L'objectif est de concevoir et de mettre en œuvre un pipeline de données
local de bout en bout, à partir du jeu de données Chicago Taxi Trips,
en utilisant Apache Airflow, PySpark, Docker Compose et un stockage
compatible S3.

La solution repose sur une architecture Medallion (Bronze, Silver,
Gold) et couvre l'ingestion, l'orchestration, la transformation,
les contrôles qualité et la production de données exploitables
pour la BI.

## 1. Vue d'ensemble de la solution

L'architecture suit le modèle Medallion :

```mermaid
flowchart TD
    A["Chicago Taxi Trips<br/>Ville de Chicago"]
    B["Source CSV 2023"]
    C["Bronze<br/>RustFS — données brutes"]
    D["Apache Airflow<br/>LocalExecutor"]
    E["PySpark<br/>Bronze vers Silver"]
    F["Silver<br/>Parquet nettoyé et typé"]
    G["PySpark<br/>Silver vers Gold"]
    H["Gold<br/>Agrégats prêts pour la BI"]

    A --> B --> C
    D --> E
    C --> E --> F --> G --> H
````

### Technologies utilisées

| Composant           | Technologie                  | Rôle                                     |
| ------------------- | ---------------------------- | ---------------------------------------- |
| Orchestration       | Apache Airflow 3.3.2         | Orchestration et suivi des traitements   |
| Traitement          | Apache Spark / PySpark 4.2.1 | Transformation des données en mode local |
| Stockage objet      | RustFS                       | Stockage compatible S3                   |
| Base de métadonnées | PostgreSQL 16                | Métadonnées Airflow                      |
| Environnement       | Docker Compose               | Infrastructure locale reproductible      |
| Format de stockage  | Parquet + Snappy             | Stockage analytique colonnaire           |

Spark est exécuté en mode local (`local[2]`) au sein de\
l'environnement d'exécution Airflow. Un cluster Spark distinct\
n'est donc pas nécessaire.

## 2. Jeu de données et provenance

Le jeu de données utilisé est Chicago Taxi Trips (2013–2023),\
publié par la Ville de Chicago.

- Source officielle :\
  [https://data.cityofchicago.org/Transportation/Taxi-Trips-2013-2023-/wrvz-psew](https://data.cityofchicago.org/Transportation/Taxi-Trips-2013-2023-/wrvz-psew?utm_source=chatgpt.com)
- Identifiant du jeu de données : `wrvz-psew`
- Période de traitement : T1 2023, du 1er janvier au\
  31 mars 2023.

Pour l'exécution locale, un miroir CSV des données 2023 distribué\
via Kaggle a été utilisé comme source de téléchargement.

L'API officielle Socrata a également été testée. Un échantillon\
de 1 000 enregistrements a été comparé au fichier CSV.\
L'extraction complète via API n'a cependant pas été utilisée\
dans le pipeline final.

Le fichier source contient environ 3,78 millions d'enregistrements.\
La période T1 2023 représente 1 446 439 enregistrements avant\
application des contrôles qualité.

## 3. Pipeline de données

### Bronze — ingestion des données brutes

Le fichier CSV original est chargé dans RustFS sans transformation.

Emplacement :

`s3://bronze/chicago_taxi_kaggle/source=2023/Taxi_Trips_2023.csv`

### Silver — nettoyage et standardisation

Le traitement PySpark `bronze_to_silver` réalise les opérations\
suivantes :

- Lecture du fichier CSV depuis Bronze.
- Normalisation des noms de colonnes au format snake_case.
- Conversion des dates et horodatages.
- Typage des champs numériques, décimaux et entiers.
- Filtrage sur la période T1 2023.
- Application des règles de qualité :
  - Identifiant `trip_id` obligatoire et non vide.
  - Horodatages de début et de fin valides.
  - Date de fin supérieure ou égale à la date de début.
  - Durée et distance des trajets non négatives.
- Déduplication sur `trip_id`.
- Ajout des colonnes de métadonnées `year`, `quarter`\
  et `processed_at`.
- Écriture au format Parquet avec compression Snappy.

Sortie :

`s3://silver/chicago_taxi/year=2023/quarter=Q1/`

### Gold — agrégations analytiques

Le traitement PySpark `silver_to_gold` exploite les données Silver\
pour produire deux jeux de données analytiques.

Indicateurs journaliers

- Nombre total de trajets.
- Chiffre d'affaires total.
- Tarif moyen.
- Distance moyenne des trajets.
- Durée moyenne des trajets.
- Total des pourboires.

Sortie : `s3://gold/chicago_taxi/daily_metrics/`

Indicateurs par zone de prise en charge

- Nombre total de trajets.
- Chiffre d'affaires total.
- Montant moyen d'un trajet.
- Distance moyenne des trajets.

Sortie : `s3://gold/chicago_taxi/pickup_area_metrics/`

## 4. Orchestration avec Airflow

Identifiant du DAG : `chicago_taxi_pipeline`

Ordre d'exécution :

```
bronze_to_silver
       |
       v
silver_to_gold
```

Le DAG est déclenché manuellement. Le rattrapage automatique\
(catchup) est désactivé et une seule exécution peut être active\
à la fois.

Les tâches disposent également d'une configuration de nouvelles\
tentatives et de délais maximaux d'exécution.

## 5. Résultats de l'exécution et validations

Les résultats suivants ont été obtenus lors de l'exécution\
locale du pipeline :

| Indicateur                               | Résultat  |
| ---------------------------------------- | --------- |
| Enregistrements source T1 2023           | 1 446 439 |
| Enregistrements rejetés                  | 36        |
| Enregistrements Silver                   | 1 446 403 |
| Fichiers Parquet Silver                  | 4         |
| Lignes d'indicateurs journaliers Gold    | 90        |
| Groupes par zone de prise en charge Gold | 78        |

La somme du nombre de trajets dans les agrégats Gold par zone\
correspond au nombre d'enregistrements Silver :\
1 446 403.

L'exécution finale du DAG Airflow s'est terminée avec succès.\
Les deux tâches `bronze_to_silver` et `silver_to_gold` ont\
retourné un code de sortie `0`.

Ces résultats correspondent à l'exécution validée et peuvent\
varier si le fichier source ou la période de traitement change.

## 6. Structure du dépôt

```
.
├── dags/
│   └── chicago_taxi_pipeline.py
├── scripts/
│   └── upload_source_to_bronze.py
├── src/
│   └── spark/
│       └── jobs/
│           ├── bronze_to_silver.py
│           └── silver_to_gold.py
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── .env.example
└── README.md
```

## 7. Installation et exécution locale

### Prérequis

- Docker Engine
- Docker Compose V2
- Python 3.12 ou version compatible pour le script d'import
- Le fichier CSV Chicago Taxi Trips 2023

### Configuration de l'environnement

Copier le fichier de configuration :

```
cp .env.example .env
```

Configurer dans `.env` les identifiants PostgreSQL, les accès\
RustFS ainsi que les deux secrets d'authentification Airflow.

Générer les secrets Airflow :

```
openssl rand -hex 32
openssl rand -hex 64
```

Utiliser la première valeur pour `AIRFLOW_API_SECRET_KEY`\
et la seconde pour `AIRFLOW_JWT_SECRET_KEY`.

Le fichier `.env` doit rester privé et ne doit jamais être\
commité dans le dépôt.

### Chargement du fichier source dans Bronze

Installer les dépendances Python :

```
python -m pip install -r requirements.txt
```

Définir les paramètres de connexion RustFS dans le terminal\
ou charger les valeurs correspondantes depuis `.env` :

```
export RUSTFS_ENDPOINT_URL=http://localhost:9000
export RUSTFS_ACCESS_KEY="your_access_key"
export RUSTFS_SECRET_KEY="your_secret_key"
```

Charger le fichier CSV :

```
python scripts/upload_source_to_bronze.py \
  --file /path/to/Taxi_Trips_2023.csv
```

Le script utilise le transfert multipart et évite de charger\
à nouveau le fichier si l'objet existe déjà à destination.

### Construction et démarrage des services

```
docker compose build
docker compose up airflow-init
docker compose up -d
docker compose ps
```

- Interface Airflow : [http://localhost:8080](http://localhost:8080?utm_source=chatgpt.com)
- Console RustFS : [http://localhost:9001](http://localhost:9001?utm_source=chatgpt.com)

La configuration actuelle utilise le gestionnaire d'authentification\
simple d'Airflow avec les identifiants locaux de développement\
`admin` / `admin`. Cette configuration est réservée à un usage\
local et doit être modifiée avant toute exposition externe.

### Déclenchement du DAG

Dans l'interface Airflow, rechercher `chicago_taxi_pipeline`,\
le dépauser si nécessaire, puis déclencher une exécution.

Il est également possible de le déclencher depuis le terminal :

```
docker compose exec airflow-scheduler \
  airflow dags trigger chicago_taxi_pipeline
```

L'ordre d'exécution attendu est :

1. `bronze_to_silver`
2. `silver_to_gold`

Les deux tâches doivent atteindre l'état `success`.

## 8. Choix techniques et compromis

- Spark en mode local : permet de conserver un environnement\
  léger et exécutable sur un poste de développement. Un cluster\
  multi-nœuds serait plus adapté à des volumes de production.
- RustFS : fournit une interface compatible S3 sans dépendance\
  à un compte cloud.
- Parquet et Snappy : permettent un stockage colonnaire\
  compressé et adapté aux traitements analytiques.
- LocalExecutor : répond aux besoins de ce pipeline à deux\
  tâches sans introduire une file de tâches distribuée.
- Écriture en mode overwrite : les couches Silver et Gold\
  sont remplacées à chaque exécution afin de garantir des résultats\
  reproductibles dans le cadre du test. En production, une stratégie\
  incrémentale et idempotente serait préférable.
- Contrôles qualité : les règles actuelles couvrent les\
  identifiants, les horodatages, les durées et les distances.\
  Des contrôles métier plus approfondis seraient nécessaires\
  dans un environnement de production.

## 9. Améliorations envisageables

Avec davantage de temps, les améliorations suivantes pourraient\
être apportées :

- Mise en place d'une ingestion et d'un traitement incrémentaux.
- Définition de contrats de schéma et détection des dérives.
- Création d'une couche dédiée aux enregistrements rejetés\
  avec suivi des indicateurs qualité.
- Ajout de tests unitaires et d'intégration.
- Mise en place du suivi de lignage et de l'observabilité.
- Intégration d'une chaîne CI/CD.
- Optimisation du partitionnement et des performances.
- Création d'un tableau de bord BI connecté aux données Gold.
- Renforcement de la gestion des secrets et de l'authentification.

## 10. Arrêt de l'environnement

Pour arrêter les conteneurs tout en conservant les volumes :

```
docker compose down
```

Pour supprimer également les volumes PostgreSQL et RustFS :

```
docker compose down -v
```

Attention : la commande `docker compose down -v` supprime\
définitivement les données persistantes locales.

## 11. Tests et validation

Le projet inclut un script de tests d'intégration :
`tests/test_pipeline_quality.py`.

Il vérifie :
- L'intégrité des identifiants et des horodatages Silver.
- L'absence de doublons et de valeurs négatives.
- Le respect de la période T1 2023.
- La réconciliation des agrégats Gold avec Silver.
- Le nombre attendu de lignes journalières et de groupes géographiques.

Exécution :

```bash
docker compose exec airflow-scheduler \
  python /opt/airflow/tests/test_pipeline_quality.py
