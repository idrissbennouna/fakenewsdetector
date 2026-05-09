# FakeNews Detector — Pipeline de détection de désinformation

Projet Python/DevOps complet visant à détecter et analyser la désinformation marocaine via un pipeline moderne.

## 🧠 Objectif
Ce projet collecte des articles de presse en temps réel depuis des flux RSS marocains et internationaux, normalise les données, applique des analyses NLP, score les risques de fake news et stocke les résultats dans un entrepôt de données.

## 🚀 Architecture globale

Le pipeline est conçu en plusieurs couches :

1. **Scraping RSS**
   - Sources principales : `Hespress`, `BBC News`, `Al Jazeera`
   - Chaque source est implémentée dans `fakenewsdetector/scrapers`
   - Les articles sont construits via `BaseScraper` et publiés dans Kafka sur le topic `raw-articles`

2. **Bronze Layer**
   - Consommateur Kafka `fakenewsdetector/kafka/consumer.py`
   - Normalisation et partitionnement en JSON dans MinIO
   - Stockage dans `fakenews-lake/bronze/...`
   - Validation simple et déduplication

3. **Silver Layer**
   - Lecture des objets Bronze depuis MinIO
   - Enrichissement NLP dans `fakenewsdetector/pipeline/silver_layer.py`
   - Scoring fake news via `fakenewsdetector/pipeline/nlp_engine.py`
   - Extraction d'entités, similarité cross-sources et détection de signaux linguistiques
   - Résultats sauvegardés dans `fakenews-lake/silver/...`
   - Articles suspects/fausses nouvelles marqués et optionnellement publiés dans Kafka sur `flagged-articles`

4. **Gold Layer**
   - Agrégation finale vers PostgreSQL
   - Tables analytiques et historique dans `fakenewsdetector/pipeline/gold_layer.py`
   - Chargement de la couche Silver, puis upsert dans `articles_enrichis` et tables dérivées

5. **Orchestration Airflow**
   - DAG principal : `fakenewsdetector/airflow/dags/fakenews_dag.py`
   - Flux : `scrape` → `bronze` → `silver` → `gold` → `notify`
   - Planifié dans Docker pour exécution régulière

6. **Interface & monitoring**
   - `superset` pour visualisations BI
   - `kafka-ui` pour inspecter les topics Kafka

---

## 📦 Services Docker fournis

Le fichier `docker-compose.yml` démarre les services suivants :

- `zookeeper`
- `kafka`
- `kafka-ui`
- `minio` + `minio-init`
- `postgres`
- `spark` + `spark-worker`
- `airflow`
- `superset`

---

## 🧩 Composants principaux

### `fakenewsdetector/scrapers`
- `base_scraper.py` : base commune pour les scrapers, détection de langue, normalisation de date, génération d'ID, envoi Kafka
- `hespress_scraper.py` : RSS Hespress
- `bbc_aljazira_scraper.py` : RSS BBC et Al Jazeera

### `fakenewsdetector/pipeline`
- `bronze_layer.py` : validation et déduplication des objets Bronze
- `silver_layer.py` : enrichissement NLP, score de désinformation, écriture Silver
- `gold_layer.py` : chargement Silver et insertion PostgreSQL
- `nlp_engine.py` : moteur de score, similarité TF-IDF, extraction d'entités

### `fakenewsdetector/airflow/dags/fakenews_dag.py`
- Orchestration du pipeline complet via Airflow
- Contrôle des erreurs et notifications de fin d'exécution

---

## ⚙️ Installation locale

### Prérequis
- Python 3.11+
- Docker & Docker Compose
- `pip` ou environnement virtuel

### Installation des dépendances Python

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

Si vous utilisez Superset séparément :

```bash
pip install -r requirements-superset.txt
```

### Démarrage des services Docker

```bash
docker compose up --build
```

### Initialisation Superset

Dans le conteneur `superset`, le service exécute déjà :

- `superset db upgrade`
- `superset fab create-admin ...`
- `superset init`
- `superset run -h 0.0.0.0 -p 8088`

Accédez à Superset sur `http://localhost:8088`.

---

## ▶️ Exécution du pipeline

### Lancement du scraping

Les scrapers peuvent être lancés manuellement ou via Airflow.

Manuellement :

```bash
python -m fakenewsdetector.scrapers.hespress_scraper
python -m fakenewsdetector.scrapers.bbc_aljazira_scraper
```

### Lancement du Bronze

```bash
python -m fakenewsdetector.pipeline.bronze_layer
```

### Lancement du Silver

```bash
python -m fakenewsdetector.pipeline.silver_layer
```

### Lancement du Gold

```bash
python -m fakenewsdetector.pipeline.gold_layer
```

---

## 🔍 Points clés techniques

- Utilisation de **Kafka** pour le transport des articles bruts
- Stockage d'objets sémantiquement partitionnés dans **MinIO**
- Analyse NLP avec **spaCy** et **scikit-learn**
- Score de fake news basé sur des critères linguistiques et catégoriels
- Agrégation analytique dans **PostgreSQL**
- Orchestration avec **Airflow**
- Visualisation avec **Superset**

---

## 🧪 Mesures de qualité

- Validation de schéma dans la couche Bronze
- Déduplication par identifiant d'article
- Gestion des erreurs dans chaque couche
- Fallback de langue si spaCy n'est pas disponible

---

## 📝 Remarques

- Le module `fakenewsdetector/kafka/producer.py` est présent mais vide ; le scraping envoie déjà les messages vers Kafka depuis `BaseScraper`.
- `main.py` contient un point d'entrée placeholder : `Hello from kafka!`
- Pour un environnement Docker, les URLs de services sont généralement référencées par le nom de service (`kafka`, `minio`, `postgres`).

---

## 📌 Améliorations possibles

- Ajouter une API REST pour consultations en temps réel
- Mettre en place une authentification pour Superset
- Ajouter davantage de sources RSS marocaines
- Renforcer le scoring par apprentissage automatique
- Déployer des tests unitaires et de bout en bout

---

## 📚 Structure du dépôt

- `docker-compose.yml` : orchestration de l'infrastructure
- `Dockerfile.airflow`, `Dockerfile.superset` : images Docker dédiées
- `fakenewsdetector/` : code applicatif principal
- `sql/init.sql` : initialisation du schéma PostgreSQL
- `requirements.txt` : dépendances Python
- `requirements-superset.txt` : dépendances Superset

---


