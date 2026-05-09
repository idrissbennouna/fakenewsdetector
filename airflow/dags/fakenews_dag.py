"""
================================================================================
DAG Airflow — FakeNews Detector Pipeline (VERSION COMPLÈTE)
================================================================================
Orchestre le pipeline complet : scraping RSS → Bronze → Silver NLP → Gold DW.

Schedule   : toutes les heures (0 * * * *)
Flow       : scrape → bronze → silver → gold → notify
Dépendances: MinIO, PostgreSQL, Kafka (via Docker Compose)

Chemin     : fakenewsdetector/airflow/dags/fakenews_dag.py
================================================================================
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.exceptions import AirflowFailException
from datetime import datetime, timedelta
import sys
import os
import subprocess
import logging

logger = logging.getLogger(__name__)

# ── PYTHONPATH ───────────────────────────────────────────────────────────────
PROJECT_PATH = "/opt/airflow/fakenewsdetector"
if PROJECT_PATH not in sys.path:
    sys.path.insert(0, PROJECT_PATH)

# ── Default Args ─────────────────────────────────────────────────────────────
default_args = {
    "owner": "fakenews-team",
    "depends_on_past": False,
    "email": ["admin@fakenews.local"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=50),
    "sla": timedelta(minutes=55),
}

# ── Utilitaire ───────────────────────────────────────────────────────────────

def _run_module(module_path: str, label: str):
    """Exécute un module Python via subprocess, logue et lève en cas d'erreur."""
    cmd = [sys.executable, "-m", module_path]
    logger.info(f"[{label}] {' '.join(cmd)}")

    result = subprocess.run(cmd, capture_output=True, text=True, cwd=PROJECT_PATH)

    if result.stdout:
        logger.info(f"[{label}] OUT:\n{result.stdout}")
    if result.stderr:
        logger.warning(f"[{label}] ERR:\n{result.stderr}")

    if result.returncode != 0:
        raise AirflowFailException(
            f"[{label}] Échec code {result.returncode}.\n{result.stderr[:800]}"
        )

    logger.info(f"[{label}] ✅ Succès")
    return result.stdout


# ── Tâches ───────────────────────────────────────────────────────────────────

def task_scrape(**kwargs):
    """Scrape les flux RSS Hespress + BBC + Al Jazeera."""
    _run_module("scrapers.hespress_scraper", "Scrape Hespress")
    _run_module("scrapers.bbc_aljazeera_scraper", "Scrape BBC+AlJazeera")
    return "SCRAPE_OK"

def task_bronze(**kwargs):
    """Valide et déduplique les articles de la couche Bronze."""
    return _run_module("pipeline.bronze_layer", "Bronze")


def task_silver(**kwargs):
    """
    Enrichissement NLP : scoring fake news, NER, similarité cross-sources,
    écriture Silver MinIO, envoi Kafka des articles flaggés.
    """
    try:
        from pipeline.silver_layer import run_silver_batch
    except ImportError as e:
        raise AirflowFailException(f"[Silver] Import error : {e}")
    run_silver_batch()
    return "SILVER_OK"


def task_gold(**kwargs):
    """
    Agrégation Gold : lecture Silver, upsert PostgreSQL,
    tables analytiques (articles_par_jour, alertes, tendances, etc.).
    """
    try:
        from pipeline.gold_layer import run_gold_batch
    except ImportError as e:
        raise AirflowFailException(f"[Gold] Import error : {e}")
    run_gold_batch()
    return "GOLD_OK"


def task_notify(**kwargs):
    """Récapitulatif de l'exécution."""
    ti = kwargs["ti"]
    silver = ti.xcom_pull(task_ids="silver_enrichment")
    gold = ti.xcom_pull(task_ids="gold_aggregation")
    msg = f"""
    ✅ FakeNews Pipeline — {kwargs['ds']}
       Silver : {silver}
       Gold   : {gold}
    """
    logger.info(msg)
    return msg


# ── DAG ──────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="fakenews_hourly_pipeline",
    default_args=default_args,
    description="Pipeline FakeNews : scraping RSS → Bronze → Silver NLP → Gold DW",
    schedule_interval="0 6 * * *",  # Chaque jour à 6h00 UTC (8h Maroc)
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    concurrency=2,
    tags=["fakenews", "scraping", "nlp", "maroc", "hourly"],
    doc_md="""
    ### 🗞️ FakeNews Detector — Pipeline Complet

    **Flow** : `scrape` → `bronze` → `silver` → `gold` → `notify`

    | Étape | Description | Fichier |
    |-------|-------------|---------|
    | Scrape | RSS Hespress → MinIO Bronze | `pipeline/scrapers/test_scraper.py` |
    | Bronze | Validation + déduplication | `pipeline/bronze_layer.py` |
    | Silver | NLP, scoring, similarité, Kafka | `pipeline/silver_layer.py` |
    | Gold | Agrégation → PostgreSQL | `pipeline/gold_layer.py` |

    **Schedule** : `0 * * * *` (toutes les heures)

    **⚠️ Docker** : modifier `localhost` → noms de service dans :
    - `test_scraper.py` : `MINIO_ENDPOINT = "http://minio:9000"`
    - `silver_layer.py` : `MINIO_ENDPOINT = "http://minio:9000"`, `KAFKA_BOOTSTRAP = "kafka:29092"`
    - `gold_layer.py` : `MINIO_ENDPOINT = "http://minio:9000"`, `PG_HOST = "postgres"`, `PG_PORT = 5432`
    """,
) as dag:

    start = EmptyOperator(task_id="start")

    scrape = PythonOperator(task_id="scrape_sources", python_callable=task_scrape)
    bronze = PythonOperator(task_id="bronze_validation", python_callable=task_bronze)
    silver = PythonOperator(task_id="silver_enrichment", python_callable=task_silver)
    gold = PythonOperator(task_id="gold_aggregation", python_callable=task_gold)
    notify = PythonOperator(task_id="notify", python_callable=task_notify)

    end = EmptyOperator(task_id="end")

    start >> scrape >> bronze >> silver >> gold >> notify >> end