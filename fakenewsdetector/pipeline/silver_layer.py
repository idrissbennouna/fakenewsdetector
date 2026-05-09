"""
Silver Layer — FakeNews Detector (version batch locale)
========================================================
Lit les JSON depuis MinIO Bronze, enrichit via NLP, écrit en Silver.

Exécution (depuis fakenewsdetector/) :
    uv run python -m pipeline.silver_layer

Endpoints locaux (hors Docker) :
    MinIO  → http://localhost:9000
    Kafka  → localhost:29092
"""

import io
import json
import time
from datetime import datetime, timezone
from typing import Optional
import boto3
from botocore.client import Config
from kafka import KafkaProducer

from pipeline.nlp_engine import FakeNewsEngine

import os
in_docker = os.environ.get("IN_DOCKER", "0") == "1"

# ── Configuration ──────────────────────────────────────────────────────────────

MINIO_ENDPOINT    = "http://minio:9000" if in_docker else "http://localhost:9000"
MINIO_ACCESS_KEY  = "minioadmin"
MINIO_SECRET_KEY  = "minioadmin"
BUCKET_BRONZE     = "fakenews-lake"
PREFIX_BRONZE     = "bronze/"
PREFIX_SILVER     = "silver/"

KAFKA_BOOTSTRAP   = "kafka:9092" if in_docker else "localhost:29092"
TOPIC_FLAGGED     = "flagged-articles"

# ── Clients ────────────────────────────────────────────────────────────────────

def get_minio_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def get_kafka_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        )
        print("[Silver] Kafka producer connecté")
        return producer
    except Exception as e:
        print(f"[Silver] ⚠️  Kafka non disponible ({e}) — les articles flaggés ne seront pas envoyés")
        return None


# ── Lecture Bronze ──────────────────────────────────────────────────────────────

def list_bronze_files(s3, already_processed: set) -> list:
    """Liste tous les JSON Bronze qui n'ont pas encore été traités."""
    paginator = s3.get_paginator("list_objects_v2")
    files = []
    for page in paginator.paginate(Bucket=BUCKET_BRONZE, Prefix=PREFIX_BRONZE):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".json") and key not in already_processed:
                files.append(key)
    return files


def load_bronze_article(s3, key: str) -> Optional[dict]:
    try:
        response = s3.get_object(Bucket=BUCKET_BRONZE, Key=key)
        content = response["Body"].read().decode("utf-8")
        return json.loads(content)
    except Exception as e:
        print(f"[Silver] ❌ Impossible de lire {key} : {e}")
        return None


# ── Écriture Silver ─────────────────────────────────────────────────────────────

def save_silver_article(s3, article: dict) -> str:
    """
    Sauvegarde l'article enrichi en Silver.
    Chemin : silver/pays=<pays>/langue=<langue>/<id>.json
    """
    pays   = article.get("pays", "inconnu").lower().replace(" ", "_")
    langue = article.get("langue_detectee", "fr")
    art_id = article.get("id", f"article_{int(time.time())}")

    # Convertir les champs complexes en JSON strings pour compatibilité Spark
    article_to_save = {**article}
    for field in ("entites", "articles_similaires"):
        if isinstance(article_to_save.get(field), (dict, list)):
            article_to_save[field] = json.dumps(article_to_save[field], ensure_ascii=False)

    # Ajouter métadonnées Silver
    article_to_save["silver_timestamp"] = datetime.now(timezone.utc).isoformat()
    article_to_save["pipeline_version"] = "1.0"

    key = f"{PREFIX_SILVER}pays={pays}/langue={langue}/{art_id}.json"
    body = json.dumps(article_to_save, ensure_ascii=False, indent=2).encode("utf-8")

    s3.put_object(
        Bucket=BUCKET_BRONZE,
        Key=key,
        Body=body,
        ContentType="application/json",
    )
    return key


# ── Pipeline principal ──────────────────────────────────────────────────────────

def run_silver_batch():
    print("=" * 60)
    print("[Silver] Démarrage du pipeline Silver Layer")
    print(f"[Silver] MinIO  → {MINIO_ENDPOINT}")
    print(f"[Silver] Kafka  → {KAFKA_BOOTSTRAP}")
    print("=" * 60)

    s3       = get_minio_client()
    producer = get_kafka_producer()
    engine   = FakeNewsEngine()

    # Vérifier quels fichiers Silver existent déjà (éviter doublons)
    already_processed = set()
    try:
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=BUCKET_BRONZE, Prefix=PREFIX_SILVER):
            for obj in page.get("Contents", []):
                # Retrouver le nom de l'article depuis le chemin silver
                fname = obj["Key"].split("/")[-1]
                already_processed.add(fname)
        print(f"[Silver] {len(already_processed)} articles déjà en Silver (seront ignorés)")
    except Exception as e:
        print(f"[Silver] ⚠️  Impossible de lire la couche Silver existante : {e}")

    # Lister les fichiers Bronze à traiter
    bronze_files = list_bronze_files(s3, set())  # on filtre manuellement après
    print(f"[Silver] {len(bronze_files)} fichiers trouvés en Bronze")

    # Filtrer ceux déjà traités (par nom de fichier)
    bronze_files = [
        f for f in bronze_files
        if f.split("/")[-1] not in already_processed
    ]
    print(f"[Silver] {len(bronze_files)} nouveaux articles à enrichir")

    if not bronze_files:
        print("[Silver] ✅ Rien à traiter — couche Silver à jour")
        return

    # Charger tous les articles pour update_corpus (similarité cross-sources)
    print("[Silver] Chargement des articles pour le corpus TF-IDF...")
    all_articles = []
    for key in bronze_files:
        art = load_bronze_article(s3, key)
        if art:
            all_articles.append(art)

    print(f"[Silver] Corpus chargé : {len(all_articles)} articles")
    engine.update_corpus(all_articles)

    # Enrichissement et sauvegarde
    stats = {"FIABLE": 0, "SUSPECT": 0, "FAKE": 0, "erreurs": 0}
    flagged_articles = []

    for i, article in enumerate(all_articles, 1):
        try:
            enriched = engine.enrich(article)
            silver_key = save_silver_article(s3, enriched)
            label = enriched["label"]
            stats[label] += 1

            # Log article par article
            print(
                f"[Silver] [{i:3}/{len(all_articles)}] "
                f"{label:7} | score={enriched['fakeness_score']:.3f} "
                f"| {enriched.get('source','?'):15} "
                f"| {enriched.get('titre','?')[:50]}"
            )

            if label in ("FAKE", "SUSPECT"):
                flagged_articles.append(enriched)

        except Exception as e:
            print(f"[Silver] ❌ Erreur sur article {article.get('id','?')} : {e}")
            stats["erreurs"] += 1

    # Envoyer les articles flaggés vers Kafka
    if flagged_articles and producer:
        print(f"\n[Silver] Envoi de {len(flagged_articles)} articles flaggés vers Kafka...")
        for art in flagged_articles:
            try:
                # Préparer un payload léger pour Kafka (sans les champs lourds)
                payload = {
                    "id":            art.get("id"),
                    "titre":         art.get("titre"),
                    "source":        art.get("source"),
                    "pays":          art.get("pays"),
                    "label":         art["label"],
                    "fakeness_score": art["fakeness_score"],
                    "corrobore":     art.get("corrobore_internationalement", False),
                    "timestamp":     art.get("silver_timestamp"),
                }
                producer.send(TOPIC_FLAGGED, value=payload)
            except Exception as e:
                print(f"[Silver] ⚠️  Erreur envoi Kafka : {e}")
        producer.flush()
        print(f"[Silver] ✅ {len(flagged_articles)} articles envoyés vers topic '{TOPIC_FLAGGED}'")

    # Résumé final
    print("\n" + "=" * 60)
    print("[Silver] ✅ PIPELINE SILVER TERMINÉ")
    print(f"  Articles traités : {len(all_articles)}")
    print(f"  FIABLE           : {stats['FIABLE']}")
    print(f"  SUSPECT          : {stats['SUSPECT']}")
    print(f"  FAKE             : {stats['FAKE']}")
    print(f"  Erreurs          : {stats['erreurs']}")
    print(f"  Flaggés Kafka    : {len(flagged_articles)}")
    print("=" * 60)

    if producer:
        producer.close()


if __name__ == "__main__":
    run_silver_batch()