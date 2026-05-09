"""
bronze_layer.py — FakeNews Detector
====================================
Couche Bronze : validation et normalisation des données scrapées.

Comme le scraper écrit déjà en Bronze, ce module fait :
  1. Validation du schéma des articles
  2. Déduplication (par ID)
  3. Nettoyage basique
  4. Log des statistiques

Usage :
    python -m pipeline.bronze_layer
    ou via Airflow DAG
"""

import json
from datetime import datetime, timezone
from collections import defaultdict

import boto3
from botocore.client import Config
from typing import Tuple
import os
in_docker = os.environ.get("IN_DOCKER", "0") == "1"

# ── Configuration ────────────────────────────────────────────────────────────

MINIO_ENDPOINT   = "http://minio:9000" if in_docker else "http://localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
BUCKET           = "fakenews-lake"
PREFIX_BRONZE    = "bronze/"

# ── Client ───────────────────────────────────────────────────────────────────

def get_minio_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


# ── Validation ───────────────────────────────────────────────────────────────

REQUIRED_FIELDS = {"id", "titre", "source", "pays", "scraped_at"}

def validate_article(art: dict) -> Tuple[bool, str]:
    """Valide qu'un article a les champs minimum requis."""
    missing = REQUIRED_FIELDS - set(art.keys())
    if missing:
        return False, f"Champs manquants : {missing}"
    if not art.get("titre") or not art.get("contenu"):
        return False, "Titre ou contenu vide"
    return True, "OK"


# ── Pipeline Bronze ──────────────────────────────────────────────────────────

def run_bronze_batch():
    print("=" * 60)
    print("[Bronze] Démarrage validation couche Bronze")
    print(f"[Bronze] MinIO → {MINIO_ENDPOINT}")
    print("=" * 60)

    s3 = get_minio_client()

    # Lister tous les fichiers Bronze
    paginator = s3.get_paginator("list_objects_v2")
    files = []
    for page in paginator.paginate(Bucket=BUCKET, Prefix=PREFIX_BRONZE):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".json"):
                files.append(obj["Key"])

    print(f"[Bronze] {len(files)} fichiers trouvés en Bronze")

    if not files:
        print("[Bronze] ⚠️ Aucun fichier — lancez d'abord le scraper")
        return

    # Validation
    stats = defaultdict(int)
    ids_seen = set()
    duplicates = 0

    for key in files:
        try:
            resp = s3.get_object(Bucket=BUCKET, Key=key)
            art = json.loads(resp["Body"].read().decode("utf-8"))

            valid, msg = validate_article(art)
            if not valid:
                print(f"[Bronze] ⚠️ {key} : {msg}")
                stats["invalides"] += 1
                continue

            if art["id"] in ids_seen:
                duplicates += 1
                stats["doublons"] += 1
                continue

            ids_seen.add(art["id"])
            stats["valides"] += 1
            stats[f"source_{art.get('source', '?')}"] += 1

        except Exception as e:
            print(f"[Bronze] ❌ Erreur lecture {key} : {e}")
            stats["erreurs"] += 1

    print("\n" + "=" * 60)
    print("[Bronze] ✅ VALIDATION BRONZE TERMINÉE")
    print(f"  Articles valides : {stats['valides']}")
    print(f"  Invalides        : {stats.get('invalides', 0)}")
    print(f"  Doublons         : {stats.get('doublons', 0)}")
    print(f"  Erreurs lecture  : {stats.get('erreurs', 0)}")
    for k, v in sorted(stats.items()):
        if k.startswith("source_"):
            print(f"  {k.replace('source_', '  → ')} : {v}")
    print("=" * 60)


if __name__ == "__main__":
    run_bronze_batch()