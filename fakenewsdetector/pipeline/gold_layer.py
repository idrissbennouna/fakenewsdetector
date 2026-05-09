"""
Gold Layer - FakeNews Detector (version batch locale)
======================================================
Lit les JSON enrichis depuis MinIO Silver, agrege et ecrit dans PostgreSQL.

Execution (depuis fakenewsdetector/) :
    uv run python -m pipeline.gold_layer

Dependances a installer si absent :
    uv add psycopg2-binary boto3

Endpoints locaux (hors Docker) :
    MinIO      -> http://localhost:9000
    PostgreSQL -> localhost:5432
"""

import json
import time
from collections import defaultdict
from datetime import datetime, timezone, date
from typing import Optional

import boto3
import psycopg2
import psycopg2.extras
from botocore.client import Config
import os
in_docker = os.environ.get("IN_DOCKER", "0") == "1"

# ── Configuration ──────────────────────────────────────────────────────────────

MINIO_ENDPOINT   = "http://minio:9000" if in_docker else "http://localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
BUCKET           = "fakenews-lake"
PREFIX_SILVER    = "silver/"

PG_HOST     = "postgres" if in_docker else "localhost"
PG_PORT     = 5432 if in_docker else 5433
PG_DB       = "fakenews_dw"
PG_USER     = "admin"
PG_PASSWORD = "admin"

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


def get_pg_connection():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    )


# ── Lecture Silver ─────────────────────────────────────────────────────────────

def load_all_silver_articles(s3) -> list:
    """Charge tous les JSON depuis la couche Silver MinIO."""
    paginator = s3.get_paginator("list_objects_v2")
    articles = []
    errors = 0

    for page in paginator.paginate(Bucket=BUCKET, Prefix=PREFIX_SILVER):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".json"):
                continue
            try:
                response = s3.get_object(Bucket=BUCKET, Key=key)
                content = response["Body"].read().decode("utf-8")
                art = json.loads(content)
                articles.append(art)
            except Exception as e:
                print(f"[Gold] [WARN]  Impossible de lire {key} : {e}")
                errors += 1

    print(f"[Gold] {len(articles)} articles Silver charges ({errors} erreurs)")
    return articles


# ── Helpers ────────────────────────────────────────────────────────────────────

def safe_ts(value) -> Optional[datetime]:
    """Convertit une chaine ISO ou None en datetime."""
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except Exception:
        return None


def safe_float(value, default=0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def truncate_day(ts: Optional[datetime]) -> Optional[datetime]:
    if ts is None:
        return None
    return ts.replace(hour=0, minute=0, second=0, microsecond=0)


# ── Ecriture PostgreSQL ────────────────────────────────────────────────────────

def upsert_articles_enrichis(cur, articles: list):
    """Table principale - tous les articles enrichis."""
    sql = """
        INSERT INTO articles_enrichis (
            id, titre, contenu, source, source_type, pays, langue_detectee,
            url, auteur, categorie, date_publication, scraped_at,
            fakeness_score, label, corrobore_internationalement,
            nb_sources_corroborantes, entites, articles_similaires,
            silver_timestamp
        ) VALUES (
            %(id)s, %(titre)s, %(contenu)s, %(source)s, %(source_type)s,
            %(pays)s, %(langue_detectee)s, %(url)s, %(auteur)s, %(categorie)s,
            %(date_publication)s, %(scraped_at)s, %(fakeness_score)s,
            %(label)s, %(corrobore_internationalement)s,
            %(nb_sources_corroborantes)s, %(entites)s, %(articles_similaires)s,
            %(silver_timestamp)s
        )
        ON CONFLICT (id) DO UPDATE SET
            fakeness_score               = EXCLUDED.fakeness_score,
            label                        = EXCLUDED.label,
            corrobore_internationalement = EXCLUDED.corrobore_internationalement,
            nb_sources_corroborantes     = EXCLUDED.nb_sources_corroborantes,
            entites                      = EXCLUDED.entites,
            articles_similaires          = EXCLUDED.articles_similaires,
            silver_timestamp             = EXCLUDED.silver_timestamp
    """
    rows = []
    for a in articles:
        # Serialiser les champs complexes si pas encore fait
        entites = a.get("entites", "{}")
        if isinstance(entites, (dict, list)):
            entites = json.dumps(entites, ensure_ascii=False)

        similaires = a.get("articles_similaires", "[]")
        if isinstance(similaires, (dict, list)):
            similaires = json.dumps(similaires, ensure_ascii=False)

        rows.append({
            "id":                           a.get("id", f"art_{int(time.time())}"),
            "titre":                        a.get("titre"),
            "contenu":                      (a.get("contenu") or "")[:5000],
            "source":                       a.get("source"),
            "source_type":                  a.get("source_type", "marocaine"),
            "pays":                         a.get("pays"),
            "langue_detectee":              a.get("langue_detectee", "fr"),
            "url":                          a.get("url"),
            "auteur":                       a.get("auteur"),
            "categorie":                    a.get("categorie"),
            "date_publication":             safe_ts(a.get("date_publication")),
            "scraped_at":                   safe_ts(a.get("scraped_at")),
            "fakeness_score":               safe_float(a.get("fakeness_score")),
            "label":                        a.get("label", "FIABLE"),
            "corrobore_internationalement": bool(a.get("corrobore_internationalement", False)),
            "nb_sources_corroborantes":     int(a.get("nb_sources_corroborantes", 0)),
            "entites":                      entites,
            "articles_similaires":          similaires,
            "silver_timestamp":             safe_ts(a.get("silver_timestamp")),
        })

    psycopg2.extras.execute_batch(cur, sql, rows, page_size=100)
    print(f"[Gold] [OK] {len(rows)} articles -> articles_enrichis")


def write_articles_par_jour(cur, articles: list):
    """Agregation : nb articles + score moyen par jour / source / label."""
    # Agregation Python
    agg = defaultdict(lambda: {"nb": 0, "scores": []})
    for a in articles:
        jour = truncate_day(safe_ts(a.get("date_publication")))
        key = (
            jour,
            a.get("source", "?"),
            a.get("pays", "?"),
            a.get("langue_detectee", "fr"),
            a.get("label", "FIABLE"),
        )
        agg[key]["nb"] += 1
        agg[key]["scores"].append(safe_float(a.get("fakeness_score")))

    # Vider la table avant re-insertion (overwrite)
    cur.execute("DELETE FROM articles_par_jour")

    sql = """
        INSERT INTO articles_par_jour
            (jour, source, pays, langue_detectee, label, nb_articles, score_moyen)
        VALUES (%(jour)s, %(source)s, %(pays)s, %(langue)s, %(label)s, %(nb)s, %(score)s)
    """
    rows = [
        {
            "jour":   k[0],
            "source": k[1],
            "pays":   k[2],
            "langue": k[3],
            "label":  k[4],
            "nb":     v["nb"],
            "score":  round(sum(v["scores"]) / len(v["scores"]), 3),
        }
        for k, v in agg.items()
    ]
    psycopg2.extras.execute_batch(cur, sql, rows)
    print(f"[Gold] [OK] {len(rows)} lignes -> articles_par_jour")


def write_comparaison_sources(cur, articles: list):
    """Agregation : comparaison presse marocaine vs internationale."""
    agg = defaultdict(lambda: {"nb": 0, "scores": [], "corrob": []})
    for a in articles:
        key = (
            a.get("categorie", "General"),
            a.get("source_type", "marocaine"),
            a.get("pays", "?"),
        )
        agg[key]["nb"] += 1
        agg[key]["scores"].append(safe_float(a.get("fakeness_score")))
        agg[key]["corrob"].append(1 if a.get("corrobore_internationalement") else 0)

    cur.execute("DELETE FROM comparaison_sources")

    sql = """
        INSERT INTO comparaison_sources
            (categorie, source_type, pays, nb_articles,
             score_moyen_fakeness, taux_corroboration)
        VALUES
            (%(categorie)s, %(source_type)s, %(pays)s, %(nb)s,
             %(score)s, %(taux)s)
    """
    rows = [
        {
            "categorie":   k[0],
            "source_type": k[1],
            "pays":        k[2],
            "nb":          v["nb"],
            "score":       round(sum(v["scores"]) / len(v["scores"]), 3),
            "taux":        round(sum(v["corrob"]) / len(v["corrob"]), 3),
        }
        for k, v in agg.items()
    ]
    psycopg2.extras.execute_batch(cur, sql, rows)
    print(f"[Gold] [OK] {len(rows)} lignes -> comparaison_sources")


def write_top_fake_sources(cur, articles: list):
    """Top sources marocaines avec le plus de fake news."""
    agg = defaultdict(lambda: {"nb": 0, "scores": []})
    for a in articles:
        if a.get("label") == "FAKE" and a.get("pays") == "Maroc":
            src = a.get("source", "?")
            agg[src]["nb"] += 1
            agg[src]["scores"].append(safe_float(a.get("fakeness_score")))

    cur.execute("DELETE FROM top_fake_sources_marocaines")

    if not agg:
        print("[Gold] [OK] 0 ligne -> top_fake_sources_marocaines (aucun FAKE detecte)")
        return

    sql = """
        INSERT INTO top_fake_sources_marocaines (source, nb_fake, score_moyen)
        VALUES (%(source)s, %(nb)s, %(score)s)
    """
    rows = [
        {
            "source": src,
            "nb":     v["nb"],
            "score":  round(sum(v["scores"]) / len(v["scores"]), 3),
        }
        for src, v in sorted(agg.items(), key=lambda x: -x[1]["nb"])
    ]
    psycopg2.extras.execute_batch(cur, sql, rows)
    print(f"[Gold] [OK] {len(rows)} lignes -> top_fake_sources_marocaines")


def write_alertes(cur, articles: list):
    """Articles avec fakeness_score > 0.6."""
    alertes = [a for a in articles if safe_float(a.get("fakeness_score")) > 0.60]

    sql = """
        INSERT INTO alertes_fake_news (
            id, titre, source, pays, langue_detectee, url,
            fakeness_score, label, date_publication,
            corrobore_internationalement, nb_sources_corroborantes,
            entites, scraped_at
        ) VALUES (
            %(id)s, %(titre)s, %(source)s, %(pays)s, %(langue)s, %(url)s,
            %(score)s, %(label)s, %(date_pub)s,
            %(corrob)s, %(nb_corrob)s,
            %(entites)s, %(scraped)s
        )
        ON CONFLICT (id) DO UPDATE SET
            fakeness_score = EXCLUDED.fakeness_score,
            label          = EXCLUDED.label
    """
    rows = []
    for a in alertes:
        entites = a.get("entites", "{}")
        if isinstance(entites, (dict, list)):
            entites = json.dumps(entites, ensure_ascii=False)
        rows.append({
            "id":        a.get("id", f"art_{int(time.time())}"),
            "titre":     a.get("titre"),
            "source":    a.get("source"),
            "pays":      a.get("pays"),
            "langue":    a.get("langue_detectee", "fr"),
            "url":       a.get("url"),
            "score":     safe_float(a.get("fakeness_score")),
            "label":     a.get("label", "FAKE"),
            "date_pub":  safe_ts(a.get("date_publication")),
            "corrob":    bool(a.get("corrobore_internationalement", False)),
            "nb_corrob": int(a.get("nb_sources_corroborantes", 0)),
            "entites":   entites,
            "scraped":   safe_ts(a.get("scraped_at")),
        })

    if rows:
        psycopg2.extras.execute_batch(cur, sql, rows)
    print(f"[Gold] [OK] {len(rows)} lignes -> alertes_fake_news")


def write_articles_sans_corroboration(cur, articles: list):
    """Articles marocains SUSPECT/FAKE sans corroboration internationale."""
    cibles = [
        a for a in articles
        if a.get("pays") == "Maroc"
        and not a.get("corrobore_internationalement", False)
        and safe_float(a.get("fakeness_score")) > 0.35
    ]

    sql = """
        INSERT INTO articles_sans_corroboration (
            id, titre, source, fakeness_score, label,
            date_publication, url, langue_detectee
        ) VALUES (
            %(id)s, %(titre)s, %(source)s, %(score)s, %(label)s,
            %(date_pub)s, %(url)s, %(langue)s
        )
        ON CONFLICT (id) DO UPDATE SET
            fakeness_score = EXCLUDED.fakeness_score,
            label          = EXCLUDED.label
    """
    rows = [
        {
            "id":      a.get("id", f"art_{int(time.time())}"),
            "titre":   a.get("titre"),
            "source":  a.get("source"),
            "score":   safe_float(a.get("fakeness_score")),
            "label":   a.get("label"),
            "date_pub": safe_ts(a.get("date_publication")),
            "url":     a.get("url"),
            "langue":  a.get("langue_detectee", "fr"),
        }
        for a in cibles
    ]
    if rows:
        psycopg2.extras.execute_batch(cur, sql, rows)
    print(f"[Gold] [OK] {len(rows)} lignes -> articles_sans_corroboration")


def write_tendances_par_langue(cur, articles: list):
    """Tendances par langue et label par jour."""
    agg = defaultdict(int)
    for a in articles:
        jour = truncate_day(safe_ts(a.get("date_publication")))
        key = (jour, a.get("langue_detectee", "fr"), a.get("label", "FIABLE"))
        agg[key] += 1

    cur.execute("DELETE FROM tendances_par_langue")

    sql = """
        INSERT INTO tendances_par_langue (jour, langue_detectee, label, nb_articles)
        VALUES (%(jour)s, %(langue)s, %(label)s, %(nb)s)
    """
    rows = [
        {"jour": k[0], "langue": k[1], "label": k[2], "nb": v}
        for k, v in agg.items()
    ]
    psycopg2.extras.execute_batch(cur, sql, rows)
    print(f"[Gold] [OK] {len(rows)} lignes -> tendances_par_langue")


# ── Pipeline principal ──────────────────────────────────────────────────────────

def run_gold_batch():
    print("=" * 60)
    print("[Gold] Demarrage du pipeline Gold Layer")
    print(f"[Gold] MinIO      -> {MINIO_ENDPOINT}")
    print(f"[Gold] PostgreSQL -> {PG_HOST}:{PG_PORT}/{PG_DB}")
    print("=" * 60)

    # Connexion MinIO
    s3 = get_minio_client()

    # Connexion PostgreSQL
    try:
        conn = get_pg_connection()
        conn.autocommit = False
        print("[Gold] [OK] Connecte a PostgreSQL")
    except Exception as e:
        print(f"[Gold] [ERR] Impossible de se connecter a PostgreSQL : {e}")
        print("[Gold] [TIP] Verifie que Docker est demarre : docker compose up -d")
        return

    # Chargement des articles Silver
    articles = load_all_silver_articles(s3)
    if not articles:
        print("[Gold] [WARN]  Aucun article Silver trouve - lance d'abord silver_layer")
        conn.close()
        return

    # Ecriture dans PostgreSQL (transaction unique)
    try:
        with conn.cursor() as cur:
            upsert_articles_enrichis(cur, articles)
            write_articles_par_jour(cur, articles)
            write_comparaison_sources(cur, articles)
            write_top_fake_sources(cur, articles)
            write_alertes(cur, articles)
            write_articles_sans_corroboration(cur, articles)
            write_tendances_par_langue(cur, articles)

        conn.commit()
        print("\n[Gold] [OK] Transaction commitee")

    except Exception as e:
        conn.rollback()
        print(f"\n[Gold] [ERR] Erreur - rollback effectue : {e}")
        raise
    finally:
        conn.close()

    # Resume
    labels = defaultdict(int)
    for a in articles:
        labels[a.get("label", "?")] += 1

    print("\n" + "=" * 60)
    print("[Gold] [OK] PIPELINE GOLD TERMINE")
    print(f"  Articles traites : {len(articles)}")
    for label, nb in sorted(labels.items()):
        print(f"  {label:10} : {nb}")
    print(f"  Tables ecrites   : 7")
    print("=" * 60)


if __name__ == "__main__":
    run_gold_batch()