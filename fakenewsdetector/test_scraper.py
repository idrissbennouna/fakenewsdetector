"""
test_scraper.py — FakeNews Detector
====================================
Scraper RSS multi-sources (Maroc + International).
Écrit les articles bruts dans MinIO (couche Bronze).

Sources Marocaines : Hespress
Sources Référence  : (à ajouter : BBC, Al Jazeera, etc.)

Usage :
    python -m pipeline.scrapers.test_scraper
    ou via Airflow DAG
"""

import json
import hashlib
import time
from datetime import datetime, timezone
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
import boto3
from botocore.client import Config

# ── Configuration MinIO ──────────────────────────────────────────────────────

MINIO_ENDPOINT   = "http://localhost:9000"   # ← changer pour "http://minio:9000" dans Docker
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
BUCKET_BRONZE    = "fakenews-lake"
PREFIX_BRONZE    = "bronze/"

# ── Sources RSS ──────────────────────────────────────────────────────────────

FEEDS = {
    # Sources marocaines
    "Hespress": {
        "url": "https://www.hespress.com/feed",
        "pays": "Maroc",
        "type": "marocaine",
        "langue": "fr",
    },
    # TODO : ajouter d'autres sources
    # "Akhbarona": {"url": "...", "pays": "Maroc", "type": "marocaine", "langue": "fr"},
    # "BBC": {"url": "http://feeds.bbci.co.uk/news/rss.xml", "pays": "Royaume-Uni", "type": "reference", "langue": "fr"},
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/rss+xml, application/xml, text/xml",
}

# ── Client MinIO ─────────────────────────────────────────────────────────────

def get_minio_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def ensure_bucket(s3):
    """Crée le bucket s'il n'existe pas."""
    try:
        s3.head_bucket(Bucket=BUCKET_BRONZE)
    except:
        s3.create_bucket(Bucket=BUCKET_BRONZE)
        print(f"[Scraper] Bucket '{BUCKET_BRONZE}' créé")


# ── Scraping RSS ─────────────────────────────────────────────────────────────

def fetch_rss(feed_url: str) -> str | None:
    """Télécharge le flux RSS."""
    try:
        r = requests.get(feed_url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        return r.text
    except Exception as e:
        print(f"[Scraper] ❌ Erreur fetch {feed_url} : {e}")
        return None


def parse_rss(xml_text: str, source_name: str, meta: dict) -> list:
    """Parse le XML RSS et extrait les articles."""
    articles = []
    soup = BeautifulSoup(xml_text, "lxml-xml")
    items = soup.find_all("item")

    print(f"[Scraper] {source_name} : {len(items)} articles trouvés")

    for item in items:
        try:
            title = item.find("title")
            title_text = title.get_text(strip=True) if title else "Sans titre"

            link = item.find("link")
            url = link.get_text(strip=True) if link else ""

            desc = item.find("description")
            description = desc.get_text(strip=True) if desc else ""

            pub_date = item.find("pubDate")
            date_pub = pub_date.get_text(strip=True) if pub_date else None

            # Générer un ID unique basé sur l'URL
            art_id = hashlib.md5(url.encode()).hexdigest()[:12]

            article = {
                "id": art_id,
                "titre": title_text,
                "contenu": description,  # Le RSS ne donne souvent qu'un résumé
                "source": source_name,
                "source_type": meta["type"],
                "pays": meta["pays"],
                "langue_detectee": meta["langue"],
                "url": url,
                "auteur": item.find("author").get_text(strip=True) if item.find("author") else None,
                "categorie": item.find("category").get_text(strip=True) if item.find("category") else "General",
                "date_publication": date_pub,
                "scraped_at": datetime.now(timezone.utc).isoformat(),
            }
            articles.append(article)
        except Exception as e:
            print(f"[Scraper] ⚠️ Erreur parsing item : {e}")
            continue

    return articles


# ── Écriture Bronze ──────────────────────────────────────────────────────────

def save_to_bronze(s3, article: dict) -> str:
    """Sauvegarde un article JSON dans MinIO (couche Bronze)."""
    source = article["source"].lower().replace(" ", "_")
    art_id = article["id"]
    date_str = datetime.now(timezone.utc).strftime("%Y/%m/%d")

    key = f"{PREFIX_BRONZE}{source}/{date_str}/{art_id}.json"
    body = json.dumps(article, ensure_ascii=False, indent=2).encode("utf-8")

    s3.put_object(
        Bucket=BUCKET_BRONZE,
        Key=key,
        Body=body,
        ContentType="application/json",
    )
    return key


# ── Pipeline principal ───────────────────────────────────────────────────────

def run_scraper():
    print("=" * 60)
    print("[Scraper] Démarrage du scraping RSS")
    print(f"[Scraper] MinIO → {MINIO_ENDPOINT}")
    print("=" * 60)

    s3 = get_minio_client()
    ensure_bucket(s3)

    total_articles = 0
    total_sources = 0

    for source_name, meta in FEEDS.items():
        xml = fetch_rss(meta["url"])
        if not xml:
            continue

        articles = parse_rss(xml, source_name, meta)
        total_sources += 1

        for art in articles:
            try:
                key = save_to_bronze(s3, art)
                total_articles += 1
                print(f"[Scraper] ✅ {art['id']} → {key}")
            except Exception as e:
                print(f"[Scraper] ❌ Erreur sauvegarde {art['id']} : {e}")

        # Respecter le serveur (rate limiting)
        time.sleep(1)

    print("\n" + "=" * 60)
    print("[Scraper] ✅ SCRAPING TERMINÉ")
    print(f"  Sources traitées : {total_sources}/{len(FEEDS)}")
    print(f"  Articles écrits  : {total_articles}")
    print("=" * 60)

    return total_articles


if __name__ == "__main__":
    run_scraper()