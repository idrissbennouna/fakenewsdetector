import requests
from bs4 import BeautifulSoup
from datetime import datetime
from kafka import KafkaProducer
from langdetect import detect
from dateutil import parser as date_parser
import json
import hashlib
import os


class BaseScraper:
    def __init__(self, source_name: str, base_url: str, pays: str = "Inconnu"):
        self.source_name = source_name
        self.base_url = base_url
        self.pays = pays
        
        in_docker = os.environ.get("IN_DOCKER", "0") == "1"
        kafka_server = "kafka:9092" if in_docker else "localhost:29092"
        
        self.producer = KafkaProducer(
            bootstrap_servers=[kafka_server],
            value_serializer=lambda v: json.dumps(
                v, ensure_ascii=False
            ).encode("utf-8"),
        )

    def fetch_page(self, url: str) -> BeautifulSoup:
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            )
        }
        resp = requests.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
        return BeautifulSoup(resp.text, "html.parser")

    def detect_language(self, text: str) -> str:
        """Détecte la langue du contenu : ar, fr, en, etc."""
        try:
            return detect(text[:500])
        except Exception:
            return "inconnu"

    def normalize_date(self, date_str: str) -> str:
        """
        Normalise n'importe quelle date en format YYYY-MM-DD.
        Gère les formats RSS (RFC 2822), ISO 8601, et les chaînes vides/invalides.
        """
        if not date_str or str(date_str).strip() in ("N/A", "", "null", "None"):
            return datetime.utcnow().strftime('%Y-%m-%d')
        
        try:
            dt = date_parser.parse(date_str)
            return dt.strftime('%Y-%m-%d')
        except Exception:
            return datetime.utcnow().strftime('%Y-%m-%d')

    def build_article(
        self,
        title: str,
        content: str,
        author: str,
        date: str,
        category: str,
        url: str,
    ) -> dict:
        langue = self.detect_language(content or title)
        date_norm = self.normalize_date(date)
        
        return {
            "id": hashlib.md5(url.encode()).hexdigest(),
            "titre": title,
            "auteur": author or "Inconnu",
            "date_publication": date_norm,
            "categorie": category or "Non classé",
            "contenu": content,
            "source": self.source_name,
            "pays": self.pays,
            "langue_detectee": langue,
            "url": url,
            "scraped_at": datetime.utcnow().isoformat(),
        }

    def send_to_kafka(self, article: dict, topic: str = "raw-articles"):
        self.producer.send(topic, value=article)
        self.producer.flush()
        print(
            f"[{self.source_name}] [{article['langue_detectee'].upper()}] "
            f"Date: {article['date_publication']} | "
            f"Envoyé : {article['titre'][:60]}..."
        )

    def scrape(self):
        raise NotImplementedError("Implémenter dans la sous-classe")