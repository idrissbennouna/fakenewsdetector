import json
import boto3
from datetime import datetime
from dateutil import parser as date_parser
from kafka import KafkaConsumer


consumer = KafkaConsumer(
    "raw-articles",
    bootstrap_servers=["localhost:29092"],
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="bronze-layer-group_v2",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
)

s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin",
)

BUCKET = "fakenews-lake"


def normalize_date(date_str) -> str:
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


def save_to_bronze(article: dict):
    # NORMALISER LA DATE DE PUBLICATION
    article["date_publication"] = normalize_date(article.get("date_publication"))
    
    date = datetime.utcnow()
    source = article.get("source", "unknown").replace(" ", "_")
    pays = article.get("pays", "inconnu").replace(" ", "_")
    langue = article.get("langue_detectee", "xx")

    # Partitionnement : source / pays / langue / date / id
    key = (
        f"bronze/{source}/{pays}/{langue}/"
        f"{date.strftime('%Y/%m/%d')}/"
        f"{article['id']}.json"
    )

    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=json.dumps(article, ensure_ascii=False),
        ContentType="application/json",
    )
    print(
        f"[Bronze] {source} [{langue}] "
        f"Date pub: {article['date_publication']} → {key}"
    )


print("[Consumer] En attente d'articles depuis Kafka...")

for message in consumer:
    try:
        save_to_bronze(message.value)
    except Exception as e:
        print(f"[Consumer] Erreur sauvegarde : {e}")