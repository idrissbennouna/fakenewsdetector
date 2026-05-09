"""
Silver Layer — FakeNews Detector (version Spark Streaming — Docker)
====================================================================
Version Spark Structured Streaming à exécuter DANS le container Spark.

⚠️  NE PAS lancer avec `uv run` — nécessite spark-submit dans Docker.

Commande de lancement :
    docker exec -it kafka-spark-1 spark-submit \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4 \
        /opt/spark/work-dir/silver_layer_spark.py

Endpoints internes Docker (pas localhost) :
    MinIO  → http://minio:9000
    Kafka  → kafka:9092
"""

import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# ── Session Spark ──────────────────────────────────────────────────────────────

spark = SparkSession.builder \
    .appName("FakeNews-Silver-Maroc") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
        "org.apache.hadoop:hadoop-aws:3.3.4",
    ) \
    .config("spark.hadoop.fs.s3a.endpoint",          "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key",         "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key",         "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access",  "true") \
    .config("spark.hadoop.fs.s3a.impl",               "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Import APRÈS création de la session Spark
from pipeline.nlp_engine import FakeNewsEngine  # noqa: E402
engine = FakeNewsEngine()


# ── Traitement par batch ────────────────────────────────────────────────────────

def process_silver(batch_df, epoch_id):
    if batch_df.isEmpty():
        print(f"[Silver] Epoch {epoch_id} — batch vide")
        return

    records = [json.loads(row.value) for row in batch_df.collect()]
    print(f"[Silver] Epoch {epoch_id} — {len(records)} articles reçus")

    # Mise à jour du corpus pour la similarité cross-sources
    engine.update_corpus(records)

    enriched = []
    for art in records:
        try:
            result = engine.enrich(art)

            # Sérialiser les champs complexes (dict/list → str) pour Spark
            for field in ("entites", "articles_similaires"):
                if isinstance(result.get(field), (dict, list)):
                    result[field] = json.dumps(result[field], ensure_ascii=False)

            enriched.append(result)
        except Exception as e:
            print(f"[Silver] Erreur enrichissement article {art.get('id','?')} : {e}")

    if not enriched:
        return

    df_enriched = spark.createDataFrame(enriched)

    # Écriture couche Silver — Parquet partitionné par pays et langue
    df_enriched.write \
        .partitionBy("pays", "langue_detectee") \
        .mode("append") \
        .parquet("s3a://fakenews-lake/silver/")

    print(f"[Silver] ✅ {len(enriched)} articles écrits en Silver (Parquet)")

    # Articles suspects → topic Kafka dédié pour alerte temps réel
    flagged = [a for a in enriched if a["label"] in ("FAKE", "SUSPECT")]
    if flagged:
        # Payload léger pour Kafka
        kafka_payloads = [
            {
                "id":             a.get("id"),
                "titre":          a.get("titre"),
                "source":         a.get("source"),
                "pays":           a.get("pays"),
                "label":          a["label"],
                "fakeness_score": a["fakeness_score"],
                "corrobore":      a.get("corrobore_internationalement", False),
            }
            for a in flagged
        ]
        df_flagged = spark.createDataFrame(kafka_payloads)
        df_flagged.selectExpr("to_json(struct(*)) AS value") \
            .write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:9092") \
            .option("topic", "flagged-articles") \
            .save()
        print(f"[Silver] 🚨 {len(flagged)} articles flaggés → topic 'flagged-articles'")

    # Stats rapides
    from collections import Counter
    label_stats = Counter(a["label"] for a in enriched)
    pays_stats  = Counter(a.get("pays", "?") for a in enriched)
    print(f"[Silver] Labels : {dict(label_stats)} | Pays : {dict(pays_stats)}")


# ── Lecture Kafka Streaming ─────────────────────────────────────────────────────

df_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "raw-articles") \
    .option("startingOffsets", "latest") \
    .option("maxOffsetsPerTrigger", "100") \
    .load() \
    .selectExpr("CAST(value AS STRING)")

query = df_stream.writeStream \
    .foreachBatch(process_silver) \
    .outputMode("append") \
    .trigger(processingTime="30 seconds") \
    .option("checkpointLocation", "/tmp/checkpoint/silver") \
    .start()

print("[Silver] 🚀 Spark Streaming démarré — en attente d'articles...")
query.awaitTermination()