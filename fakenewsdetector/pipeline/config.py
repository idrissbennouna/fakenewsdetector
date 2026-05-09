"""
config.py - Configuration centralisee
Detecte automatiquement si on tourne dans Docker ou en local.
"""
import os

# Si IN_DOCKER=1 est defini, on utilise les noms de services Docker
IN_DOCKER = os.environ.get("IN_DOCKER", "0") == "1"

if IN_DOCKER:
    MINIO_ENDPOINT  = "http://minio:9000"
    KAFKA_BOOTSTRAP = "kafka:9092"
    PG_HOST         = "postgres"
    PG_PORT         = 5432
else:
    MINIO_ENDPOINT  = "http://localhost:9000"
    KAFKA_BOOTSTRAP = "localhost:29092"
    PG_HOST         = "localhost"
    PG_PORT         = 5433

MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
BUCKET           = "fakenews-lake"

PG_DB       = "fakenews_dw"
PG_USER     = "admin"
PG_PASSWORD = "admin"