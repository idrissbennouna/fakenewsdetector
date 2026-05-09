import os

# Force PostgreSQL comme base de donnees Superset
SQLALCHEMY_DATABASE_URI = os.environ.get(
    "SQLALCHEMY_DATABASE_URI",
    "postgresql+psycopg2://admin:admin@postgres:5432/superset_db"
)

# Cle secrete
SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "fakenews-maroc-secret-key-2025")

# Desactiver SQLite
PREVENT_UNSAFE_DB_CONNECTIONS = False