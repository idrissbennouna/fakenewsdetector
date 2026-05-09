-- ============================================================
-- FakeNews Detector — Data Warehouse PostgreSQL
-- init.sql : création des tables Gold Layer
-- ============================================================

-- Base de données déjà créée par Docker (POSTGRES_DB=fakenews_dw)
-- Ce script crée uniquement les tables

-- ── Table 1 : articles par jour, source et label ─────────────────────────────
CREATE TABLE IF NOT EXISTS articles_par_jour (
    id                  SERIAL PRIMARY KEY,
    jour                TIMESTAMP,
    source              VARCHAR(100),
    pays                VARCHAR(100),
    langue_detectee     VARCHAR(10),
    label               VARCHAR(20),
    nb_articles         INTEGER,
    score_moyen         NUMERIC(5, 3),
    inserted_at         TIMESTAMP DEFAULT NOW()
);

-- ── Table 2 : comparaison presse marocaine vs internationale ─────────────────
CREATE TABLE IF NOT EXISTS comparaison_sources (
    id                      SERIAL PRIMARY KEY,
    categorie               VARCHAR(200),
    source_type             VARCHAR(50),
    pays                    VARCHAR(100),
    nb_articles             INTEGER,
    score_moyen_fakeness    NUMERIC(5, 3),
    taux_corroboration      NUMERIC(5, 3),
    inserted_at             TIMESTAMP DEFAULT NOW()
);

-- ── Table 3 : top sources marocaines qui diffusent des fake news ─────────────
CREATE TABLE IF NOT EXISTS top_fake_sources_marocaines (
    id          SERIAL PRIMARY KEY,
    source      VARCHAR(100),
    nb_fake     INTEGER,
    score_moyen NUMERIC(5, 3),
    inserted_at TIMESTAMP DEFAULT NOW()
);

-- ── Table 4 : alertes fake news (score > 0.6) ────────────────────────────────
CREATE TABLE IF NOT EXISTS alertes_fake_news (
    id                          VARCHAR(200) PRIMARY KEY,
    titre                       TEXT,
    source                      VARCHAR(100),
    pays                        VARCHAR(100),
    langue_detectee             VARCHAR(10),
    url                         TEXT,
    fakeness_score              NUMERIC(5, 3),
    label                       VARCHAR(20),
    date_publication            TIMESTAMP,
    corrobore_internationalement BOOLEAN,
    nb_sources_corroborantes    INTEGER,
    entites                     TEXT,   -- JSON sérialisé
    scraped_at                  TIMESTAMP,
    inserted_at                 TIMESTAMP DEFAULT NOW()
);

-- ── Table 5 : articles sans corroboration internationale ─────────────────────
CREATE TABLE IF NOT EXISTS articles_sans_corroboration (
    id                  VARCHAR(200) PRIMARY KEY,
    titre               TEXT,
    source              VARCHAR(100),
    fakeness_score      NUMERIC(5, 3),
    label               VARCHAR(20),
    date_publication    TIMESTAMP,
    url                 TEXT,
    langue_detectee     VARCHAR(10),
    inserted_at         TIMESTAMP DEFAULT NOW()
);

-- ── Table 6 : tendances par langue ───────────────────────────────────────────
CREATE TABLE IF NOT EXISTS tendances_par_langue (
    id              SERIAL PRIMARY KEY,
    jour            TIMESTAMP,
    langue_detectee VARCHAR(10),
    label           VARCHAR(20),
    nb_articles     INTEGER,
    inserted_at     TIMESTAMP DEFAULT NOW()
);

-- ── Table 7 : tous les articles enrichis (vue complète pour Superset) ────────
CREATE TABLE IF NOT EXISTS articles_enrichis (
    id                          VARCHAR(200) PRIMARY KEY,
    titre                       TEXT,
    contenu                     TEXT,
    source                      VARCHAR(100),
    source_type                 VARCHAR(50),
    pays                        VARCHAR(100),
    langue_detectee             VARCHAR(10),
    url                         TEXT,
    auteur                      VARCHAR(200),
    categorie                   VARCHAR(200),
    date_publication            TIMESTAMP,
    scraped_at                  TIMESTAMP,
    fakeness_score              NUMERIC(5, 3),
    label                       VARCHAR(20),
    corrobore_internationalement BOOLEAN,
    nb_sources_corroborantes    INTEGER,
    entites                     TEXT,   -- JSON sérialisé
    articles_similaires         TEXT,   -- JSON sérialisé
    silver_timestamp            TIMESTAMP,
    inserted_at                 TIMESTAMP DEFAULT NOW()
);

-- ── Index pour les requêtes Superset ─────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_articles_source     ON articles_enrichis(source);
CREATE INDEX IF NOT EXISTS idx_articles_label      ON articles_enrichis(label);
CREATE INDEX IF NOT EXISTS idx_articles_pays       ON articles_enrichis(pays);
CREATE INDEX IF NOT EXISTS idx_articles_date       ON articles_enrichis(date_publication);
CREATE INDEX IF NOT EXISTS idx_alertes_score       ON alertes_fake_news(fakeness_score);

-- ── Message de confirmation ───────────────────────────────────────────────────
DO $$
BEGIN
    RAISE NOTICE '✅ FakeNews DW — toutes les tables créées avec succès';
END $$;