-- ============================================================================
-- sql/schema.sql
-- ----------------------------------------------------------------------------
-- DDL for the SERVING layer.
--
-- This schema is consumed by Power BI / Excel / SSRS once the Gold Delta
-- tables are exported here from Databricks.
--
-- The script is written in a dialect that works on BOTH:
--   * SQLite (local demo)         -> sqlite3 data/serving.db < sql/schema.sql
--   * Azure SQL Database          -> connect via SSMS / sqlcmd and run the script
--
-- For a real Azure deployment you may want to:
--   * Replace DOUBLE / DECIMAL with the exact T-SQL types you prefer.
--   * Use schemas like ``[gold].[category_kpis]``.
--   * Add CLUSTERED COLUMNSTORE indexes for analytical workloads.
-- ============================================================================

-- Drop existing tables so the script is idempotent.
DROP TABLE IF EXISTS gold_category_kpis;
DROP TABLE IF EXISTS gold_top_products;
DROP TABLE IF EXISTS gold_price_buckets;

-- ----------------------------------------------------------------------------
-- 1. Per-category KPIs
-- ----------------------------------------------------------------------------
CREATE TABLE gold_category_kpis (
    category                TEXT        NOT NULL,
    product_count           INTEGER     NOT NULL,
    avg_price               DECIMAL(10, 2),
    min_price               DECIMAL(10, 2),
    max_price               DECIMAL(10, 2),
    total_inventory_value   DECIMAL(14, 2),
    avg_rating              DECIMAL(4, 2),
    total_reviews           INTEGER,
    _gold_loaded_at         TIMESTAMP
);

-- ----------------------------------------------------------------------------
-- 2. Top-rated products
-- ----------------------------------------------------------------------------
CREATE TABLE gold_top_products (
    id                      INTEGER     NOT NULL,
    title                   TEXT        NOT NULL,
    category                TEXT        NOT NULL,
    price                   DECIMAL(10, 2),
    rating_rate             DECIMAL(4, 2),
    rating_count            INTEGER,
    _gold_loaded_at         TIMESTAMP
);

-- ----------------------------------------------------------------------------
-- 3. Price-bucket distribution
-- ----------------------------------------------------------------------------
CREATE TABLE gold_price_buckets (
    price_bucket            TEXT        NOT NULL,
    product_count           INTEGER     NOT NULL,
    avg_price               DECIMAL(10, 2),
    _gold_loaded_at         TIMESTAMP
);

-- ----------------------------------------------------------------------------
-- Indexes (helpful on Azure SQL; SQLite ignores them gracefully)
-- ----------------------------------------------------------------------------
CREATE INDEX idx_category_kpis_category ON gold_category_kpis(category);
CREATE INDEX idx_top_products_category  ON gold_top_products(category);
CREATE INDEX idx_top_products_rating    ON gold_top_products(rating_rate);
