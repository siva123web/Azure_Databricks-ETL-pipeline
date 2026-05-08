-- ============================================================================
-- sql/queries.sql
-- ----------------------------------------------------------------------------
-- A set of analytical SQL queries that run against the serving layer
-- (Azure SQL Database in production, SQLite locally).
--
-- Run after ``gold_layer.py`` has populated the database:
--   sqlite3 data/serving.db < sql/queries.sql
-- ============================================================================


-- ----------------------------------------------------------------------------
-- Q1.  Top categories by total inventory value.
-- Useful to see where the catalog has the most "money on the shelf".
-- ----------------------------------------------------------------------------
SELECT
    category,
    product_count,
    avg_price,
    total_inventory_value
FROM gold_category_kpis
ORDER BY total_inventory_value DESC;


-- ----------------------------------------------------------------------------
-- Q2.  Average rating per category, sorted by best customer experience.
-- ----------------------------------------------------------------------------
SELECT
    category,
    avg_rating,
    total_reviews
FROM gold_category_kpis
WHERE total_reviews > 0
ORDER BY avg_rating DESC;


-- ----------------------------------------------------------------------------
-- Q3.  Top 10 highest-rated products overall.
-- ----------------------------------------------------------------------------
SELECT
    id,
    title,
    category,
    price,
    rating_rate,
    rating_count
FROM gold_top_products
ORDER BY rating_rate DESC, rating_count DESC
LIMIT 10;


-- ----------------------------------------------------------------------------
-- Q4.  Price-bucket distribution — how is the catalog priced?
-- ----------------------------------------------------------------------------
SELECT
    price_bucket,
    product_count,
    avg_price,
    ROUND(
        100.0 * product_count / SUM(product_count) OVER (),
        2
    ) AS pct_of_total
FROM gold_price_buckets;


-- ----------------------------------------------------------------------------
-- Q5.  Categories that punch above their weight — high rating + high revenue.
-- A simple composite score: avg_rating * total_inventory_value.
-- ----------------------------------------------------------------------------
SELECT
    category,
    avg_rating,
    total_inventory_value,
    ROUND(avg_rating * total_inventory_value, 2) AS rating_value_score
FROM gold_category_kpis
ORDER BY rating_value_score DESC;


-- ----------------------------------------------------------------------------
-- Q6.  Cross-table: best-rated product per category.
-- Demonstrates a window function (works on Azure SQL and SQLite >= 3.25).
-- ----------------------------------------------------------------------------
SELECT category, id, title, rating_rate, rating_count
FROM (
    SELECT
        t.*,
        ROW_NUMBER() OVER (
            PARTITION BY category
            ORDER BY rating_rate DESC, rating_count DESC
        ) AS rn
    FROM gold_top_products t
) ranked
WHERE rn = 1
ORDER BY category;
