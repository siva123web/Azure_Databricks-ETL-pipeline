"""
databricks/gold_layer.py
========================

GOLD LAYER — business-level aggregates, ready for BI.

Three Gold tables are produced from the Silver products dataset:

    1. ``gold_category_kpis``  — KPIs per product category
                                 (count, avg price, total inventory value, avg rating).
    2. ``gold_top_products``   — Top 10 highest-rated products with at least N reviews.
    3. ``gold_price_buckets``  — Distribution of products across price buckets.

The Gold layer is also exported to a SQLite database at ``data/serving.db`` so
``sql/queries.sql`` can be executed against it. In production this export step
would target **Azure SQL Database** via JDBC.
"""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path

from pyspark.sql import DataFrame, functions as F

try:
    from databricks._spark_session import get_spark
except ImportError:
    from _spark_session import get_spark  # type: ignore[no-redef]


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
PROJECT_ROOT: Path = Path(__file__).resolve().parent.parent
SILVER_PATH: str = str(PROJECT_ROOT / "data" / "silver" / "products")
GOLD_BASE: Path = PROJECT_ROOT / "data" / "gold"
SERVING_DB_PATH: Path = PROJECT_ROOT / "data" / "serving.db"

# Minimum reviews a product must have to qualify for "top products".
MIN_RATING_COUNT: int = 50


# ---------------------------------------------------------------------------
# Aggregations
# ---------------------------------------------------------------------------
def build_category_kpis(silver_df: DataFrame) -> DataFrame:
    """Per-category KPIs — the bread and butter of any e-commerce dashboard."""
    return (
        silver_df.filter(F.col("is_valid"))
        .groupBy("category")
        .agg(
            F.count("*").alias("product_count"),
            F.round(F.avg("price"), 2).alias("avg_price"),
            F.round(F.min("price"), 2).alias("min_price"),
            F.round(F.max("price"), 2).alias("max_price"),
            F.round(F.sum("price"), 2).alias("total_inventory_value"),
            F.round(F.avg("rating_rate"), 2).alias("avg_rating"),
            F.sum("rating_count").alias("total_reviews"),
        )
        .orderBy(F.col("total_inventory_value").desc())
        .withColumn("_gold_loaded_at", F.current_timestamp())
    )


def build_top_products(silver_df: DataFrame, min_reviews: int = MIN_RATING_COUNT) -> DataFrame:
    """Top 10 products by rating, restricted to those with enough reviews."""
    return (
        silver_df.filter(F.col("is_valid") & (F.col("rating_count") >= min_reviews))
        .select(
            "id",
            "title",
            "category",
            "price",
            "rating_rate",
            "rating_count",
        )
        .orderBy(F.col("rating_rate").desc(), F.col("rating_count").desc())
        .limit(10)
        .withColumn("_gold_loaded_at", F.current_timestamp())
    )


def build_price_buckets(silver_df: DataFrame) -> DataFrame:
    """Bucket products into price tiers for distribution analysis."""
    bucketed = silver_df.withColumn(
        "price_bucket",
        F.when(F.col("price") < 25, F.lit("0-25"))
        .when(F.col("price") < 75, F.lit("25-75"))
        .when(F.col("price") < 200, F.lit("75-200"))
        .otherwise(F.lit("200+")),
    )

    return (
        bucketed.groupBy("price_bucket")
        .agg(
            F.count("*").alias("product_count"),
            F.round(F.avg("price"), 2).alias("avg_price"),
        )
        # Sort the buckets in their natural order rather than alphabetically.
        .withColumn(
            "_sort_key",
            F.when(F.col("price_bucket") == "0-25", 1)
            .when(F.col("price_bucket") == "25-75", 2)
            .when(F.col("price_bucket") == "75-200", 3)
            .otherwise(4),
        )
        .orderBy("_sort_key")
        .drop("_sort_key")
        .withColumn("_gold_loaded_at", F.current_timestamp())
    )


# ---------------------------------------------------------------------------
# Persistence helpers
# ---------------------------------------------------------------------------
def write_gold(df: DataFrame, table_name: str) -> None:
    """Write a Gold DataFrame as a Delta table under ``data/gold/<table>``."""
    out_path = str(GOLD_BASE / table_name)
    print(f"[gold] Writing {table_name} to {out_path}")
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(out_path)
    )


def export_gold_to_sqlite(db_path: Path = SERVING_DB_PATH) -> None:
    """Copy each Gold Delta table into the serving SQLite database.

    Uses pandas to bridge Spark → SQLite. For real Azure SQL, swap the
    SQLite connection for ``pyodbc`` / ``sqlalchemy.create_engine("mssql+pyodbc://...")``
    and use ``df.write.format("jdbc")`` instead.
    """
    spark = get_spark("gold_export")

    db_path.parent.mkdir(parents=True, exist_ok=True)
    print(f"[gold] Exporting Gold tables to {db_path}")

    tables = ["gold_category_kpis", "gold_top_products", "gold_price_buckets"]

    with sqlite3.connect(db_path) as conn:
        for table in tables:
            delta_path = str(GOLD_BASE / table)
            if not os.path.isdir(delta_path):
                print(f"[gold] Skipping {table} — Delta path not found.")
                continue

            pdf = spark.read.format("delta").load(delta_path).toPandas()
            # ``replace`` keeps the export idempotent.
            pdf.to_sql(table, conn, if_exists="replace", index=False)
            print(f"[gold]   {table}: {len(pdf)} rows exported")

    spark.stop()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main() -> None:
    spark = get_spark("gold_layer")

    print(f"[gold] Reading Silver from {SILVER_PATH}")
    silver_df = spark.read.format("delta").load(SILVER_PATH)

    category_kpis = build_category_kpis(silver_df)
    top_products = build_top_products(silver_df)
    price_buckets = build_price_buckets(silver_df)

    print("[gold] gold_category_kpis preview:")
    category_kpis.show(truncate=False)

    print("[gold] gold_top_products preview:")
    top_products.show(truncate=80)

    print("[gold] gold_price_buckets preview:")
    price_buckets.show(truncate=False)

    write_gold(category_kpis, "gold_category_kpis")
    write_gold(top_products, "gold_top_products")
    write_gold(price_buckets, "gold_price_buckets")

    spark.stop()

    # Push to the serving DB so the SQL queries can run.
    export_gold_to_sqlite()
    print("[gold] OK")


if __name__ == "__main__":
    main()
