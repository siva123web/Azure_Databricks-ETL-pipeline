"""
databricks/silver_layer.py
==========================

SILVER LAYER — cleaned & conformed.

This stage takes the Bronze Delta table and produces a **trusted, query-ready**
dataset. Typical Silver work for our e-commerce dataset:

    * Drop rows where required fields are NULL (``id``, ``title``, ``price``).
    * Cast columns to proper Spark types (``id`` → int, ``price`` → decimal).
    * Flatten the nested ``rating`` struct into ``rating_rate`` and ``rating_count``.
    * Trim/lowercase string fields and standardise the ``category`` column.
    * Deduplicate on the natural key (``id``) keeping the latest record.
    * Add quality flags (``is_valid``, ``has_rating``).

Output: Delta table at ``data/silver/products`` with a clean, stable schema.
"""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, functions as F, Window
from pyspark.sql.types import DecimalType

try:
    from databricks._spark_session import get_spark
except ImportError:
    from _spark_session import get_spark  # type: ignore[no-redef]


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
PROJECT_ROOT: Path = Path(__file__).resolve().parent.parent
BRONZE_PATH: str = str(PROJECT_ROOT / "data" / "bronze" / "products")
SILVER_PATH: str = str(PROJECT_ROOT / "data" / "silver" / "products")


def read_bronze(spark) -> DataFrame:
    """Read the Bronze Delta table written by ``bronze_layer.py``."""
    print(f"[silver] Reading Bronze from {BRONZE_PATH}")
    return spark.read.format("delta").load(BRONZE_PATH)


def clean_products(bronze_df: DataFrame) -> DataFrame:
    """Apply all Silver-level cleansing rules.

    The function is split into small, well-named ``withColumn`` calls so each
    transformation is easy to read, test, and audit.
    """

    # 1. Required fields cannot be null. Drop rows that fail this contract.
    cleaned = bronze_df.dropna(subset=["id", "title", "price", "category"])

    # 2. Type-cast numeric columns explicitly. Decimal(10,2) for currency.
    cleaned = (
        cleaned
        .withColumn("id", F.col("id").cast("int"))
        .withColumn("price", F.col("price").cast(DecimalType(10, 2)))
    )

    # 3. Normalize string fields — trim whitespace and lower-case the category
    #    so that "Men's clothing" and "men's clothing" don't double-count later.
    cleaned = (
        cleaned
        .withColumn("title", F.trim(F.col("title")))
        .withColumn("description", F.trim(F.col("description")))
        .withColumn("category", F.lower(F.trim(F.col("category"))))
    )

    # 4. Flatten the nested ``rating`` struct (rating.rate, rating.count).
    #    The FakeStore API returns: {"rate": 3.9, "count": 120}
    cleaned = (
        cleaned
        .withColumn("rating_rate", F.col("rating.rate").cast("double"))
        .withColumn("rating_count", F.col("rating.count").cast("int"))
        .drop("rating")
    )

    # 5. Quality flags — useful for downstream filtering + dashboards.
    cleaned = (
        cleaned
        .withColumn("has_rating", F.col("rating_rate").isNotNull())
        .withColumn(
            "is_valid",
            (F.col("price") > 0) & (F.length(F.col("title")) > 0),
        )
    )

    # 6. Deduplicate on the natural key. If the API returned the same id twice
    #    in different ingestion runs, keep the most recently ingested row.
    window = Window.partitionBy("id").orderBy(F.col("_ingested_at").desc())
    cleaned = (
        cleaned.withColumn("_row_number", F.row_number().over(window))
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
    )

    # 7. Audit column for the Silver layer itself.
    cleaned = cleaned.withColumn("_silver_loaded_at", F.current_timestamp())

    return cleaned


def write_silver_delta(df: DataFrame, silver_path: str) -> None:
    """Persist the curated DataFrame as a Delta table.

    We partition by ``category`` because most analytical queries filter or
    group on it, and the cardinality is low (~4 categories).
    """
    print(f"[silver] Writing Silver Delta to {silver_path}")
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("category")
        .save(silver_path)
    )


def main() -> None:
    spark = get_spark("silver_layer")

    bronze_df = read_bronze(spark)
    silver_df = clean_products(bronze_df)

    print(f"[silver] Silver row count: {silver_df.count()}")
    silver_df.printSchema()
    silver_df.show(5, truncate=80)

    write_silver_delta(silver_df, SILVER_PATH)
    spark.stop()
    print("[silver] OK")


if __name__ == "__main__":
    main()
