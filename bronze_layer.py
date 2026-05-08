"""
databricks/bronze_layer.py
==========================

BRONZE LAYER — raw, schema-on-read.

In a real Databricks workspace this would be a notebook. We've kept it as a
plain ``.py`` file so it runs anywhere (local PySpark **or** Databricks).
On Databricks just paste each block into a notebook cell.

What this layer does
--------------------
* Reads every JSON file under ``data/raw/products/`` (all partitions).
* Adds a few audit columns (``_source_file``, ``_bronze_loaded_at``).
* Writes the data **as-is** to a Delta table in ``data/bronze/products``.

Why "as-is"?
    The Bronze layer is the **single source of truth** of what the source returned.
    Any transformations (cleaning, deduplication, joins) belong to Silver/Gold.
    This makes it possible to *replay* downstream layers without re-hitting the API.
"""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, functions as F

# When run locally we share a SparkSession helper. On Databricks ``spark`` is
# already a global, so we keep this import lazy.
try:
    from databricks._spark_session import get_spark
except ImportError:  # running from inside the ``databricks`` folder
    from _spark_session import get_spark  # type: ignore[no-redef]


# ---------------------------------------------------------------------------
# Configuration — replace with abfss:// paths when running on Azure.
# ---------------------------------------------------------------------------
PROJECT_ROOT: Path = Path(__file__).resolve().parent.parent
RAW_PATH: str = str(PROJECT_ROOT / "data" / "raw" / "products")
BRONZE_PATH: str = str(PROJECT_ROOT / "data" / "bronze" / "products")


def read_raw_json(spark, raw_path: str) -> DataFrame:
    """Load every JSON file under ``raw_path`` into a Spark DataFrame.

    ``recursiveFileLookup`` lets us pick up files inside the
    ``ingest_date=YYYY-MM-DD`` partitions written by ``ingest_api.py``.
    ``multiLine=true`` is required because each file contains a JSON *array*.
    """
    print(f"[bronze] Reading raw JSON from {raw_path}")
    return (
        spark.read.option("multiLine", "true")
        .option("recursiveFileLookup", "true")
        .json(raw_path)
        # ``input_file_name()`` keeps lineage for debugging / re-processing.
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_bronze_loaded_at", F.current_timestamp())
    )


def write_bronze_delta(df: DataFrame, bronze_path: str) -> None:
    """Persist the DataFrame as a Delta table in ``overwrite`` mode.

    For the portfolio demo we overwrite each run so it stays idempotent.
    In production you'd typically use ``append`` + ``MERGE`` for upserts.
    """
    print(f"[bronze] Writing Delta table to {bronze_path}")
    (
        df.write.format("delta")
        .mode("overwrite")
        # ``mergeSchema`` lets new fields from the API flow through automatically.
        .option("mergeSchema", "true")
        .save(bronze_path)
    )


def main() -> None:
    spark = get_spark("bronze_layer")

    raw_df = read_raw_json(spark, RAW_PATH)
    print(f"[bronze] Raw row count: {raw_df.count()}")
    raw_df.printSchema()

    write_bronze_delta(raw_df, BRONZE_PATH)

    # Quick sanity check — read it back.
    bronze_df = spark.read.format("delta").load(BRONZE_PATH)
    print(f"[bronze] Bronze row count: {bronze_df.count()}")
    bronze_df.show(5, truncate=False)

    spark.stop()
    print("[bronze] OK")


if __name__ == "__main__":
    main()
