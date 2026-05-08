"""
databricks/_spark_session.py
============================

Helper that returns a SparkSession configured with **Delta Lake**.

On a real Azure Databricks cluster you wouldn't need any of this — Spark and
Delta are pre-installed, and ``spark`` is a global. But because we want this
project to run **locally** for portfolio reviewers, we build the session here.
"""

from __future__ import annotations

from pyspark.sql import SparkSession


def get_spark(app_name: str = "azure-databricks-portfolio") -> SparkSession:
    """Return a SparkSession with the Delta Lake extensions wired in.

    The configuration mirrors what Databricks Runtime sets by default:
    - ``spark.sql.extensions`` enables Delta SQL syntax (``MERGE``, ``OPTIMIZE`` …).
    - ``spark.sql.catalog.spark_catalog`` registers the Delta catalog so
      ``saveAsTable`` and ``CREATE TABLE`` go through Delta.
    """
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # Quieter logs, friendlier for demos.
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.shuffle.partitions", "4")  # tiny dataset
    )

    # ``configure_spark_with_delta_pip`` automatically adds the right
    # delta-spark JAR at the matching version.
    try:
        from delta import configure_spark_with_delta_pip
        builder = configure_spark_with_delta_pip(builder)
    except ImportError:
        # Fallback: assume the JAR is already on the classpath (e.g. on Databricks).
        pass

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
