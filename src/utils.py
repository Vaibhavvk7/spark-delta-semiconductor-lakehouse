from __future__ import annotations
from typing import Dict, Any
import yaml

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


def load_config(path: str = "configs/config.yaml") -> Dict[str, Any]:
    with open(path, "r") as f:
        return yaml.safe_load(f)


def build_spark(app_name: str) -> SparkSession:
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

