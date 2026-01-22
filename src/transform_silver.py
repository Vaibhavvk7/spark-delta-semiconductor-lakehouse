from __future__ import annotations
from pyspark.sql import functions as F
from pyspark.sql import Window

from src.utils import build_spark, load_config


def main():
    cfg = load_config()
    spark = build_spark(cfg["app"]["name"])

    bronze_path = cfg["paths"]["bronze"]
    silver_path = cfg["paths"]["silver"]
    c = cfg["columns"]

    df = spark.read.format("delta").load(bronze_path)

    # Parse timestamp + derive date
    df2 = (
        df.withColumn("event_ts", F.to_timestamp(F.col(c["event_ts"])))
          .withColumn("event_date", F.to_date(F.col("event_ts")))
    )

    # Basic cleaning / sanity ranges (tune if you want)
    df2 = df2.filter(
        (F.col("event_ts").isNotNull()) &
        (F.col(c["tool_id"]).isNotNull()) &
        (F.col(c["wafer_id"]).isNotNull()) &
        (F.col(c["process_step"]).isNotNull()) &
        (F.col(c["temperature_c"]).between(200, 600)) &
        (F.col(c["pressure_pa"]) > 0) &
        (F.col(c["voltage_v"]) > 0) &
        (F.col(c["current_a"]) > 0) &
        (F.col(c["vibration_level"]) >= 0) &
        (F.col(c["defect_count"]) >= 0) &
        (F.col(c["yield_flag"]).isin([0, 1]))
    )

    # Dedup by (tool_id, wafer_id, process_step, event_ts) keeping latest ingest
    df2 = df2.withColumn(
        "row_key",
        F.sha2(F.concat_ws("||",
                           F.col(c["tool_id"]).cast("string"),
                           F.col(c["wafer_id"]).cast("string"),
                           F.col(c["process_step"]).cast("string"),
                           F.col("event_ts").cast("string")), 256)
    )
    w = Window.partitionBy("row_key").orderBy(F.col("_ingested_at").desc())
    df2 = df2.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")

    (df2.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("event_date")
        .save(silver_path))

    print(f"Wrote silver: {silver_path}")


if __name__ == "__main__":
    main()

