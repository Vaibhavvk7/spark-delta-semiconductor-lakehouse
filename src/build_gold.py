from __future__ import annotations
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast
from pyspark.sql import Row

from src.utils import build_spark, load_config


def main():
    cfg = load_config()
    spark = build_spark(cfg["app"]["name"])
    c = cfg["columns"]

    silver = cfg["paths"]["silver"]
    gold_tool = cfg["paths"]["gold_tool_daily"]
    gold_step = cfg["paths"]["gold_step_daily"]

    df = spark.read.format("delta").load(silver)

    # Small "tool metadata" dim to show broadcast join (optimization signal)
    tool_dim = spark.createDataFrame([
        Row(tool_id="ETCH_01", tool_family="ETCH"),
        Row(tool_id="ETCH_02", tool_family="ETCH"),
        Row(tool_id="CVD_01", tool_family="CVD"),
        Row(tool_id="CVD_02", tool_family="CVD"),
        Row(tool_id="IMPLANT_01", tool_family="IMPLANT"),
    ])

    df = df.join(broadcast(tool_dim), on=c["tool_id"], how="left")

    # Tool daily metrics (yield, defects, sensor stats)
    tool_daily = (
        df.groupBy("event_date", c["tool_id"], "tool_family")
          .agg(
              F.count("*").alias("events"),
              F.avg(F.col(c["yield_flag"]).cast("double")).alias("yield_rate"),
              F.avg(F.col(c["defect_count"]).cast("double")).alias("avg_defects"),
              F.percentile_approx(c["temperature_c"], 0.95).alias("temp_p95"),
              F.percentile_approx(c["vibration_level"], 0.95).alias("vibration_p95")
          )
    )

    (tool_daily.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("event_date")
        .save(gold_tool))

    # Step daily metrics
    step_daily = (
        df.groupBy("event_date", c["process_step"])
          .agg(
              F.count("*").alias("events"),
              F.avg(F.col(c["yield_flag"]).cast("double")).alias("yield_rate"),
              F.avg(F.col(c["defect_count"]).cast("double")).alias("avg_defects")
          )
    )

    (step_daily.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("event_date")
        .save(gold_step))

    # Show plan (optimization proof)
    print("Explain plan (tool_daily):")
    tool_daily.explain(True)

    print(f"Wrote gold tables:\n- {gold_tool}\n- {gold_step}")


if __name__ == "__main__":
    main()

