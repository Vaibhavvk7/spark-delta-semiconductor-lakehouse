from __future__ import annotations
from pyspark.sql import functions as F
from src.utils import build_spark, load_config


def fail_if(df, condition, msg: str):
    if df.filter(condition).limit(1).count() > 0:
        raise ValueError(msg)


def main():
    cfg = load_config()
    spark = build_spark(cfg["app"]["name"])
    silver = cfg["paths"]["silver"]
    c = cfg["columns"]

    df = spark.read.format("delta").load(silver)

    # Required non-nulls
    required = ["event_ts", "event_date", c["tool_id"], c["wafer_id"], c["process_step"], "row_key"]
    for colname in required:
        fail_if(df, F.col(colname).isNull(), f"DQ FAIL: nulls in {colname}")

    # Value constraints
    fail_if(df, F.col(c["defect_count"]) < 0, "DQ FAIL: negative defect_count")
    fail_if(df, ~F.col(c["yield_flag"]).isin([0, 1]), "DQ FAIL: yield_flag not in {0,1}")

    # Uniqueness of row_key
    dups = df.groupBy("row_key").count().filter(F.col("count") > 1)
    if dups.limit(1).count() > 0:
        raise ValueError("DQ FAIL: duplicate row_key detected")

    # Freshness-ish check: latest date has rows
    mx = df.select(F.max("event_date").alias("mx")).collect()[0]["mx"]
    if mx is None:
        raise ValueError("DQ FAIL: empty silver table")
    latest_rows = df.filter(F.col("event_date") == F.lit(mx)).count()
    if latest_rows == 0:
        raise ValueError("DQ FAIL: latest partition has 0 rows")

    print(f"DQ PASS: silver OK. Latest={mx}, rows={latest_rows}")


if __name__ == "__main__":
    main()

