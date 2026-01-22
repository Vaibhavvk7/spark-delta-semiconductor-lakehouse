from pyspark.sql import functions as F
from src.utils import build_spark, load_config

def show_basic(df, name):
    print("\n" + "="*80)
    print(f"{name}")
    print("="*80)
    print("Count:", df.count())
    df.printSchema()
    df.show(5, truncate=False)

def main():
    cfg = load_config()
    spark = build_spark(cfg["app"]["name"])

    paths = cfg["paths"]
    bronze = spark.read.format("delta").load(paths["bronze"])
    silver = spark.read.format("delta").load(paths["silver"])
    gold_tool = spark.read.format("delta").load(paths["gold_tool_daily"])
    gold_step = spark.read.format("delta").load(paths["gold_step_daily"])

    show_basic(bronze, "BRONZE")
    show_basic(silver, "SILVER")
    show_basic(gold_tool, "GOLD: tool_daily_metrics")
    show_basic(gold_step, "GOLD: step_daily_metrics")

if __name__ == "__main__":
    main()

