from __future__ import annotations
import os, json
from pathlib import Path
from typing import Set, List

from pyspark.sql import functions as F
from src.utils import build_spark, load_config

STATE_FILE = "lakehouse/_state/bronze_ingest_state.json"


def _load_state() -> Set[str]:
    if not os.path.exists(STATE_FILE):
        return set()
    with open(STATE_FILE, "r") as f:
        return set(json.load(f))


def _save_state(processed: Set[str]) -> None:
    Path(os.path.dirname(STATE_FILE)).mkdir(parents=True, exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump(sorted(list(processed)), f, indent=2)


def list_raw_files(raw_dir: str) -> List[str]:
    p = Path(raw_dir)
    return sorted([str(x) for x in p.glob("*.csv")])


def main():
    cfg = load_config()
    spark = build_spark(cfg["app"]["name"])

    raw_dir = cfg["paths"]["raw"]
    bronze_path = cfg["paths"]["bronze"]
    required = list(cfg["columns"].values())

    processed = _load_state()
    all_files = list_raw_files(raw_dir)
    new_files = [f for f in all_files if f not in processed]

    if not new_files:
        print("No new raw files to ingest.")
        return

    df = (
        spark.read.format(cfg["input"]["format"])
        .option("header", cfg["input"]["header"])
        .option("inferSchema", cfg["input"]["inferSchema"])
        .load(new_files)
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_ingested_at", F.current_timestamp())
    )

    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in input: {missing}")

    (df.write.format("delta")
        .mode("append")
        .save(bronze_path))

    processed.update(new_files)
    _save_state(processed)

    print(f"Ingested {len(new_files)} file(s) -> bronze: {bronze_path}")


if __name__ == "__main__":
    main()

