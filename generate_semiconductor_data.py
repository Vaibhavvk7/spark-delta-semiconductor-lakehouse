import csv
import random
import uuid
from datetime import datetime, timedelta
import os

OUTPUT_DIR = "data/raw"
NUM_FILES = 3              # increase for more scale
ROWS_PER_FILE = 50000      # Spark starts to matter here

TOOLS = ["ETCH_01", "ETCH_02", "CVD_01", "CVD_02", "IMPLANT_01"]
PROCESS_STEPS = ["etch", "deposition", "implant", "clean", "inspection"]

os.makedirs(OUTPUT_DIR, exist_ok=True)

start_time = datetime.now() - timedelta(days=7)

for file_idx in range(NUM_FILES):
    file_path = os.path.join(OUTPUT_DIR, f"fab_telemetry_{file_idx}.csv")
    with open(file_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "timestamp",
            "tool_id",
            "wafer_id",
            "process_step",
            "temperature_c",
            "pressure_pa",
            "voltage_v",
            "current_a",
            "vibration_level",
            "defect_count",
            "yield_flag"
        ])

        for _ in range(ROWS_PER_FILE):
            tool = random.choice(TOOLS)
            step = random.choice(PROCESS_STEPS)

            temperature = random.gauss(350, 15)
            pressure = random.gauss(101325, 5000)
            voltage = random.gauss(5, 0.5)
            current = random.gauss(1.2, 0.2)
            vibration = abs(random.gauss(0.02, 0.01))

            defect_count = max(0, int(random.gauss(1, 2)))
            yield_flag = 1 if defect_count < 3 else 0

            writer.writerow([
                (start_time + timedelta(seconds=random.randint(0, 604800))).isoformat(),
                tool,
                f"WAFER_{uuid.uuid4().hex[:8]}",
                step,
                round(temperature, 2),
                round(pressure, 2),
                round(voltage, 3),
                round(current, 3),
                round(vibration, 4),
                defect_count,
                yield_flag
            ])

    print(f"Generated {file_path}")
