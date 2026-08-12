import random
import csv
from datetime import datetime, timedelta

random.seed(42)

MODELS = ["Volvo-FH16", "Scania-R500", "MAN-TGX", "Mercedes-Actros", "DAF-XF"]

# 500 "normal" vehicles
normal_vehicles = [(f"V{i:04d}", random.choice(MODELS)) for i in range(1, 501)]

# 5 "hot" vehicles that will generate ~1000x more log rows (skew simulation)
hot_vehicles = [(f"HOT{i:02d}", random.choice(MODELS)) for i in range(1, 6)]

rows = []
base_time = datetime(2026, 1, 1, 0, 0, 0)

# Normal vehicles: ~200 readings each
for vid, model in normal_vehicles:
    for j in range(200):
        temp = round(random.gauss(85, 8), 2)  # engine temp in Celsius
        ts = base_time + timedelta(seconds=j * 30)
        rows.append((vid, model, temp, ts.isoformat()))

# Hot vehicles: ~200,000 readings each (the skew)
for vid, model in hot_vehicles:
    for j in range(200_000):
        # simulate slightly elevated/noisy readings for hot trucks
        temp = round(random.gauss(95, 12), 2)
        ts = base_time + timedelta(seconds=j * 2)
        rows.append((vid, model, temp, ts.isoformat()))

random.shuffle(rows)

with open("/home/claude/telemetry_raw.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["vehicle_id", "vehicle_model", "engine_temp", "timestamp"])
    w.writerows(rows)

print(f"Total rows written: {len(rows)}")
print(f"Normal vehicles: {len(normal_vehicles)} x ~200 rows")
print(f"Hot (skewed) vehicles: {len(hot_vehicles)} x 200,000 rows")
