import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

spark = (
    SparkSession.builder
    .appName("GlobalTelemetryPlatform")
    .master("local[4]")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.ui.showConsoleProgress", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
sc = spark.sparkContext
sc.setCheckpointDir("/home/claude/checkpoints")

schema = StructType([
    StructField("vehicle_id", StringType(), True),
    StructField("vehicle_model", StringType(), True),
    StructField("engine_temp", DoubleType(), True),
    StructField("timestamp", TimestampType(), True),
])

print("=" * 70)
print("STEP 1: INGESTION (read + narrow transformations)")
print("=" * 70)
raw_df = spark.read.schema(schema).option("header", True).csv("/home/claude/telemetry_raw.csv")

clean_df = (
    raw_df
    .filter(F.col("engine_temp").isNotNull())
    .withColumn("engine_temp_c", F.col("engine_temp"))
)
total_rows = clean_df.count()
print(f"Total ingested rows: {total_rows}")
print(f"Number of partitions after read: {raw_df.rdd.getNumPartitions()}")

print()
print("=" * 70)
print("STEP 2: DEMONSTRATE THE SKEW (row count per vehicle_id, top 10)")
print("=" * 70)
skew_check = (
    clean_df.groupBy("vehicle_id")
    .agg(F.count("*").alias("row_count"))
    .orderBy(F.desc("row_count"))
)
skew_check.show(10, truncate=False)

print("=" * 70)
print("STEP 3: NAIVE (UNSALTED) AGGREGATION - TIMED")
print("=" * 70)
t0 = time.time()
naive_result = (
    clean_df.groupBy("vehicle_id")
    .agg(F.avg("engine_temp_c").alias("avg_temp"), F.count("*").alias("cnt"))
)
naive_count = naive_result.count()  # action forces execution
t1 = time.time()
print(f"Naive groupBy completed. Distinct vehicles: {naive_count}")
print(f"Naive aggregation wall time: {t1 - t0:.3f}s")
print("(all 200,000 rows for each HOT vehicle land in ONE partition/task -> straggler)")

print()
print("=" * 70)
print("STEP 4: SALTED AGGREGATION (skew mitigation) - TIMED")
print("=" * 70)
NUM_SALTS = 20
t2 = time.time()

salted_df = raw_df.withColumn(
    "salted_vehicle_id",
    F.concat(F.col("vehicle_id"), F.lit("_"), (F.rand() * NUM_SALTS).cast("int"))
)

partial_agg = (
    salted_df.groupBy("salted_vehicle_id", "vehicle_model")
    .agg(F.sum("engine_temp").alias("sum_temp"), F.count("*").alias("cnt"))
)

final_agg = (
    partial_agg
    .withColumn("vehicle_id", F.split(F.col("salted_vehicle_id"), "_").getItem(0))
    .groupBy("vehicle_id", "vehicle_model")
    .agg((F.sum("sum_temp") / F.sum("cnt")).alias("avg_engine_temp"),
         F.sum("cnt").alias("total_readings"))
)
salted_count = final_agg.count()
t3 = time.time()
print(f"Salted aggregation completed. Distinct vehicles: {salted_count}")
print(f"Salted aggregation wall time: {t3 - t2:.3f}s")

print()
print("Sample of final salted aggregation result (hot vehicles):")
final_agg.filter(F.col("vehicle_id").startswith("HOT")).orderBy("vehicle_id").show(truncate=False)

print("Sample of final salted aggregation result (normal vehicles, first 5):")
final_agg.filter(~F.col("vehicle_id").startswith("HOT")).orderBy("vehicle_id").show(5, truncate=False)

print()
print("=" * 70)
print("STEP 5: VERIFY correctness -> naive vs salted results should match")
print("=" * 70)
naive_avgs = {r["vehicle_id"]: round(r["avg_temp"], 4) for r in naive_result.collect()}
salted_avgs = {r["vehicle_id"]: round(r["avg_engine_temp"], 4) for r in final_agg.collect()}
mismatches = 0
for vid in naive_avgs:
    if abs(naive_avgs[vid] - salted_avgs.get(vid, -999)) > 0.01:
        mismatches += 1
print(f"Vehicles compared: {len(naive_avgs)}, mismatches (tolerance 0.01): {mismatches}")
print("Correctness check: PASSED" if mismatches == 0 else "Correctness check: FAILED")

print()
print("=" * 70)
print("STEP 6: LINEAGE INSPECTION")
print("=" * 70)
print(clean_df.rdd.toDebugString().decode() if isinstance(clean_df.rdd.toDebugString(), bytes) else clean_df.rdd.toDebugString())

print()
print("=" * 70)
print("STEP 7: EXPLAIN PLAN - showing Stage-forming shuffle (wide dependency)")
print("=" * 70)
final_agg.explain(mode="formatted")

print()
print("=" * 70)
print("STEP 8: ITERATIVE LOOP + CHECKPOINTING (truncating a growing DAG)")
print("=" * 70)
iterative_df = clean_df.select("vehicle_id", "vehicle_model", "engine_temp_c")
CHECKPOINT_EVERY = 10
NUM_ITERS = 30

lineage_lengths = []
for i in range(NUM_ITERS):
    iterative_df = iterative_df.withColumn(
        f"adj_{i}", F.col("engine_temp_c") + F.lit(i) * 0.01
    )
    if (i + 1) % CHECKPOINT_EVERY == 0:
        pre_lines = len(iterative_df.rdd.toDebugString().splitlines())
        iterative_df = iterative_df.checkpoint(eager=True)
        post_lines = len(iterative_df.rdd.toDebugString().splitlines())
        lineage_lengths.append((i + 1, pre_lines, post_lines))
        print(f"Iteration {i+1}: lineage depth BEFORE checkpoint = {pre_lines} lines, "
              f"AFTER checkpoint = {post_lines} lines (truncated)")

print()
print(f"Final row count after {NUM_ITERS} iterations with periodic checkpointing: {iterative_df.count()}")
print("Final lineage (should be shallow -> reads from last checkpoint, not 30 chained transforms):")
print(iterative_df.rdd.toDebugString())

print()
print("=" * 70)
print("STEP 9: WRITE FINAL RESULT TO DISK (Action)")
print("=" * 70)
final_agg.write.mode("overwrite").parquet("/home/claude/output/avg_temp_by_vehicle")
print("Written to /home/claude/output/avg_temp_by_vehicle")

spark.stop()
print()
print("PIPELINE COMPLETE.")
