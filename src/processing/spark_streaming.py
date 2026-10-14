"""
Spark file-stream consumer for live YouTube chat micro-batches.

Run from project root:
    python -m src.processing.spark_streaming
Stop on Windows:
    New-Item -ItemType File -Path .\\stop_spark_streaming.flag -Force
"""

from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType

PROJECT_ROOT = Path(__file__).resolve().parents[2]

# MUST match INBOX_DIR in src/ingestion/livechat_stream.py
INPUT_PATH = (
    PROJECT_ROOT / "data" / "streaming" / "livechat_inbox"
)
# Final output and checkpoint (keep these consistent across restarts).
OUTPUT_PATH = (
    PROJECT_ROOT / "data" / "processed" / "livechat_stream"
)
CHECKPOINT_PATH = (
    PROJECT_ROOT / "data" / "checkpoints" / "livechat"
)
STOP_FILE = PROJECT_ROOT / "stop_spark_streaming.flag"

SCHEMA = StructType([
    StructField("message_id", StringType()),
    StructField("video_id", StringType()),
    StructField("author", StringType()),
    StructField("message", StringType()),
    StructField("timestamp", StringType()),
    StructField("elapsed_time", StringType()),
    StructField("elapsed_time_ms", LongType()),
    StructField("type", StringType()),
    StructField("amount", StringType()),
])


def process_livechat_stream():
    INPUT_PATH.mkdir(parents=True, exist_ok=True)

    # Clear an obsolete signal from an earlier run.
    STOP_FILE.unlink(missing_ok=True)

    spark = (
        SparkSession.builder
        .appName("YouTubeLiveChatStreaming")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    query = None

    try:
        print(f"[+] Watching inbox: {INPUT_PATH}")
        print(f"[+] Parquet output: {OUTPUT_PATH}")
        print(f"[+] Checkpoint: {CHECKPOINT_PATH}")
        print(f"[+] Stop signal: {STOP_FILE.name}", flush=True)

        df = spark.readStream.schema(SCHEMA).json(str(INPUT_PATH))

        # Stateless ingestion: no stateful deduplication (not exactly-once by ID).
        clean_df = df.dropna(subset=["video_id", "message_id"])

        query = (
            clean_df.writeStream
            .format("parquet")
            .outputMode("append")
            .option("path", str(OUTPUT_PATH))
            .option("checkpointLocation", str(CHECKPOINT_PATH))
            .partitionBy("video_id")
            .trigger(processingTime="10 seconds")
            .start()
        )
        print("[+] Spark Streaming started!", flush=True)

        last_batch_id = -1
        while query.isActive:
            if STOP_FILE.exists():
                print("[!] Spark stop signal detected.", flush=True)
                break

            query.awaitTermination(2)
            progress = query.lastProgress
            if progress is None:
                continue

            batch_id = progress["batchId"]
            if batch_id == last_batch_id:
                continue
            last_batch_id = batch_id

            print("\n========== SPARK MICRO-BATCH ==========", flush=True)
            print(f"Batch ID: {batch_id}")
            print(f"Input rows: {progress['numInputRows']}")
            print(f"Input rate: "
                  f"{progress.get('inputRowsPerSecond', 0):.2f} rows/s")
            print(f"Processing rate: "
                  f"{progress.get('processedRowsPerSecond', 0):.2f} rows/s")
            print(f"Duration: {progress.get('durationMs', {})}")
            print(f"Query active: {query.isActive}")
            print("=======================================\n", flush=True)

    except KeyboardInterrupt:
        print("\n[!] Ctrl+C received. Prefer stop_spark_streaming.flag "
              "on Windows.", flush=True)

    finally:
        if query is not None:
            try:
                if query.isActive:
                    query.stop()
            except Exception as exc:
                print(f"[!] Spark query stop warning: {exc}")

        try:
            spark.stop()
        except Exception as exc:
            print(f"[!] SparkSession stop warning: {exc}")

        STOP_FILE.unlink(missing_ok=True)
        print("[+] Spark streaming process stopped.", flush=True)


if __name__ == "__main__":
    process_livechat_stream()
