from pathlib import Path


def process_comments():
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col

    # Define paths
    project_root = Path(__file__).resolve().parents[2]
    raw_path = project_root / "data" / "raw"
    processed_path = project_root / "data" / "processed" / "comments"

    comment_files = list(raw_path.glob("comments_*.json"))

    if not comment_files:
        print("[-] No comment JSON files found.")
        return

    # Create Spark session
    spark = (
        SparkSession.builder.appName("YoutubeDataProcessing")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )

    try:
        # Read raw JSON
        print(f"[+] Reading {len(comment_files)} comment files...")

        df = spark.read.option("multiline", "true").json(
            [str(path) for path in comment_files]
        )

        # Flatten data
        clean_df = df.select(
            col("snippet.videoId").alias("video_id"),
            col("snippet.topLevelComment.snippet.authorDisplayName").alias("author"),
            col("snippet.topLevelComment.snippet.textOriginal").alias("comment"),
            col("snippet.topLevelComment.snippet.likeCount").alias("likes"),
            col("snippet.topLevelComment.snippet.publishedAt").alias("published_at"),
        )

        clean_df.show(5)

        # 5. Data quality checks
        total_raw = clean_df.count()

        dedup_df = clean_df.dropDuplicates(["video_id", "author", "comment"])

        final_df = dedup_df.dropna(subset=["video_id", "author", "published_at"])

        total_valid = final_df.count()
        dropped_records = total_raw - total_valid

        print(f"[+] Total raw: {total_raw}")
        print(f"[+] Total valid: {total_valid}")
        print(f"[+] Dropped records: {dropped_records}")

        # 6. Save as Parquet
        final_df.write.mode("overwrite").partitionBy("video_id").parquet(
            str(processed_path)
        )

        print(f"[+] Parquet saved to: {processed_path}")

    finally:
        spark.stop()
        print("[+] Spark session stopped.")


if __name__ == "__main__":
    process_comments()
