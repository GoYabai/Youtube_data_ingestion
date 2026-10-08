import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Data flatten
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
data_path = os.path.join(project_root, "data", "raw", "comments_*.json")


spark = (
    SparkSession.builder.appName("YoutubeDataProcessing")
    .config("spark.driver.memory", "4g")
    .getOrCreate()
)

print(f"[+] Reading from file path:\n{data_path}")

df = spark.read.option("multiline", "true").json(data_path)

clean_df = df.select(
    col("snippet.videoId").alias("video_id"),
    col("snippet.topLevelComment.snippet.authorDisplayName").alias("author"),
    col("snippet.topLevelComment.snippet.textOriginal").alias("comment"),
    col("snippet.topLevelComment.snippet.likeCount").alias("likes"),
    col("snippet.topLevelComment.snippet.publishedAt").alias("published_at"),
)

clean_df.show(5)

# Data quality check
total_raw = clean_df.count()
dedup_df = clean_df.dropDuplicates(["video_id", "author", "comment"])
final_df = dedup_df.dropna(subset=["video_id", "author", "published_at"])

total_valid = final_df.count()
dropped_records = total_raw - total_valid
print("total raw-total valid-drop record")
print(total_raw, total_valid, dropped_records)

# Parquet & Partitioning
processed_path = os.path.join(project_root, "data", "processed")

final_df.write.mode("overwrite").partitionBy("video_id").parquet(processed_path)
print("[+] Succesful turn data into parquet file")
