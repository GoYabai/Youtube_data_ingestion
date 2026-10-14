"""Single entry point for YouTube comments, chat replay, and live Spark ingestion.

Run from project root (or any current directory):
    python main.py

For LIVE mode the menu launches Spark and yt-dlp producer in separate child
processes, then stops both gracefully when the user presses Enter.
"""

import os
import subprocess
import sys
import time
from pathlib import Path

from src.processing.extract_video_id import extract_video_id


PROJECT_ROOT = Path(__file__).resolve().parent
DATA_DIR = PROJECT_ROOT / "data"
PRODUCER_STOP = PROJECT_ROOT / "stop_livechat_producer.flag"
SPARK_STOP = PROJECT_ROOT / "stop_spark_streaming.flag"
LIVE_OUTPUT = DATA_DIR / "processed" / "livechat_stream"
DEMO_OUTPUT = DATA_DIR / "demo" / "livechat_101"


def ask_video_url():
    url = input("\nEnter YouTube video URL: ").strip()
    video_id = extract_video_id(url)
    if not video_id:
        print("[-] Invalid YouTube URL.")
        return None, None
    print(f"[+] Video ID: {video_id}")
    return url, video_id


def fetch_comments_single():
    _, video_id = ask_video_url()
    if not video_id:
        return
    from src.ingestion.youtube_api import fetch_youtube_comments
    fetch_youtube_comments(video_id)


def fetch_chat_replay():
    url, video_id = ask_video_url()
    if not video_id:
        return
    from src.ingestion.livechat_scraper import fetch_livechat
    print("[+] Downloading available chat replay...")
    print(f"[+] Saved to: {fetch_livechat(url)}")


def fetch_comments_batch():
    links_file = DATA_DIR / "video_links.txt"
    if not links_file.is_file():
        links_file.parent.mkdir(parents=True, exist_ok=True)
        links_file.touch()
        print(f"[-] Created {links_file}; add YouTube URLs and retry.")
        return

    urls = [line.strip() for line in links_file.read_text(encoding="utf-8").splitlines()
            if line.strip() and not line.lstrip().startswith("#")]
    if not urls:
        print(f"[-] No URLs in {links_file}.")
        return

    from src.ingestion.youtube_api import fetch_youtube_comments
    print(f"[+] Found {len(urls)} video URLs.")
    for index, url in enumerate(urls, start=1):
        video_id = extract_video_id(url)
        if not video_id:
            print(f"[-] Skipping invalid URL #{index}: {url}")
            continue
        print(f"\n[+] Video {index}/{len(urls)}: {video_id}")
        try:
            fetch_youtube_comments(video_id)
        except Exception as exc:
            print(f"[-] Failed to fetch {video_id}: {exc}")
    print("[+] Batch fetch completed.")


def process_comments_batch():
    from src.processing.spark_processor import process_comments
    process_comments()


def _wait_for_exit(process, name, timeout):
    """Allow a child to shut down; forcibly terminate only as a fallback."""
    if process is None:
        return
    try:
        process.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        print(f"[!] {name} did not stop within {timeout}s. Terminating...")
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
    print(f"[+] {name} exited (code {process.returncode}).")


def run_live_pipeline():
    """Start live chat producer + Spark together and stop them via flag files."""
    url, video_id = ask_video_url()
    if not video_id:
        return

    # Child scripts use the same project root and will also remove stale flags.
    for flag in (PRODUCER_STOP, SPARK_STOP):
        flag.unlink(missing_ok=True)

    # On Windows isolate subprocesses from Ctrl+C sent to the menu process;
    # on POSIX give each child its own process session for the same reason.
    group_options = (
        {"creationflags": subprocess.CREATE_NEW_PROCESS_GROUP}
        if os.name == "nt"
        else {"start_new_session": True}
    )

    consumer = None
    producer = None
    try:
        print("\n[+] Starting Spark Streaming consumer...", flush=True)
        consumer = subprocess.Popen(
            [sys.executable, "-m", "src.processing.spark_streaming"],
            cwd=str(PROJECT_ROOT),
            stdin=subprocess.DEVNULL,
            **group_options,
        )

        # The file-stream consumer also handles producer files created before
        # Spark is fully ready, so an arbitrary long startup sleep is unnecessary.
        time.sleep(1)
        if consumer.poll() is not None:
            print(f"[-] Spark exited immediately (code {consumer.returncode}).")
            return

        print("[+] Starting live chat producer...", flush=True)
        producer = subprocess.Popen(
            [sys.executable, "-m", "src.ingestion.livechat_stream", url],
            cwd=str(PROJECT_ROOT),
            stdin=subprocess.DEVNULL,
            **group_options,
        )

        print("\n[+] LIVE pipeline is running.")
        print("[+] Watch 'Published batch' and 'Input rows' above.")
        input("[+] Press ENTER to stop producer, then Spark safely...\n")
    except (KeyboardInterrupt, EOFError):
        print("\n[!] Stop requested.")
    finally:
        # Stop producer first so no new input files arrive after Spark stops.
        if producer is not None and producer.poll() is None:
            PRODUCER_STOP.touch()
        _wait_for_exit(producer, "Live producer", 25)

        if consumer is not None and consumer.poll() is None:
            print("[+] Allowing Spark 15s to ingest the final JSON batches...", flush=True)
            time.sleep(15)
            SPARK_STOP.touch()
        _wait_for_exit(consumer, "Spark consumer", 45)

        # Both workers normally remove their own signals. Clean up any
        # leftover flags if they were force-terminated.
        PRODUCER_STOP.unlink(missing_ok=True)
        SPARK_STOP.unlink(missing_ok=True)
        print(f"[+] Output directory: {LIVE_OUTPUT}")


def inspect_parquet():
    """Inspect processed live-chat Parquet or the bundled demonstration data."""
    print("\n1: Real streaming output")
    print("2: Bundled 101-record demo")
    source = input("Select dataset (1-2): ").strip()
    if source not in ("1", "2"):
        print("[-] Invalid choice.")
        return

    path = LIVE_OUTPUT if source == "1" else DEMO_OUTPUT
    if not path.is_dir() or not any(path.rglob("*.parquet")):
        print(f"[-] No Parquet found in: {path}")
        print("[!] Run option 5, or choose option 2 for demo data.")
        return

    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col

    spark = SparkSession.builder.appName("InspectYouTubeLiveChat").getOrCreate()
    try:
        df = spark.read.parquet(str(path))
        print(f"\n[+] Dataset: {path}")
        print(f"[+] Total messages: {df.count()}")
        print("\n[+] Messages by type:")
        df.groupBy("video_id", "type").count().show(truncate=False)
        print("\n[+] First 10 messages:")
        df.select("video_id", "author", "message", "timestamp", "type", "amount").show(10, truncate=60)
        print("\n[+] Duplicate IDs in this dataset:")
        df.groupBy("video_id", "message_id").count().filter(col("count") > 1).show(10)
    finally:
        spark.stop()


def main():
    # For third-party tools that resolve files relative to CWD.
    os.chdir(PROJECT_ROOT)
    menu = {
        "1": fetch_comments_single,
        "2": fetch_chat_replay,
        "3": fetch_comments_batch,
        "4": process_comments_batch,
        "5": run_live_pipeline,
        "6": inspect_parquet,
    }

    print("=" * 55)
    print("YOUTUBE DATA INGESTION PIPELINE")
    print("=" * 55)
    while True:
        print("\n1: Fetch comments (single video)")
        print("2: Fetch live chat replay (VOD, if available)")
        print("3: Batch fetch comments (data/video_links.txt)")
        print("4: Process comments with Spark (batch)")
        print("5: LIVE chat + Spark Streaming (automatic)")
        print("6: Inspect Parquet output / sample data")
        print("0: Exit")
        try:
            choice = input("Your choice (0-6): ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\n[+] Exiting.")
            return

        if choice == "0":
            print("[+] Exiting.")
            return
        action = menu.get(choice)
        if not action:
            print("[-] Select a number from 0 to 6.")
            continue
        try:
            action()
        except (EOFError, KeyboardInterrupt):
            print("\n[!] Operation cancelled.")
        except Exception as exc:
            print(f"[-] Operation failed: {type(exc).__name__}: {exc}")


if __name__ == "__main__":
    main()
