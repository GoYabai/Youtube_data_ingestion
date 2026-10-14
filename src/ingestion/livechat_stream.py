"""
Producer for a live YouTube chat. Save completed NDJSON micro-batches
into the same inbox that spark_streaming.py watches.

Run from the project root:
    python -m src.ingestion.livechat_stream "https://www.youtube.com/watch?v=..."
Stop on Windows (from a second terminal):
    New-Item -ItemType File -Path .\\stop_livechat_producer.flag -Force
"""

import json
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

from src.processing.extract_video_id import extract_video_id
from src.ingestion.livechat_scraper import parse_chat_line


PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Keep these aligned with spark_streaming.py.
INBOX_DIR = PROJECT_ROOT / "data" / "streaming" / "livechat_inbox"
STAGING_DIR = PROJECT_ROOT / "data" / "streaming" / "staging"
STOP_FILE = PROJECT_ROOT / "stop_livechat_producer.flag"


def read_new_messages(file_path, position, video_id):
    """Read only complete new NDJSON lines; return (records, byte offset)."""
    messages = []

    if not file_path.exists():
        return messages, position

    # A replaced/truncated file should not leave us seeking beyond EOF.
    if file_path.stat().st_size < position:
        print("[!] Raw chat file shrank; resetting read position to 0.")
        position = 0

    with file_path.open("rb") as f:
        f.seek(position)

        while True:
            line = f.readline()

            # If writer has not finished the line yet, retry next poll.
            if not line or not line.endswith(b"\n"):
                break

            position = f.tell()

            try:
                record = parse_chat_line(line.decode("utf-8"), video_id)
            except (UnicodeDecodeError, ValueError) as exc:
                print(f"[-] Skipping malformed chat line: {exc}")
                continue

            if not record or not isinstance(record, dict):
                continue

            if record.get("type") in ("system", "placeholder"):
                continue

            if not record.get("message_id"):
                continue

            messages.append(record)

    return messages, position


def publish_batch(messages, video_id, session_id, batch_number):
    """Publish a complete NDJSON file atomically to Spark's input folder."""
    if not messages:
        return

    filename = f"{video_id}_{session_id}_{batch_number:06d}.json"
    temp_path = STAGING_DIR / (filename + ".tmp")
    final_path = INBOX_DIR / filename

    with temp_path.open("w", encoding="utf-8", newline="\n") as f:
        for message in messages:
            f.write(json.dumps(message, ensure_ascii=False) + "\n")

    # Same drive/filesystem: Spark never sees an incomplete JSON file.
    os.replace(temp_path, final_path)

    print(f"[+] Published batch {batch_number}: {len(messages)} messages "
          f"-> {final_path.name}", flush=True)


def stream_livechat(video_url, interval=10, max_batch=200):
    """Download live chat while periodically emitting micro-batches."""
    video_id = extract_video_id(video_url)
    if not video_id:
        raise ValueError("Invalid YouTube URL")

    session_id = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    raw_dir = (PROJECT_ROOT / "data" / "raw" /
               "livechat_download" / session_id)

    for directory in (raw_dir, STAGING_DIR, INBOX_DIR):
        directory.mkdir(parents=True, exist_ok=True)

    # Clear an obsolete signal from an earlier run.
    STOP_FILE.unlink(missing_ok=True)

    raw_file = raw_dir / f"{video_id}.live_chat.json"
    part_file = raw_dir / f"{video_id}.live_chat.json.part"

    command = [
        sys.executable, "-m", "yt_dlp",
        "--skip-download", "--write-subs",
        "--sub-langs", "live_chat", "--no-playlist",
        "--no-progress",
        "-o", str(raw_dir / "%(id)s.%(ext)s"),
        video_url,
    ]

    print(f"[+] Live chat producer: {video_id}")
    print(f"[+] Raw chat folder: {raw_dir}")
    print(f"[+] Publishing to: {INBOX_DIR}")
    print(f"[+] Stop signal: {STOP_FILE.name}", flush=True)

    process = subprocess.Popen(command, cwd=str(PROJECT_ROOT))
    position = 0
    pending = []
    batch_number = 0
    last_publish = time.monotonic()
    last_status = time.monotonic()
    seen_source = None

    def consume_new_data():
        nonlocal position, seen_source

        source = part_file if part_file.exists() else (
            raw_file if raw_file.exists() else None
        )
        if source is None:
            return

        if seen_source is None:
            print(f"[+] Reading chat from: {source.name}", flush=True)
            seen_source = source

        records, position = read_new_messages(source, position, video_id)
        pending.extend(records)

    def flush_batch():
        nonlocal batch_number, last_publish
        if not pending:
            return

        next_number = batch_number + 1
        publish_batch(pending, video_id, session_id, next_number)
        batch_number = next_number
        pending.clear()
        last_publish = time.monotonic()

    stopped_by_user = False
    try:
        while True:
            if STOP_FILE.exists():
                stopped_by_user = True
                print("[!] Stop signal detected.", flush=True)
                break

            consume_new_data()
            now = time.monotonic()

            if pending and (
                len(pending) >= max_batch
                or now - last_publish >= interval
            ):
                flush_batch()

            if now - last_status >= 20:
                size = (
                    part_file.stat().st_size if part_file.exists()
                    else raw_file.stat().st_size if raw_file.exists()
                    else 0
                )
                print(f"[+] Status: raw_bytes={size}, "
                      f"read_bytes={position}, waiting_records={len(pending)}",
                      flush=True)
                last_status = now

            if process.poll() is not None:
                break

            time.sleep(0.5)

    except KeyboardInterrupt:
        stopped_by_user = True
        print("\n[!] Ctrl+C received. Stopping producer.", flush=True)

    finally:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()

        # Drain complete remaining records after downloader exits.
        consume_new_data()
        flush_batch()
        STOP_FILE.unlink(missing_ok=True)

        print(f"[+] Producer stopped. Total batches: {batch_number}",
              flush=True)
        if not stopped_by_user and process.returncode not in (0, None):
            print(f"[!] yt-dlp exited unexpectedly: {process.returncode}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print('Usage: python -m src.ingestion.livechat_stream "URL"')
        sys.exit(1)
    stream_livechat(sys.argv[1])
