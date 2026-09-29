from pathlib import Path

from src.ingestion.livechat_scraper import fetch_livechat_vod
from src.ingestion.youtube_api import fetch_youtube_comments
from src.processing.extract_video_id import extract_video_id

if __name__ == "__main__":
    print("=" * 50)
    print("YOUTUBE DATA INGESTION PIPELINE")
    print("=" * 50)

    while True:
        print("\n[+] Select an option:")
        print("1: Fetch comment (Single video)")
        print("2: Fetch live chat (Single video)")
        print("3: Batch fetch comments (From data/video_links.txt)")
        print("0: Exit")

        try:
            choice = int(input("Your choice (0-3): "))
            if choice not in (0, 1, 2, 3):
                print("[-] Please enter a valid choice (0-3)")
                continue
        except ValueError:
            print("[-] Please enter a number (0-3)")
            continue

        if choice == 0:
            print("[+] End the program.")
            break

        if choice == 3:
            file_path = Path("data/video_links.txt")
            if not file_path.exists():
                file_path.parent.mkdir(parents=True, exist_ok=True)
                file_path.touch()
                print(f"[-] File not found. Created empty file at: {file_path}")
                print("[!] Please add YouTube links to this file and try again.")
                continue

            with open(file_path, "r", encoding="utf-8") as f:
                links = [line.strip() for line in f if line.strip()]

            if not links:
                print(f"[-] The file {file_path} is empty.")
                continue

            print(f"[+] Found {len(links)} links. Starting batch processing...")

            for idx, url in enumerate(links, 1):
                print(f"\n--- Processing video {idx}/{len(links)} ---")
                video_id = extract_video_id(url)

                if video_id:
                    try:
                        fetch_youtube_comments(video_id)
                    except Exception as e:
                        print(f"[-] Error fetching {video_id}: {e}")
                else:
                    print(f"[-] Invalid link format: {url}")

            print("\n[+] Batch processing completed!")
            continue

        user_input = input("\nEnter video YouTube link: ")
        video_id = extract_video_id(user_input)

        if not video_id:
            print("[-] Error: Invalid link. Please enter a correct url")
            continue

        print(f"[+] Found video id: {video_id}")

        if choice == 1:
            print("[+] Start fetching video comments...")
            try:
                fetch_youtube_comments(video_id)
            except Exception as e:
                print(f"[-] Error: {e}")
            print("-" * 50)

        elif choice == 2:
            print("[+] Start fetching video live chat...")
            try:
                fetch_livechat_vod(video_id)
            except Exception as e:
                print(f"[-] Error: {e}")
            print("-" * 50)
