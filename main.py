from src.ingestion.livechat_scraper import fetch_livechat_vod
from src.ingestion.youtube_api import fetch_youtube_comments
from src.processing.extract_video_id import extract_video_id

if __name__ == "__main__":
    print("=" * 50)
    print("YOUTUBE COMMENT INGESTION PIPELINE")
    print("=" * 50)

    while True:
        user_input = input("\nEnter video YouTube link ('exit' to escape): ")
        if user_input.strip().lower() == "exit":
            print("End the program.")
            break

        video_id = extract_video_id(user_input)
        if not video_id:
            print("\n[-] Error: Invalid link. Please enter a correct url")
            continue

        print(f"\n[+] Found video id: {video_id}")

        while True:
            try:
                choice = int(
                    input(
                        "\n[+] Please enter fetch options:\n1: fetch comment\n2: fetch live chat\nYour choice: "
                    )
                )
                if choice in (1, 2):
                    break
                else:
                    print("[-] Please enter a valid choice (1 or 2)")
            except ValueError:
                print("[-] Please enter a number (1 or 2)")

        if choice == 1:
            print("[+] Start fetch the video comment...")
            try:
                fetch_youtube_comments(video_id)
                print(
                    f"\n[+] Done! File raw save into: data/raw/comments_{video_id}.json"
                )
            except Exception as e:
                print(f"\n[-] Error: {e}")
            print("-" * 50)

        elif choice == 2:
            print("[+] Start fetch the video live chat...")
            try:
                fetch_livechat_vod(video_id)
                print(
                    f"\n[+] Done! File raw save into: data/raw/livechat_{video_id}.json"
                )
            except Exception as e:
                print(f"\n[-] Error: {e}")
            print("-" * 50)
