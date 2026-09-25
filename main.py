from scr.processing.extract_video_id import extract_video_id

from src.ingestion.youtube_api import fetch_youtube_comments

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

        if video_id:
            print(f"\n[+] Found video id: {video_id}")
            print("[+] Start fetch the video comment...")

            fetch_youtube_comments(video_id)

            print(f"\n[+] Done! File raw save into: data/raw/comments_{video_id}.json")
            print("-" * 50)
        else:
            print("\n[-] Error: Invalid link. Please enter a correct url")
