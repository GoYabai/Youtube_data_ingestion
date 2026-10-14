import json
import os
from pathlib import Path

import dotenv
import requests

# Load API key
PROJECT_ROOT = Path(__file__).resolve().parents[2]
dotenv.load_dotenv(PROJECT_ROOT / ".env")
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")


def fetch_youtube_comments(video_id):
    if not YOUTUBE_API_KEY:
        raise RuntimeError("Missing YOUTUBE_API_KEY. Set it in .env in the project root.")

    all_comments = []  # Store comments
    page_token = None  # Pagination token
    url_comment = "https://www.googleapis.com/youtube/v3/commentThreads"

    # API parameters
    params = {
        "part": "snippet",
        "videoId": video_id,
        "key": YOUTUBE_API_KEY,
        "maxResults": 100,
    }

    print("[+] Fetching comments via API...")

    while True:
        # Set page token if exists
        if page_token:
            params["pageToken"] = page_token

        # Fetch data
        response = requests.get(url=url_comment, params=params).json()

        # Handle API errors (e.g., quota exceeded)
        if "error" in response:
            print(f"\n[-] API Error: {response['error']['message']}")
            break

        # Extract and append comments
        items = response.get("items", [])
        all_comments.extend(items)

        # Print progress on the same line
        print(f"\r   -> Retrieved {len(all_comments)} comments...", end="", flush=True)

        next_token = response.get("nextPageToken")
        page_token = next_token
        if not page_token:
            break

    print("\n[+] Fetching completed. Saving file...")

    save_into = PROJECT_ROOT / "data" / "raw" / f"comments_{video_id}.json"
    save_into.parent.mkdir(parents=True, exist_ok=True)

    with open(save_into, mode="w", encoding="utf-8") as f:
        json.dump(all_comments, f, ensure_ascii=False, indent=4)

    print(f"[+] Successfully saved {len(all_comments)} comments from {video_id}")
