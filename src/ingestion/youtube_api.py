import json
import os
from pathlib import Path

import dotenv
import requests

# load the api key
dotenv.load_dotenv()

# Get the key
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")


# fetch youtube comment
def fetch_youtube_comments(video_id):
    all_comments = []  # store all comment from video
    page_token = None  # mark the page for the loop
    url_comment = "https://www.googleapis.com/youtube/v3/commentThreads"

    params = {
        "part": "snippet",
        "videoId": video_id,
        "key": YOUTUBE_API_KEY,
        "maxResults": 100,
    }  # parameter for the api

    while True:
        # move to the next page, if none get the first comment
        if page_token:
            params["pageToken"] = page_token

        # fetch the page_token from vidieo
        response = requests.get(url=url_comment, params=params).json()

        # get item from request to all_comment
        items = response.get("items", [])
        all_comments.extend(items)

        next_token = response.get("nextPageToken")
        page_token = next_token
        if not page_token:
            break

    save_into = Path(f"data/raw/comments_{video_id}.json")
    save_into.parent.mkdir(parents=True, exist_ok=True)

    with open(save_into, mode="w", encoding="utf-8") as f:
        json.dump(all_comments, f, ensure_ascii=False, indent=4)
    print(f"Successful save {len(all_comments)} comment from {video_id}")
