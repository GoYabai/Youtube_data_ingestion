import requests
import os
import dotenv

# load the api key
dotenv.load_dotenv()

# Get the key
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")


# video id for the api to load
video_id_temp = "Jk38OqdAQxc"


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
            params["page_token"] = page_token

        # fetch the page_token from vidieo
        response = requests.get(url=url_comment, params=params).json()

        # get item from request to all_comment
        items = response.get("items", [])
        all_comments.extend(items)

        next_token = response.get("nextPageToken")
        page_token = next_token
        if not page_token:
            break
