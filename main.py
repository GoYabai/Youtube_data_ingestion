from src.ingestion.youtube_api import fetch_youtube_comments

if __name__ == "__main__":
    # video id for the api to load
    video_id_temp = "Jk38OqdAQxc"
    fetch_youtube_comments(video_id_temp)
