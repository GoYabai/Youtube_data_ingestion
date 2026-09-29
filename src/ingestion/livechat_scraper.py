import json
from pathlib import Path

import chat_downloader


def fetch_livechat_vod(video_id):
    all_chat_messages = []
    url = "https://www.youtube.com/watch?v=" + str(video_id)

    print("[+] Sart fetch live chat from vod, please wait... ")
    try:
        chat = chat_downloader.ChatDownloader().get_chat(url)
        for mes in chat:
            simplified_mes = {
                "author": mes.get("author", {}).get("name"),
                "time": mes.get("time_text"),
                "message": mes.get("message"),
            }
            all_chat_messages.append(simplified_mes)
            if len(all_chat_messages) % 1000 == 0:
                print(f"Successfully retrieved {len(all_chat_messages)} live chat")
    except Exception:
        raise Exception("\n[-] Error: This is a normal vid not a VOD.")

    save_into = Path(f"data/raw/livechat_{video_id}.json")
    save_into.parent.mkdir(parents=True, exist_ok=True)

    with open(save_into, mode="w", encoding="utf-8") as f:
        json.dump(all_chat_messages, f, ensure_ascii=False, indent=4)
    print(f"Successful save {len(all_chat_messages)} comment from {video_id}")
