import json
from pathlib import Path

import pytchat


def fetch_livechat_vod(video_id):
    all_chat_messages = []

    print("[+] Fetching live chat, please wait...")
    try:
        # Initialize scraper
        chat = pytchat.create(video_id=video_id)

        # Check if chat is available
        if not chat.is_alive():
            raise Exception("No live chat found or access blocked.")

        # Fetch messages
        while chat.is_alive():
            for c in chat.get().sync_items():
                simplified_mes = {
                    "author": c.author.name,
                    "time": c.datetime,
                    "message": c.message,
                }
                all_chat_messages.append(simplified_mes)

                # Print progress
                current_count = len(all_chat_messages)
                if current_count % 1000 == 0:
                    print(f"   -> Retrieved {current_count} messages...")

    except Exception as e:
        raise Exception(f"Scraping error: {e}")

    # Check for empty results
    if len(all_chat_messages) == 0:
        raise Exception("No messages found in this video.")

    # Save to JSON
    save_into = Path(f"data/raw/livechat_{video_id}.json")
    save_into.parent.mkdir(parents=True, exist_ok=True)

    with open(save_into, mode="w", encoding="utf-8") as f:
        json.dump(all_chat_messages, f, ensure_ascii=False, indent=4)

    print(f"[+] Successfully saved {len(all_chat_messages)} messages from {video_id}")
