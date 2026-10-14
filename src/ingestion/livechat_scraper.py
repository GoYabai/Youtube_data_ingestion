import json
from datetime import datetime, timezone
from pathlib import Path

import yt_dlp

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def extract_text(message_data):
    """
    Extract readable text from YouTube message.runs.

    A run can contain:
    - text
    - emoji

    We only keep normal text here.
    """

    if not message_data:
        return ""

    runs = message_data.get("runs", [])

    text_parts = []

    for run in runs:
        if "text" in run:
            text_parts.append(run["text"])

        elif "emoji" in run:
            emoji_data = run["emoji"]

            shortcuts = emoji_data.get("shortcuts", [])

            if shortcuts:
                text_parts.append(shortcuts[0])

    return "".join(text_parts)


def normalize_type(renderer_name):

    type_mapping = {
        "liveChatTextMessageRenderer": "text",
        "liveChatPaidMessageRenderer": "super_chat",
        "liveChatPaidStickerRenderer": "super_sticker",
        "liveChatMembershipItemRenderer": "membership",
        "giftMessageViewModel": "gift",
        "liveChatViewerEngagementMessageRenderer": "system",
        "liveChatPlaceholderItemRenderer": "placeholder",
    }

    return type_mapping.get(renderer_name, renderer_name)


def timestamp_to_datetime(timestamp_usec):
    """
    Convert YouTube timestampUsec into ISO datetime.
    """

    if timestamp_usec is None:
        return None

    try:
        timestamp_sec = int(timestamp_usec) / 1_000_000

        dt = datetime.fromtimestamp(
            timestamp_sec,
            tz=timezone.utc,
        )

        return dt.isoformat()

    except (ValueError, TypeError):
        return None



def parse_chat_line(line, video_id):
    """
    Parse a single yt-dlp live chat JSON line.

    Supports:
        - VOD live chat replay
        - Active YouTube livestream chat

    Returns:
        dict or None
    """

    # 1. Convert JSON string into dictionary
    try:
        data = json.loads(line)
    except json.JSONDecodeError:
        return None

    if not isinstance(data, dict):
        return None

    # 2. Detect LIVE or VOD
    is_live = data.get("isLive", False)

    # 3. Get replayChatItemAction
    replay_action = data.get("replayChatItemAction")

    if not isinstance(replay_action, dict):
        return None

    actions = replay_action.get("actions", [])

    # 4. Find actual chat messages
    for action in actions:

        if not isinstance(action, dict):
            continue

        add_action = action.get("addChatItemAction")

        if not isinstance(add_action, dict):
            continue

        item = add_action.get("item", {})

        if not isinstance(item, dict):
            continue

        # 5. Detect renderer
        renderer_name = None
        renderer = None

        for key, value in item.items():
            if (
                key.endswith(("Renderer", "ViewModel"))
                and isinstance(value, dict)
            ):
                renderer_name = key
                renderer = value
                break

        if renderer is None:
            continue

        # 6. Skip system and placeholder events
        if renderer_name in (
            "liveChatPlaceholderItemRenderer",
            "liveChatViewerEngagementMessageRenderer",
        ):
            continue

        # 7. Extract message ID
        message_id = renderer.get("id")

        if not message_id:
            continue

        # 8. Extract author
        author = (
            renderer.get("authorName") or {}
        ).get("simpleText")

        # 9. Extract message text
        message = extract_text(
            renderer.get("message")
        )

        # 10. Convert timestampUsec to ISO datetime
        timestamp = timestamp_to_datetime(
            renderer.get("timestampUsec")
        )

        
        # Handle giftMessageViewModel separately
        if renderer_name == "giftMessageViewModel":

            author_data = renderer.get("authorName") or {}
            text_data = renderer.get("text") or {}

            # Gift uses "content" instead of "simpleText"
            author = author_data.get("content") or author

            # Gift announcement text is stored in "text.content"
            message = text_data.get("content") or message

            # No timestampUsec was found in the inspected gift schema.
            # Keep timestamp = None rather than inventing one.


        # 11. Handle LIVE / VOD timing
        if is_live:
            elapsed_time = None
            elapsed_time_ms = None

        else:
            elapsed_time = (
                renderer.get("timestampText") or {}
            ).get("simpleText")

            raw_offset = replay_action.get(
                "videoOffsetTimeMsec"
            )

            try:
                elapsed_time_ms = (
                    int(raw_offset)
                    if raw_offset is not None
                    else None
                )
            except (ValueError, TypeError):
                elapsed_time_ms = None

        # 12. Normalize message type
        message_type = normalize_type(renderer_name)

        # 13. Extract Super Chat / Super Sticker amount
        amount = None

        if renderer_name in (
            "liveChatPaidMessageRenderer",
            "liveChatPaidStickerRenderer",
        ):
            amount = (
                renderer.get("purchaseAmountText") or {}
            ).get("simpleText")

        # 14. Return normalized message
        return {
            "message_id": message_id,
            "video_id": video_id,
            "author": author,
            "message": message,
            "timestamp": timestamp,
            "elapsed_time": elapsed_time,
            "elapsed_time_ms": elapsed_time_ms,
            "type": message_type,
            "amount": amount,
        }

    return None



def download_live_chat(video_url, output_dir):
    """
    Use yt-dlp to download live chat.

    yt-dlp will handle:
    - VOD replay chat
    - live stream chat
    """

    output_dir = Path(output_dir)

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    options = {
        "skip_download": True,
        "writesubtitles": True,
        "subtitleslangs": ["live_chat"],
        "outtmpl": str(output_dir / "%(id)s"),
        "quiet": False,
    }

    with yt_dlp.YoutubeDL(options) as ydl:
        info = ydl.extract_info(
            video_url,
            download=True,
        )

    video_id = info["id"]

    chat_file = output_dir / f"{video_id}.live_chat.json"

    return video_id, chat_file


def parse_live_chat_file(
    chat_file,
    video_id,
):
    """
    Read yt-dlp NDJSON file and
    convert it into normalized records.
    """

    messages = []

    with open(
        chat_file,
        "r",
        encoding="utf-8",
    ) as f:
        for line in f:
            line = line.strip()

            if not line:
                continue

            record = parse_chat_line(
                line,
                video_id,
            )

            if record is None:
                continue

            messages.append(record)

    return messages


def save_messages(
    messages,
    video_id,
):
    """
    Save normalized messages.
    """

    output_path = PROJECT_ROOT / "data" / "raw" / f"livechat_{video_id}.json"

    output_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with open(
        output_path,
        "w",
        encoding="utf-8",
    ) as f:
        json.dump(
            messages,
            f,
            ensure_ascii=False,
            indent=4,
        )

    return output_path


def fetch_livechat(video_url):
    """
    Main ingestion function.
    """

    print("[+] Fetching YouTube live chat...")

    temp_dir = PROJECT_ROOT / "data" / "raw" / "livechat_download"

    try:
        # -----------------------------------------------------
        # 1. Download raw chat using yt-dlp
        # -----------------------------------------------------

        video_id, chat_file = download_live_chat(
            video_url,
            temp_dir,
        )

        print(f"[+] Video ID: {video_id}")

        print(f"[+] Raw chat file: {chat_file}")

        # -----------------------------------------------------
        # 2. Check file
        # -----------------------------------------------------

        if not chat_file.exists():
            raise FileNotFoundError(f"Live chat file not found: {chat_file}")

        # -----------------------------------------------------
        # 3. Parse raw NDJSON
        # -----------------------------------------------------

        messages = parse_live_chat_file(
            chat_file,
            video_id,
        )

        # -----------------------------------------------------
        # 4. Check messages
        # -----------------------------------------------------

        if not messages:
            raise ValueError("No usable live chat messages found.")

        print(f"[+] Parsed {len(messages)} messages.")

        # -----------------------------------------------------
        # 5. Save normalized data
        # -----------------------------------------------------

        output_path = save_messages(
            messages,
            video_id,
        )

        print(f"[+] Successfully saved to:" f"\n    {output_path}")

        return output_path

    except Exception as e:
        raise Exception(f"Live chat ingestion failed: {e}") from e
