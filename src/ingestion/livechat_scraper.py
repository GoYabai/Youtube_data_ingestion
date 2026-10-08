import json
from datetime import datetime, timezone
from pathlib import Path

import yt_dlp


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
    """
    Convert YouTube renderer name into a simpler type.
    """

    type_mapping = {
        "liveChatTextMessageRenderer": "text",
        "liveChatPaidMessageRenderer": "super_chat",
        "liveChatMembershipItemRenderer": "membership",
        "liveChatPaidStickerRenderer": "paid_sticker",
        "liveChatViewerEngagementMessageRenderer": "system",
        "liveChatPlaceholderItemRenderer": "placeholder",
    }

    if renderer_name in type_mapping:
        return type_mapping[renderer_name]

    return renderer_name


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
    Parse one NDJSON line produced by yt-dlp.

    Return:
        normalized message dictionary
        or None if the line is not a real chat message.
    """

    try:
        data = json.loads(line)
    except json.JSONDecodeError:
        return None

    # ---------------------------------------------------------
    # 1. Get replayChatItemAction
    # ---------------------------------------------------------

    replay_action = data.get("replayChatItemAction")

    if not replay_action:
        return None

    # ---------------------------------------------------------
    # 2. Get actions
    # ---------------------------------------------------------

    actions = replay_action.get("actions", [])

    if not actions:
        return None

    # ---------------------------------------------------------
    # 3. Find addChatItemAction
    # ---------------------------------------------------------

    add_action = None

    for action in actions:
        if "addChatItemAction" in action:
            add_action = action["addChatItemAction"]
            break

    if not add_action:
        return None

    # ---------------------------------------------------------
    # 4. Get item
    # ---------------------------------------------------------

    item = add_action.get("item")

    if not item:
        return None

    # ---------------------------------------------------------
    # 5. Detect renderer type
    # ---------------------------------------------------------

    renderer_name = None
    renderer = None

    for key, value in item.items():
        if key.endswith("Renderer") or key.endswith("ViewModel"):
            renderer_name = key
            renderer = value
            break

    if renderer_name is None:
        return None

    # ---------------------------------------------------------
    # 6. Ignore placeholder
    # ---------------------------------------------------------

    if renderer_name == "liveChatPlaceholderItemRenderer":
        return None

    # ---------------------------------------------------------
    # 7. Extract author
    # ---------------------------------------------------------

    author = None

    author_name = renderer.get("authorName")

    if author_name:
        author = author_name.get("simpleText")

    # ---------------------------------------------------------
    # 8. Extract message
    # ---------------------------------------------------------

    message = extract_text(renderer.get("message"))

    # ---------------------------------------------------------
    # 9. Extract timestamps
    # ---------------------------------------------------------

    timestamp_usec = renderer.get("timestampUsec")

    timestamp = timestamp_to_datetime(timestamp_usec)

    timestamp_text = None

    timestamp_text_data = renderer.get("timestampText")

    if timestamp_text_data:
        timestamp_text = timestamp_text_data.get("simpleText")

    # ---------------------------------------------------------
    # 10. Extract replay offset
    # ---------------------------------------------------------

    video_offset_ms = replay_action.get("videoOffsetTimeMsec")

    if video_offset_ms is not None:
        try:
            video_offset_ms = int(video_offset_ms)

        except (ValueError, TypeError):
            video_offset_ms = None

    # ---------------------------------------------------------
    # 11. Normalize type
    # ---------------------------------------------------------

    message_type = normalize_type(renderer_name)

    amount = None

    if renderer_name == "liveChatPaidMessageRenderer":
        purchase_amount = renderer.get("purchaseAmountText")
        if purchase_amount:
            amount = purchase_amount.get("simpleText")

    # ---------------------------------------------------------
    # 12. Return normalized record
    # ---------------------------------------------------------

    return {
        "video_id": video_id,
        "author": author,
        "message": message,
        "timestamp": timestamp,
        "elapsed_time": timestamp_text,
        "elapsed_time_ms": video_offset_ms,
        "type": message_type,
        "amount": amount,
    }


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

    output_path = Path(f"data/raw/livechat_{video_id}.json")

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

    temp_dir = Path("data/raw/livechat_download")

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
