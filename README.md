# YouTube Data Ingestion Pipeline — Python + PySpark

Pipeline thu thập bình luận, live chat VOD và live chat của livestream YouTube, sau đó lưu dữ liệu có cấu trúc bằng Apache Spark.

## Cấu trúc project

```text
Youtube_data_ingestion/
├── main.py                        # CLI tập trung, chạy tất cả chức năng
├── requirements.txt
├── .env.example                   # Mẫu cấu hình (không có API key thật)
├── data/
│   ├── video_links.txt            # Danh sách URL cho chế độ batch
│   └── demo/livechat_101/         # 101 record Parquet đã chạy thử (mẫu)
├── src/
│   ├── ingestion/
│   │   ├── youtube_api.py         # YouTube comments qua API
│   │   ├── livechat_scraper.py    # VOD chat / chuẩn hoá chat
│   │   └── livechat_stream.py     # Producer cho livestream
│   └── processing/
│       ├── extract_video_id.py
│       ├── spark_processor.py     # Xử lý comments (batch)
│       └── spark_streaming.py     # Spark Structured Streaming
├── BAO_CAO_DU_AN.docx
└── HUONG_DAN_NOP_BAI.md
```

Các thư mục `data/raw`, `data/streaming`, `data/processed`, `data/checkpoints` được tạo khi chạy; không kèm dữ liệu raw lớn hoặc checkpoint trong bài nộp.

## Cài đặt

Yêu cầu Python, Java/JDK tương thích PySpark và các thư viện:

```powershell
python -m pip install -r requirements.txt
```

Riêng chức năng đọc **comments** dùng YouTube Data API. Sao chép `.env.example` thành `.env` rồi điền `YOUTUBE_API_KEY` (không đưa `.env` vào bản nộp). Các chế độ chat bằng `yt-dlp` không dùng API key này. Nếu yt-dlp báo thiếu JavaScript runtime, cần bổ sung runtime theo tài liệu phiên bản yt-dlp bạn đang dùng.

## Chạy từ một menu duy nhất

Đứng tại thư mục gốc dự án (hoặc chỉ định đường dẫn `main.py`):

```powershell
python main.py
```

| Menu | Chức năng |
|---|---|
| **1** | Lấy comments của một video từ YouTube Data API → `data/raw/comments_<id>.json` |
| **2** | Tải live chat replay VOD nếu YouTube cung cấp track `live_chat` → `data/raw/livechat_<id>.json` |
| **3** | Lấy comments theo URL trong `data/video_links.txt` |
| **4** | PySpark xử lý comments JSON → `data/processed/comments/` |
| **5** | **Tự khởi động cả Spark consumer và producer cho livestream** từ một URL |
| **6** | Xem/đếm dữ liệu Parquet của livestream vừa chạy hoặc **demo 101 records** |
| **0** | Thoát |

### Chạy livestream bằng menu 5

1. Chạy `python main.py`, chọn **5** và nhập URL một livestream **đang hoạt động**, có bật live chat.
2. Menu khởi động hai process: `spark_streaming.py` và `livechat_stream.py`; cả hai chạy đồng thời trong cùng cửa sổ console, **không cần mở hai terminal**.
3. Theo dõi log `Published batch ...` (producer) và `Input rows: ...` (Spark).
4. **Nhấn Enter trong menu** để dừng producer trước, dành thêm thời gian cho Spark đọc batch cuối rồi dừng Spark. Trên Windows nên dùng Enter thay vì Ctrl+C để tránh ngắt Java/Py4J đột ngột.
5. Chọn **6 → 1** để xem kết quả streaming thật. Chọn **6 → 2** để xem bộ dữ liệu demo kể cả khi không có livestream.

Cấu trúc dữ liệu trong chế độ streaming:

```text
YouTube live chat → yt-dlp (.live_chat.json.part)
                  → parse_chat_line()
                  → data/streaming/staging/       (file tạm)
                  → data/streaming/livechat_inbox/ (file JSON hoàn chỉnh)
                  → Spark Structured Streaming
                  → data/processed/livechat_stream/video_id=<id>/*.parquet
                    checkpoint: data/checkpoints/livechat/
```

**Lưu ý về checkpoint:** để tiếp tục cùng logic streaming, giữ checkpoint hiện tại. Nếu thay đổi các phép stateful processing hoặc shuffle partitions thì không tùy tiện dùng checkpoint không tương thích; khi thử nghiệm cần dùng cặp input/output/checkpoint mới. Các tên thư mục `*_live_test_v1` thuộc lần kiểm thử cũ không còn được mã nguồn hiện tại sử dụng.

## Chạy từng thành phần riêng (khi cần debug)

```powershell
# Terminal 1
python -m src.processing.spark_streaming

# Terminal 2
python -m src.ingestion.livechat_stream "https://www.youtube.com/watch?v=LIVE_VIDEO_ID"
```

Nếu chạy theo hai terminal độc lập, có thể tạo file `stop_livechat_producer.flag` rồi `stop_spark_streaming.flag` lần lượt tại thư mục gốc để dừng qua tín hiệu.

## Kết quả thực tế đã kiểm thử (14/10/2026)

- Video: `gDTu22Qha1U`.
- Producer xuất **10 file batch**, tổng **101 sự kiện**.
- Spark đọc file JSON mới khi livestream đang diễn ra, ghi **101 record** vào Parquet theo `video_id`.
- Schema gồm: `message_id`, `video_id`, `author`, `message`, `timestamp`, `elapsed_time`, `elapsed_time_ms`, `type`, `amount`.
- Bộ Parquet 101 record được giữ tại `data/demo/livechat_101/` để trình diễn ngoại tuyến bằng menu **6 → 2**.
- Sau khi sửa parser Gift, tác giả và nội dung Gift được lấy từ `authorName.content` và `text.content`; Parquet của phiên kiểm thử cũ chưa được ghi lại từ parser mới.

## Giới hạn

- Chat replay VOD chỉ tải được nếu nguồn YouTube có cung cấp.
- Streaming dùng file-based micro-batch, không phải Kafka/WebSocket và chưa bật stateful `dropDuplicates()`; không cam kết loại trùng trong mọi tình huống.
- Event `gift` có thể không có `timestamp` trong renderer gốc. Chưa kiểm thử đầy đủ mọi renderer đặc biệt (Super Sticker, Membership...).
- Kết quả live thực tế phụ thuộc kết nối YouTube/yt-dlp và môi trường Spark/Java tại máy chạy.

## Bảo mật và nộp bài

Không đưa `.env`, `.git`, `data/raw`, `data/streaming`, `data/checkpoints`, dữ liệu cá nhân hoặc các thư mục ảo vào bài nộp. Giữ source, README, báo cáo và một bộ dữ liệu demo nhỏ. Có thể dùng `python tao_goi_nop.py` từ project hiện tại nếu cần đóng gói lại.
