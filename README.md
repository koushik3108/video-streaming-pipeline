# Video Streaming Pipeline

An end-to-end video streaming backend built with **Python + FFmpeg**, supporting both **live RTMP streaming** and **VOD (Video on Demand)** with adaptive-bitrate HLS output.

---

## Features

- **Live Streaming** — Ingest RTMP from OBS/encoders, transcode to multi-rendition HLS in real-time
- **VOD Processing** — Upload video files; automatically transcode to 4-quality ABR HLS ladder
- **Adaptive Bitrate (ABR)** — 1080p / 720p / 480p / 360p quality levels with master playlist
- **REST API** — Full FastAPI CRUD for managing streams and videos
- **Web Player** — Built-in HLS.js player at `/player`
- **Storage Backends** — Local disk or Amazon S3
- **Progress Tracking** — Poll transcoding progress in real-time (0–100%)

---

## Prerequisites

```bash
# FFmpeg (required)
sudo apt install ffmpeg        # Ubuntu/Debian
brew install ffmpeg             # macOS

# Python 3.10+
python --version

# (Optional) NGINX with RTMP module for live ingest
sudo apt install nginx libnginx-mod-rtmp
```

---

## Quick Start

```bash
# 1. Clone / enter project
cd video-streaming-pipeline

# 2. Install dependencies
pip install -r requirements.txt

# 3. Start the server
uvicorn src.main:app --host 0.0.0.0 --port 8080 --reload

# API docs at: http://localhost:8080/docs
# Player at:   http://localhost:8080/player
```

---

## Live Streaming Workflow

### 1. Create a stream slot

```bash
curl -X POST http://localhost:8080/api/live/streams \
  -H "Content-Type: application/json" \
  -d '{"stream_key": "my-show"}'
```

Response includes:
- `rtmp_url` — point your encoder here
- `hls_master_url` — share with viewers

### 2. Start the FFmpeg ingest process

```bash
curl -X POST http://localhost:8080/api/live/streams/{stream_id}/start
```

### 3. Configure OBS

- **Server:** `rtmp://localhost:1935/live`
- **Stream Key:** `my-show`

### 4. Stop the stream

```bash
curl -X POST http://localhost:8080/api/live/streams/{stream_id}/stop
```

---

## VOD Workflow

### 1. Upload a video

```bash
curl -X POST http://localhost:8080/api/vod/videos \
  -F "file=@/path/to/video.mp4"
```

Returns a `video_id`. Transcoding starts automatically in the background.

### 2. Poll for progress

```bash
curl http://localhost:8080/api/vod/videos/{video_id}
# {"status": "processing", "progress": 47.3, ...}
```

### 3. Play when ready

```bash
# status = "ready"
# Open: http://localhost:8080/player#/streams/vod/{video_id}/master.m3u8
```

---

## API Reference

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/api/live/streams` | Create live stream slot |
| `POST` | `/api/live/streams/{id}/start` | Start FFmpeg ingest |
| `POST` | `/api/live/streams/{id}/stop` | Stop live broadcast |
| `GET`  | `/api/live/streams` | List all streams |
| `GET`  | `/api/live/streams/{id}` | Stream details |
| `POST` | `/api/vod/videos` | Upload & start transcoding |
| `GET`  | `/api/vod/videos` | List all VOD assets |
| `GET`  | `/api/vod/videos/{id}` | Asset details + progress |
| `POST` | `/api/vod/videos/{id}/process` | Re-trigger transcoding |
| `DELETE` | `/api/vod/videos/{id}` | Delete asset + files |

Full interactive docs: `http://localhost:8080/docs`

---

## Project Structure

```
video-streaming-pipeline/
├── src/
│   ├── main.py                  # FastAPI app + startup
│   ├── api/
│   │   ├── live.py              # Live streaming endpoints
│   │   └── vod.py               # VOD endpoints
│   ├── pipeline/
│   │   ├── transcoder.py        # FFmpeg wrapper + HLS packager
│   │   ├── live_ingest.py       # Live stream session manager
│   │   └── vod_processor.py     # VOD upload + transcode pipeline
│   ├── storage/
│   │   └── manager.py           # Local / S3 storage abstraction
│   └── utils/
│       ├── logger.py            # Loguru logging setup
│       └── helpers.py           # Shared utilities
├── static/
│   └── player.html              # HLS.js web player
├── streams/                     # HLS output (served as static files)
├── uploads/                     # Incoming VOD uploads
├── logs/                        # Rotating log files
├── tests/
│   ├── test_transcoder.py
│   └── test_vod_pipeline.py
├── config.yaml                  # All configuration
└── requirements.txt
```

---

## Configuration

Edit `config.yaml` to customize:

- FFmpeg binary paths
- HLS segment duration
- ABR rendition ladder (bitrates, resolutions)
- Storage backend (local / S3)
- CORS origins

---

## Running Tests

```bash
pip install pytest pytest-asyncio
pytest tests/ -v
```

---

## S3 Storage

To use Amazon S3 instead of local disk:

1. Set in `config.yaml`:
   ```yaml
   storage:
     backend: "s3"
     s3:
       bucket_name: "my-video-bucket"
       region: "us-east-1"
   ```

2. Provide credentials via environment:
   ```bash
   export AWS_ACCESS_KEY_ID=...
   export AWS_SECRET_ACCESS_KEY=...
   ```
