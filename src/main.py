"""
Video Streaming Pipeline - FastAPI Application Entry Point

Start with:
    uvicorn src.main:app --host 0.0.0.0 --port 8080 --reload
"""

import yaml
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from src.api import live_router, vod_router
from src.pipeline import FFmpegTranscoder, LiveStreamManager, VODProcessor
from src.storage import StorageManager
from src.utils import logger, setup_logger, ensure_dir


# Load configuration

def load_config(path: str = "config.yaml") -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)


# App lifespan (startup / shutdown)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize all pipeline components on startup."""
    config = load_config()

    # Logging
    log_cfg = config.get("logging", {})
    setup_logger(log_dir=log_cfg.get("dir", "logs"), level=log_cfg.get("level", "INFO"))
    logger.info("Video Streaming Pipeline starting up...")

    # Ensure output directories exist
    ensure_dir(config["live"]["output_dir"])
    ensure_dir(config["vod"]["upload_dir"])
    ensure_dir(config["vod"]["output_dir"])

    # Core transcoder
    ffcfg = config.get("ffmpeg", {})
    transcoder = FFmpegTranscoder(
        ffmpeg_bin=ffcfg.get("binary", "ffmpeg"),
        ffprobe_bin=ffcfg.get("ffprobe_binary", "ffprobe"),
        threads=ffcfg.get("threads", 0),
    )

    # Storage manager
    storage_cfg = config.get("storage", {})
    s3_cfg = storage_cfg.get("s3", {})
    storage = StorageManager(
        backend=storage_cfg.get("backend", "local"),
        local_base_dir=storage_cfg.get("local", {}).get("base_dir", "."),
        s3_bucket=s3_cfg.get("bucket_name"),
        s3_region=s3_cfg.get("region", "us-east-1"),
        s3_prefix=s3_cfg.get("prefix", "streams/"),
        base_url=f"http://{config['server']['host']}:{config['server']['port']}",
    )

    # Live stream manager
    live_cfg = config["live"]
    live_manager = LiveStreamManager(
        transcoder=transcoder,
        output_base_dir=live_cfg["output_dir"],
        renditions=live_cfg["renditions"],
        segment_duration=live_cfg.get("segment_duration", 2),
        base_url=f"http://localhost:{config['server']['port']}",
    )

    # VOD processor
    vod_cfg = config["vod"]
    vod_processor = VODProcessor(
        transcoder=transcoder,
        upload_dir=vod_cfg["upload_dir"],
        output_base_dir=vod_cfg["output_dir"],
        renditions=vod_cfg["renditions"],
        segment_duration=vod_cfg.get("segment_duration", 6),
        base_url=f"http://localhost:{config['server']['port']}",
        max_upload_bytes=vod_cfg.get("max_upload_size_mb", 2048) * 1024 * 1024,
    )

    # Attach to app state for use in route handlers
    app.state.config = config
    app.state.transcoder = transcoder
    app.state.storage = storage
    app.state.live_manager = live_manager
    app.state.vod_processor = vod_processor

    logger.info("All pipeline components initialized.")
    yield  # app is running

    # Shutdown: gracefully stop any active live streams
    logger.info("Shutting down pipeline...")
    for stream in live_manager.get_active_streams():
        await live_manager.stop_stream(stream.stream_id)
    logger.info("Shutdown complete.")


# App factory

def create_app(config_path: str = "config.yaml") -> FastAPI:
    config = load_config(config_path)

    app = FastAPI(
        title="Video Streaming Pipeline",
        description=(
            "End-to-end video streaming pipeline with live RTMP ingest, "
            "multi-rendition HLS transcoding, and VOD processing."
        ),
        version="1.0.0",
        docs_url="/docs",
        redoc_url="/redoc",
        lifespan=lifespan,
    )

    # CORS
    origins = config.get("server", {}).get("cors_origins", ["*"])
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # API routers
    app.include_router(live_router)
    app.include_router(vod_router)

    # Serve HLS files and the web player as static content
    streams_dir = Path("streams")
    streams_dir.mkdir(exist_ok=True)
    app.mount("/streams", StaticFiles(directory="streams"), name="streams")
    app.mount("/player", StaticFiles(directory="static", html=True), name="player")

    # Health check
    @app.get("/health", tags=["Health"])
    async def health():
        return {"status": "ok", "service": "video-streaming-pipeline"}

    # Root info
    @app.get("/", tags=["Info"])
    async def root():
        return {
            "service": "Video Streaming Pipeline",
            "version": "1.0.0",
            "docs": "/docs",
            "player": "/player",
            "endpoints": {
                "live": "/api/live/streams",
                "vod": "/api/vod/videos",
            },
        }

    return app


# Create the ASGI app instance
app = create_app()


# CLI entry point

if __name__ == "__main__":
    import uvicorn
    cfg = load_config()
    srv = cfg.get("server", {})
    uvicorn.run(
        "src.main:app",
        host=srv.get("host", "0.0.0.0"),
        port=srv.get("port", 8080),
        reload=srv.get("debug", False),
        log_level="info",
    )
