"""
VOD (Video on Demand) REST API routes.

Endpoints:
  POST   /api/vod/videos              - Upload and ingest a video file
  POST   /api/vod/videos/{id}/process - Start transcoding
  GET    /api/vod/videos              - List all VOD assets
  GET    /api/vod/videos/{id}         - Get asset details and progress
  DELETE /api/vod/videos/{id}         - Delete asset and all files
"""

import asyncio
import os
import tempfile

from fastapi import APIRouter, BackgroundTasks, File, HTTPException, Request, UploadFile
from pydantic import BaseModel
from typing import List, Optional

router = APIRouter(prefix="/api/vod", tags=["Video on Demand"])


# Response model

class VideoResponse(BaseModel):
    video_id: str
    original_filename: str
    status: str
    progress: float
    duration_secs: float
    file_size_bytes: int
    hls_master_url: Optional[str]
    created_at: str
    completed_at: Optional[str]
    error: Optional[str]


def _to_response(video) -> VideoResponse:
    return VideoResponse(
        video_id=video.video_id,
        original_filename=video.original_filename,
        status=video.status,
        progress=round(video.progress, 1),
        duration_secs=round(video.duration_secs, 2),
        file_size_bytes=video.file_size_bytes,
        hls_master_url=video.hls_master_url,
        created_at=video.created_at.isoformat(),
        completed_at=video.completed_at.isoformat() if video.completed_at else None,
        error=video.error,
    )


# Routes

@router.post("/videos", response_model=VideoResponse, status_code=202)
async def upload_video(
    request: Request,
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
):
    """
    Upload a video file for VOD processing.

    The file is saved to disk immediately; transcoding starts in the
    background. Poll GET /api/vod/videos/{id} for progress.
    """
    processor = request.app.state.vod_processor

    # Stream upload to a temp file to handle large files safely
    suffix = os.path.splitext(file.filename or "video.mp4")[1].lower()
    with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
        tmp_path = tmp.name
        while True:
            chunk = await file.read(1024 * 1024)  # read 1 MB at a time
            if not chunk:
                break
            tmp.write(chunk)

    try:
        video = await processor.ingest(tmp_path, file.filename or "upload.mp4")
    except ValueError as exc:
        os.unlink(tmp_path)
        raise HTTPException(status_code=400, detail=str(exc))

    # Kick off transcoding as a background task
    background_tasks.add_task(_process_video, processor, video.video_id)

    return _to_response(video)


@router.post("/videos/{video_id}/process", response_model=VideoResponse)
async def reprocess_video(video_id: str, request: Request, background_tasks: BackgroundTasks):
    """
    Re-trigger transcoding for an already-uploaded video
    (useful if processing previously failed).
    """
    processor = request.app.state.vod_processor
    video = processor.get_video(video_id)
    if not video:
        raise HTTPException(status_code=404, detail=f"Video '{video_id}' not found")
    if video.status == "processing":
        raise HTTPException(status_code=409, detail="Video is already being processed")

    video.status = "uploaded"
    video.progress = 0.0
    video.error = None
    background_tasks.add_task(_process_video, processor, video_id)
    return _to_response(video)


@router.get("/videos", response_model=List[VideoResponse])
async def list_videos(request: Request):
    """List all VOD assets and their current status."""
    processor = request.app.state.vod_processor
    return [_to_response(v) for v in processor.list_videos()]


@router.get("/videos/{video_id}", response_model=VideoResponse)
async def get_video(video_id: str, request: Request):
    """Get details and transcoding progress for a single VOD asset."""
    processor = request.app.state.vod_processor
    video = processor.get_video(video_id)
    if not video:
        raise HTTPException(status_code=404, detail=f"Video '{video_id}' not found")
    return _to_response(video)


@router.delete("/videos/{video_id}", status_code=204)
async def delete_video(video_id: str, request: Request):
    """Delete a VOD asset and all associated files."""
    processor = request.app.state.vod_processor
    video = processor.get_video(video_id)
    if not video:
        raise HTTPException(status_code=404, detail=f"Video '{video_id}' not found")
    await processor.delete(video_id)


# Background helpers

async def _process_video(processor, video_id: str) -> None:
    """Runs the full VOD transcoding pipeline as a background task."""
    try:
        await processor.process(video_id)
    except Exception as exc:
        from src.utils import logger
        logger.error(f"Background processing error for {video_id}: {exc}")
