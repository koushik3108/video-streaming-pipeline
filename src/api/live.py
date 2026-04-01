"""
Live streaming REST API routes.

Endpoints:
  POST   /api/live/streams              - Create a new stream slot
  POST   /api/live/streams/{id}/start   - Start streaming
  POST   /api/live/streams/{id}/stop    - Stop streaming
  GET    /api/live/streams              - List all streams
  GET    /api/live/streams/{id}         - Get stream details
  DELETE /api/live/streams/{id}         - Remove a stream
"""

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel
from typing import List, Optional

router = APIRouter(prefix="/api/live", tags=["Live Streaming"])


# Request / Response models

class CreateStreamRequest(BaseModel):
    stream_key: str
    rtmp_base: str = "rtmp://localhost:1935/live"


class StreamResponse(BaseModel):
    stream_id: str
    stream_key: str
    rtmp_url: str
    status: str
    hls_master_url: Optional[str]
    viewers: int
    started_at: Optional[str]
    ended_at: Optional[str]


def _to_response(stream) -> StreamResponse:
    return StreamResponse(
        stream_id=stream.stream_id,
        stream_key=stream.stream_key,
        rtmp_url=stream.rtmp_url,
        status=stream.status,
        hls_master_url=stream.hls_master_url,
        viewers=stream.viewers,
        started_at=stream.started_at.isoformat() if stream.started_at else None,
        ended_at=stream.ended_at.isoformat() if stream.ended_at else None,
    )


# Routes

@router.post("/streams", response_model=StreamResponse, status_code=201)
async def create_stream(body: CreateStreamRequest, request: Request):
    """
    Register a new live stream slot.
    Returns RTMP ingest URL and HLS playback URL.
    """
    manager = request.app.state.live_manager
    stream = manager.create_stream(
        stream_key=body.stream_key,
        rtmp_base=body.rtmp_base,
    )
    return _to_response(stream)


@router.post("/streams/{stream_id}/start", response_model=StreamResponse)
async def start_stream(stream_id: str, request: Request):
    """Launch the FFmpeg ingest process and begin HLS output."""
    manager = request.app.state.live_manager
    stream = manager.get_stream(stream_id)
    if not stream:
        raise HTTPException(status_code=404, detail=f"Stream '{stream_id}' not found")
    stream = await manager.start_stream(stream_id)
    return _to_response(stream)


@router.post("/streams/{stream_id}/stop", response_model=StreamResponse)
async def stop_stream(stream_id: str, request: Request):
    """Terminate the live FFmpeg process and finalize the stream."""
    manager = request.app.state.live_manager
    stream = manager.get_stream(stream_id)
    if not stream:
        raise HTTPException(status_code=404, detail=f"Stream '{stream_id}' not found")
    stream = await manager.stop_stream(stream_id)
    return _to_response(stream)


@router.get("/streams", response_model=List[StreamResponse])
async def list_streams(request: Request, active_only: bool = False):
    """List all registered streams (optionally filter to live-only)."""
    manager = request.app.state.live_manager
    streams = manager.get_active_streams() if active_only else manager.list_streams()
    return [_to_response(s) for s in streams]


@router.get("/streams/{stream_id}", response_model=StreamResponse)
async def get_stream(stream_id: str, request: Request):
    """Get full details for a single stream."""
    manager = request.app.state.live_manager
    stream = manager.get_stream(stream_id)
    if not stream:
        raise HTTPException(status_code=404, detail=f"Stream '{stream_id}' not found")
    return _to_response(stream)
