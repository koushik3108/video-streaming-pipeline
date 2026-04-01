"""
Integration-style tests for the VOD processing pipeline.
Run with: pytest tests/ -v
"""

import os
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.pipeline.transcoder import FFmpegTranscoder, Rendition
from src.pipeline.vod_processor import VODProcessor


# ----- Fixtures -----

RENDITIONS_CONFIG = [
    {"name": "720p", "width": 1280, "height": 720, "video_bitrate": "2000k", "audio_bitrate": "128k"},
    {"name": "480p", "width": 854,  "height": 480, "video_bitrate": "800k",  "audio_bitrate": "96k"},
]


@pytest.fixture
def tmp_dirs(tmp_path):
    upload_dir = tmp_path / "uploads"
    output_dir = tmp_path / "vod"
    upload_dir.mkdir()
    output_dir.mkdir()
    return upload_dir, output_dir


@pytest.fixture
def mock_transcoder():
    t = MagicMock(spec=FFmpegTranscoder)
    t.ffmpeg_bin = "ffmpeg"
    t.ffprobe_bin = "ffprobe"
    t.threads = 0
    t.get_duration.return_value = 60.0
    t.build_hls_command.return_value = ["ffmpeg", "-version"]
    t.run_async = AsyncMock(return_value=(0, ""))
    t.write_master_playlist.return_value = "/tmp/master.m3u8"
    return t


@pytest.fixture
def processor(tmp_dirs, mock_transcoder):
    upload_dir, output_dir = tmp_dirs
    return VODProcessor(
        transcoder=mock_transcoder,
        upload_dir=str(upload_dir),
        output_base_dir=str(output_dir),
        renditions=RENDITIONS_CONFIG,
        segment_duration=6,
        base_url="http://localhost:8080",
    )


# ----- Ingest tests -----

class TestVODIngest:
    @pytest.mark.asyncio
    async def test_ingest_moves_file_to_upload_dir(self, processor, tmp_path):
        # Create a temp source file
        src = tmp_path / "sample.mp4"
        src.write_bytes(b"fake video data")
        video = await processor.ingest(str(src), "sample.mp4")
        assert video.status == "uploaded"
        assert Path(video.upload_path).exists()
        assert video.video_id.startswith("vod-")

    @pytest.mark.asyncio
    async def test_ingest_rejects_unsupported_extension(self, processor, tmp_path):
        src = tmp_path / "script.exe"
        src.write_bytes(b"not a video")
        with pytest.raises(ValueError, match="Unsupported file type"):
            await processor.ingest(str(src), "script.exe")

    @pytest.mark.asyncio
    async def test_ingest_rejects_oversized_file(self, processor, tmp_path):
        src = tmp_path / "big.mp4"
        src.write_bytes(b"x")  # tiny file but we'll mock the size check
        processor.max_upload_bytes = 0  # force rejection
        with pytest.raises(ValueError, match="File too large"):
            await processor.ingest(str(src), "big.mp4")


# ----- Process tests -----

class TestVODProcess:
    @pytest.mark.asyncio
    async def test_process_sets_status_ready_on_success(self, processor, tmp_path):
        src = tmp_path / "video.mp4"
        src.write_bytes(b"fake data")
        video = await processor.ingest(str(src), "video.mp4")
        result = await processor.process(video.video_id)
        assert result.status == "ready"
        assert result.progress == 100.0

    @pytest.mark.asyncio
    async def test_process_sets_status_failed_on_ffmpeg_error(self, processor, tmp_path, mock_transcoder):
        mock_transcoder.run_async = AsyncMock(return_value=(1, "FFmpeg error output"))
        src = tmp_path / "video.mp4"
        src.write_bytes(b"fake data")
        video = await processor.ingest(str(src), "video.mp4")
        result = await processor.process(video.video_id)
        assert result.status == "failed"
        assert result.error is not None

    @pytest.mark.asyncio
    async def test_process_records_duration(self, processor, tmp_path, mock_transcoder):
        mock_transcoder.get_duration.return_value = 120.5
        src = tmp_path / "long.mp4"
        src.write_bytes(b"fake data")
        video = await processor.ingest(str(src), "long.mp4")
        result = await processor.process(video.video_id)
        assert result.duration_secs == pytest.approx(120.5)


# ----- Query tests -----

class TestVODQueries:
    @pytest.mark.asyncio
    async def test_list_videos(self, processor, tmp_path):
        for i in range(3):
            src = tmp_path / f"v{i}.mp4"
            src.write_bytes(b"x")
            await processor.ingest(str(src), f"v{i}.mp4")
        assert len(processor.list_videos()) == 3

    @pytest.mark.asyncio
    async def test_get_unknown_video_returns_none(self, processor):
        assert processor.get_video("nonexistent-id") is None
