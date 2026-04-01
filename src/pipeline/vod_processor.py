"""
VOD (Video on Demand) pipeline.

Flow: Upload, Validate, Transcode (ABR HLS), Serve.

Each uploaded video gets a unique video_id. FFmpeg produces multiple
quality renditions packaged as HLS with a master playlist.
"""

import asyncio
import os
import shutil
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from src.pipeline.transcoder import FFmpegTranscoder, Rendition, TranscodeJob
from src.utils import logger, generate_stream_id, ensure_dir, get_file_extension, human_readable_size


@dataclass
class VODVideo:
    """Represents a VOD asset at any stage of its lifecycle."""
    video_id: str
    original_filename: str
    upload_path: str
    output_dir: str
    renditions: List[Rendition]
    status: str = "uploaded"         # uploaded | processing | ready | failed
    progress: float = 0.0
    duration_secs: float = 0.0
    file_size_bytes: int = 0
    hls_master_url: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    completed_at: Optional[datetime] = None
    error: Optional[str] = None
    _job: Optional[TranscodeJob] = field(default=None, repr=False)


class VODProcessor:
    """
    Handles VOD ingestion and transcoding.

    Typical usage:
        processor = VODProcessor(transcoder, ...)
        video = await processor.ingest(file_path, original_filename)
        asyncio.create_task(processor.process(video.video_id))
        # poll video.status / video.progress
    """

    ALLOWED_EXTENSIONS = {".mp4", ".mov", ".avi", ".mkv", ".webm", ".flv", ".ts"}

    def __init__(
        self,
        transcoder: FFmpegTranscoder,
        upload_dir: str,
        output_base_dir: str,
        renditions: List[Dict],
        segment_duration: int = 6,
        base_url: str = "http://localhost:8080",
        max_upload_bytes: int = 2 * 1024 ** 3,  # 2 GB default
    ):
        self.transcoder = transcoder
        self.upload_dir = Path(upload_dir)
        self.output_base_dir = Path(output_base_dir)
        self.segment_duration = segment_duration
        self.base_url = base_url.rstrip("/")
        self.max_upload_bytes = max_upload_bytes
        self._videos: Dict[str, VODVideo] = {}

        self.renditions = [
            Rendition(
                name=r["name"],
                width=r["width"],
                height=r["height"],
                video_bitrate=r["video_bitrate"],
                audio_bitrate=r["audio_bitrate"],
                fps=r.get("fps", 30),
            )
            for r in renditions
        ]

    # Ingest

    async def ingest(self, temp_path: str, original_filename: str) -> VODVideo:
        """
        Accept an uploaded file:
          1. Validate extension and size
          2. Move to permanent upload directory
          3. Register the VODVideo record

        Returns the VODVideo (status='uploaded'). Call process() next.
        """
        ext = get_file_extension(original_filename)
        if ext not in self.ALLOWED_EXTENSIONS:
            raise ValueError(
                f"Unsupported file type '{ext}'. Allowed: {', '.join(sorted(self.ALLOWED_EXTENSIONS))}"
            )

        size = os.path.getsize(temp_path)
        if size > self.max_upload_bytes:
            raise ValueError(
                f"File too large: {human_readable_size(size)} "
                f"(limit {human_readable_size(self.max_upload_bytes)})"
            )

        video_id = generate_stream_id("vod")
        dest_path = self.upload_dir / f"{video_id}{ext}"
        ensure_dir(str(self.upload_dir))
        shutil.move(temp_path, dest_path)

        output_dir = str(self.output_base_dir / video_id)
        ensure_dir(output_dir)

        video = VODVideo(
            video_id=video_id,
            original_filename=original_filename,
            upload_path=str(dest_path),
            output_dir=output_dir,
            renditions=self.renditions,
            file_size_bytes=size,
            hls_master_url=f"{self.base_url}/streams/vod/{video_id}/master.m3u8",
        )
        self._videos[video_id] = video
        logger.info(
            f"VOD ingested: id={video_id}, file={original_filename}, "
            f"size={human_readable_size(size)}"
        )
        return video

    # Processing

    async def process(self, video_id: str) -> VODVideo:
        """
        Run the full transcoding pipeline for a VOD asset:
          - Probe duration
          - Run multi-rendition FFmpeg transcode
          - Write master HLS playlist
          - Update status
        """
        video = self._get_or_raise(video_id)
        video.status = "processing"
        video.progress = 0.0

        try:
            # Probe the source for duration (enables progress tracking)
            logger.info(f"Probing {video_id}: {video.upload_path}")
            video.duration_secs = self.transcoder.get_duration(video.upload_path)
            logger.info(f"{video_id} duration: {video.duration_secs:.1f}s")

            # Build and run the FFmpeg transcode command
            cmd = self.transcoder.build_hls_command(
                input_path=video.upload_path,
                output_dir=video.output_dir,
                renditions=video.renditions,
                segment_duration=self.segment_duration,
                is_live=False,
            )

            job = TranscodeJob(
                job_id=video_id,
                input_path=video.upload_path,
                output_dir=video.output_dir,
                renditions=video.renditions,
                segment_duration=self.segment_duration,
            )
            video._job = job

            # Bind progress to video
            async def progress_poll():
                while job.status == "running":
                    video.progress = job.progress
                    await asyncio.sleep(0.5)

            poll_task = asyncio.create_task(progress_poll())
            rc, stderr = await self.transcoder.run_async(
                cmd, job=job, duration_secs=video.duration_secs
            )
            poll_task.cancel()

            if rc != 0:
                raise RuntimeError(f"FFmpeg exited {rc}: {stderr[-400:]}")

            # Write master playlist
            self.transcoder.write_master_playlist(video.output_dir, video.renditions)

            video.status = "ready"
            video.progress = 100.0
            video.completed_at = datetime.utcnow()
            logger.info(f"VOD {video_id} ready at {video.hls_master_url}")

        except Exception as exc:
            video.status = "failed"
            video.error = str(exc)
            logger.error(f"VOD processing failed for {video_id}: {exc}")

        return video

    async def delete(self, video_id: str) -> None:
        """Remove all files associated with a VOD asset."""
        video = self._get_or_raise(video_id)

        # Cancel if still processing
        if video._job and video.status == "processing":
            await self.transcoder.cancel_job(video._job)

        # Delete files
        for path in [video.upload_path, video.output_dir]:
            p = Path(path)
            if p.exists():
                if p.is_dir():
                    shutil.rmtree(p)
                else:
                    p.unlink()

        del self._videos[video_id]
        logger.info(f"VOD {video_id} deleted.")

    # Queries

    def get_video(self, video_id: str) -> Optional[VODVideo]:
        return self._videos.get(video_id)

    def list_videos(self) -> List[VODVideo]:
        return list(self._videos.values())

    def _get_or_raise(self, video_id: str) -> VODVideo:
        video = self._videos.get(video_id)
        if not video:
            raise KeyError(f"Video '{video_id}' not found.")
        return video
