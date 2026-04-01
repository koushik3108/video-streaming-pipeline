"""
Live streaming pipeline manager.

Architecture:
  OBS/Encoder ingests via RTMP to FFmpeg, which outputs adaptive-bitrate
  HLS segments for delivery via CDN or direct player access.

This module manages live stream sessions: starting/stopping FFmpeg
processes that pull from an RTMP source and output adaptive-bitrate HLS.
"""

import asyncio
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from src.pipeline.transcoder import FFmpegTranscoder, Rendition, TranscodeJob
from src.utils import logger, generate_stream_id, ensure_dir, sanitize_stream_key


@dataclass
class LiveStream:
    """Represents an active (or historical) live stream session."""
    stream_id: str
    stream_key: str
    rtmp_url: str
    output_dir: str
    renditions: List[Rendition]
    status: str = "idle"          # idle | live | ended | error
    started_at: Optional[datetime] = None
    ended_at: Optional[datetime] = None
    viewers: int = 0
    hls_master_url: Optional[str] = None
    _job: Optional[TranscodeJob] = field(default=None, repr=False)


class LiveStreamManager:
    """
    Manages the lifecycle of live stream sessions.

    Each stream is identified by a stream_key (e.g. "my-show-key").
    When a broadcast starts, this manager launches an FFmpeg process
    that reads from the RTMP URL and writes HLS output.
    """

    def __init__(
        self,
        transcoder: FFmpegTranscoder,
        output_base_dir: str,
        renditions: List[Dict],
        segment_duration: int = 2,
        base_url: str = "http://localhost:8080",
    ):
        self.transcoder = transcoder
        self.output_base_dir = output_base_dir
        self.segment_duration = segment_duration
        self.base_url = base_url.rstrip("/")
        self._streams: Dict[str, LiveStream] = {}

        # Convert config dicts to Rendition objects
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

    # Public API

    def create_stream(self, stream_key: str, rtmp_base: str = "rtmp://localhost:1935/live") -> LiveStream:
        """
        Register a new live stream slot.
        Returns the LiveStream object (not yet started).
        """
        safe_key = sanitize_stream_key(stream_key)
        stream_id = generate_stream_id("live")
        output_dir = str(Path(self.output_base_dir) / safe_key)
        ensure_dir(output_dir)

        stream = LiveStream(
            stream_id=stream_id,
            stream_key=safe_key,
            rtmp_url=f"{rtmp_base}/{safe_key}",
            output_dir=output_dir,
            renditions=self.renditions,
            hls_master_url=f"{self.base_url}/streams/live/{safe_key}/master.m3u8",
        )
        self._streams[stream_id] = stream
        logger.info(f"Stream registered: id={stream_id}, key={safe_key}")
        return stream

    async def start_stream(self, stream_id: str) -> LiveStream:
        """
        Launch the FFmpeg ingest process for a registered stream.
        """
        stream = self._get_or_raise(stream_id)
        if stream.status == "live":
            logger.warning(f"Stream {stream_id} is already live.")
            return stream

        # Build the FFmpeg command for live HLS output
        cmd = self._build_live_ffmpeg_cmd(stream)

        job = TranscodeJob(
            job_id=stream_id,
            input_path=stream.rtmp_url,
            output_dir=stream.output_dir,
            renditions=stream.renditions,
            segment_duration=self.segment_duration,
        )
        stream._job = job
        stream.status = "live"
        stream.started_at = datetime.utcnow()

        # Write master playlist upfront
        self.transcoder.write_master_playlist(stream.output_dir, stream.renditions)

        # Launch FFmpeg in the background (runs until stream ends)
        asyncio.create_task(
            self._run_ffmpeg(stream, job, cmd),
            name=f"live-{stream_id}",
        )

        logger.info(f"Stream {stream_id} started. HLS: {stream.hls_master_url}")
        return stream

    async def stop_stream(self, stream_id: str) -> LiveStream:
        """Gracefully terminate the FFmpeg process for a live stream."""
        stream = self._get_or_raise(stream_id)
        if stream._job:
            await self.transcoder.cancel_job(stream._job)
        stream.status = "ended"
        stream.ended_at = datetime.utcnow()
        logger.info(f"Stream {stream_id} stopped.")
        return stream

    def get_stream(self, stream_id: str) -> Optional[LiveStream]:
        return self._streams.get(stream_id)

    def list_streams(self) -> List[LiveStream]:
        return list(self._streams.values())

    def get_active_streams(self) -> List[LiveStream]:
        return [s for s in self._streams.values() if s.status == "live"]

    # Internal helpers

    def _build_live_ffmpeg_cmd(self, stream: LiveStream) -> List[str]:
        """
        Build the FFmpeg command for RTMP to HLS conversion.

        Uses 'event' playlist type during the broadcast, then switches
        to 'vod' on finish so the recording remains seekable.
        """
        cmd = [
            self.transcoder.ffmpeg_bin,
            "-y",
            "-fflags", "+genpts+nobuffer",
            "-rtmp_buffer", "100",
            "-i", stream.rtmp_url,
        ]

        if self.transcoder.threads > 0:
            cmd += ["-threads", str(self.transcoder.threads)]

        # Filter complex for scaling
        filter_parts = []
        for i, r in enumerate(stream.renditions):
            filter_parts.append(
                f"[v:0]scale=w={r.width}:h={r.height}:"
                f"force_original_aspect_ratio=decrease,"
                f"pad={r.width}:{r.height}:(ow-iw)/2:(oh-ih)/2[v{i}]"
            )
        cmd += ["-filter_complex", ";".join(filter_parts)]

        # Per-rendition HLS output
        for i, r in enumerate(stream.renditions):
            out_dir = Path(stream.output_dir) / r.name
            out_dir.mkdir(parents=True, exist_ok=True)
            cmd += [
                "-map", f"[v{i}]",
                "-map", "a:0",
                "-c:v", "libx264",
                "-preset", "ultrafast",   # low-latency preset for live
                "-tune", "zerolatency",
                "-b:v", r.video_bitrate,
                "-maxrate", r.video_bitrate,
                "-bufsize", str(int(r.video_bitrate.replace("k", "")) * 2) + "k",
                "-c:a", "aac",
                "-b:a", r.audio_bitrate,
                "-ac", "2",
                "-ar", "44100",
                "-r", str(r.fps),
                "-g", str(r.fps * 2),
                "-sc_threshold", "0",
                "-f", "hls",
                "-hls_time", str(self.segment_duration),
                "-hls_list_size", "10",        # keep last N segments in playlist
                "-hls_flags", "delete_segments+append_list",
                "-hls_segment_type", "mpegts",
                "-hls_segment_filename", str(out_dir / "seg%05d.ts"),
                str(out_dir / "stream.m3u8"),
            ]

        return cmd

    async def _run_ffmpeg(
        self, stream: LiveStream, job: TranscodeJob, cmd: List[str]
    ) -> None:
        """Background coroutine that runs FFmpeg and updates stream state."""
        try:
            rc, stderr = await self.transcoder.run_async(cmd, job=job)
            if rc != 0 and stream.status != "ended":
                stream.status = "error"
                logger.error(f"Live stream {stream.stream_id} FFmpeg exited with code {rc}")
        except Exception as exc:
            stream.status = "error"
            logger.exception(f"Unexpected error in live stream {stream.stream_id}: {exc}")
        finally:
            if stream.status == "live":
                stream.status = "ended"
                stream.ended_at = datetime.utcnow()

    def _get_or_raise(self, stream_id: str) -> LiveStream:
        stream = self._streams.get(stream_id)
        if not stream:
            raise KeyError(f"Stream '{stream_id}' not found.")
        return stream
