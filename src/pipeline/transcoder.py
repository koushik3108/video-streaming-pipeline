"""
Core FFmpeg transcoder - wraps subprocess calls to FFmpeg for all
encoding, probing, and HLS packaging tasks.
"""

import asyncio
import json
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from src.utils import logger


@dataclass
class Rendition:
    """Defines a single output quality level."""
    name: str             # e.g. "720p"
    width: int
    height: int
    video_bitrate: str    # e.g. "2500k"
    audio_bitrate: str    # e.g. "128k"
    fps: int = 30


@dataclass
class TranscodeJob:
    """Describes a transcoding task and its current state."""
    job_id: str
    input_path: str
    output_dir: str
    renditions: List[Rendition]
    segment_duration: int = 6
    status: str = "pending"          # pending | running | done | failed
    progress: float = 0.0            # 0.0 to 100.0
    error: Optional[str] = None
    process: Optional[asyncio.subprocess.Process] = field(default=None, repr=False)


class FFmpegTranscoder:
    """
    Wrapper around FFmpeg that handles:
    - Media probing (get video/audio metadata)
    - Multi-rendition HLS transcoding (ABR ladder)
    - Async subprocess management with progress tracking
    """

    def __init__(self, ffmpeg_bin: str = "ffmpeg", ffprobe_bin: str = "ffprobe", threads: int = 0):
        self.ffmpeg_bin = ffmpeg_bin
        self.ffprobe_bin = ffprobe_bin
        self.threads = threads  # 0 = auto

    # Probing

    def probe(self, input_path: str) -> Dict:
        """
        Run ffprobe on a file and return parsed JSON metadata.
        Returns a dict with 'format' and 'streams' keys.
        Raises RuntimeError on failure.
        """
        cmd = [
            self.ffprobe_bin,
            "-v", "quiet",
            "-print_format", "json",
            "-show_format",
            "-show_streams",
            input_path,
        ]
        logger.debug(f"Probing: {input_path}")
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            if result.returncode != 0:
                raise RuntimeError(f"ffprobe error: {result.stderr.strip()}")
            return json.loads(result.stdout)
        except subprocess.TimeoutExpired:
            raise RuntimeError("ffprobe timed out after 30 seconds")

    def get_duration(self, input_path: str) -> float:
        """Return video duration in seconds."""
        info = self.probe(input_path)
        return float(info.get("format", {}).get("duration", 0))

    def get_video_info(self, input_path: str) -> Optional[Dict]:
        """Return the first video stream dict, or None."""
        info = self.probe(input_path)
        for stream in info.get("streams", []):
            if stream.get("codec_type") == "video":
                return stream
        return None

    # HLS Packaging

    def build_hls_command(
        self,
        input_path: str,
        output_dir: str,
        renditions: List[Rendition],
        segment_duration: int = 6,
        is_live: bool = False,
    ) -> List[str]:
        """
        Build a multi-rendition FFmpeg command that produces an ABR HLS stream.

        Output layout:
          output_dir/
            <name>/stream.m3u8   (per-rendition playlist)
            <name>/seg%03d.ts    (TS segments)
            master.m3u8          (master playlist)
        """
        Path(output_dir).mkdir(parents=True, exist_ok=True)

        cmd = [
            self.ffmpeg_bin,
            "-y",                          # overwrite output
            "-i", input_path,
        ]

        # Thread setting
        if self.threads > 0:
            cmd += ["-threads", str(self.threads)]

        # Live stream flags
        if is_live:
            cmd += ["-re"]  # read input at native frame rate (simulate live)

        # Per-rendition video + audio encode
        filter_parts = []
        for i, r in enumerate(renditions):
            filter_parts.append(
                f"[v:0]scale=w={r.width}:h={r.height}:force_original_aspect_ratio=decrease,"
                f"pad={r.width}:{r.height}:(ow-iw)/2:(oh-ih)/2[v{i}]"
            )

        cmd += ["-filter_complex", ";".join(filter_parts)]

        # Map each rendition
        for i, r in enumerate(renditions):
            out_dir = Path(output_dir) / r.name
            out_dir.mkdir(parents=True, exist_ok=True)

            cmd += [
                "-map", f"[v{i}]",
                "-map", "a:0",
                "-c:v", "libx264",
                "-crf", "23",
                "-preset", "veryfast",
                "-b:v", r.video_bitrate,
                "-maxrate", r.video_bitrate,
                "-bufsize", str(int(r.video_bitrate.replace("k", "")) * 2) + "k",
                "-c:a", "aac",
                "-b:a", r.audio_bitrate,
                "-ac", "2",
                "-ar", "44100",
                "-r", str(r.fps),
                "-g", str(r.fps * 2),      # keyframe interval = 2x fps
                "-sc_threshold", "0",
                "-f", "hls",
                "-hls_time", str(segment_duration),
                "-hls_playlist_type", "event" if is_live else "vod",
                "-hls_segment_filename", str(out_dir / "seg%03d.ts"),
                str(out_dir / "stream.m3u8"),
            ]

        return cmd

    def write_master_playlist(self, output_dir: str, renditions: List[Rendition]) -> str:
        """Write a master HLS playlist that references all rendition sub-playlists."""
        lines = ["#EXTM3U", "#EXT-X-VERSION:3", ""]
        for r in renditions:
            bandwidth = int(r.video_bitrate.replace("k", "")) * 1000
            lines += [
                f"#EXT-X-STREAM-INF:BANDWIDTH={bandwidth},"
                f"RESOLUTION={r.width}x{r.height},"
                f'NAME="{r.name}"',
                f"{r.name}/stream.m3u8",
                "",
            ]
        master_path = Path(output_dir) / "master.m3u8"
        master_path.write_text("\n".join(lines))
        logger.info(f"Master playlist written: {master_path}")
        return str(master_path)

    # Async execution

    async def run_async(
        self,
        cmd: List[str],
        job: Optional[TranscodeJob] = None,
        duration_secs: float = 0,
    ) -> Tuple[int, str]:
        """
        Execute an FFmpeg command asynchronously, streaming stderr for
        progress updates on a TranscodeJob object.

        Returns (return_code, stderr_output).
        """
        logger.info(f"Running FFmpeg: {' '.join(cmd[:6])} ...")
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        if job:
            job.process = proc
            job.status = "running"

        stderr_lines = []

        async def read_stderr():
            while True:
                line = await proc.stderr.readline()
                if not line:
                    break
                text = line.decode("utf-8", errors="replace").strip()
                stderr_lines.append(text)

                # Parse progress from FFmpeg's "time=HH:MM:SS.xx" output
                if job and duration_secs > 0 and "time=" in text:
                    try:
                        t_str = text.split("time=")[1].split(" ")[0]
                        h, m, s = t_str.split(":")
                        elapsed = int(h) * 3600 + int(m) * 60 + float(s)
                        job.progress = min(100.0, (elapsed / duration_secs) * 100)
                    except Exception:
                        pass

        await asyncio.gather(read_stderr(), proc.wait())
        stderr_text = "\n".join(stderr_lines)

        if job:
            if proc.returncode == 0:
                job.status = "done"
                job.progress = 100.0
                logger.info(f"Job {job.job_id} completed successfully.")
            else:
                job.status = "failed"
                job.error = stderr_text[-500:]  # last 500 chars
                logger.error(f"Job {job.job_id} failed. FFmpeg exit code: {proc.returncode}")

        return proc.returncode, stderr_text

    async def cancel_job(self, job: TranscodeJob) -> None:
        """Terminate a running FFmpeg process."""
        if job.process and job.process.returncode is None:
            job.process.terminate()
            try:
                await asyncio.wait_for(job.process.wait(), timeout=5)
            except asyncio.TimeoutError:
                job.process.kill()
            job.status = "cancelled"
            logger.warning(f"Job {job.job_id} cancelled.")
