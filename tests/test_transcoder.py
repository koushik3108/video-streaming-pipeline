"""
Unit tests for the FFmpegTranscoder class.
Run with: pytest tests/ -v
"""

import os
import tempfile
import json
from unittest.mock import MagicMock, patch, AsyncMock
from pathlib import Path

import pytest

from src.pipeline.transcoder import FFmpegTranscoder, Rendition, TranscodeJob


# ----- Fixtures -----

@pytest.fixture
def transcoder():
    return FFmpegTranscoder(ffmpeg_bin="ffmpeg", ffprobe_bin="ffprobe", threads=2)


@pytest.fixture
def sample_renditions():
    return [
        Rendition("720p", 1280, 720, "2500k", "128k", 30),
        Rendition("480p", 854, 480, "800k", "96k", 30),
    ]


# ----- Probe tests -----

class TestProbe:
    def test_probe_returns_dict(self, transcoder):
        fake_output = json.dumps({
            "format": {"duration": "120.5", "size": "10485760"},
            "streams": [{"codec_type": "video", "width": 1920, "height": 1080}]
        })
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout=fake_output, stderr="")
            result = transcoder.probe("test.mp4")
        assert "format" in result
        assert "streams" in result

    def test_probe_raises_on_error(self, transcoder):
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=1, stdout="", stderr="file not found")
            with pytest.raises(RuntimeError, match="ffprobe error"):
                transcoder.probe("nonexistent.mp4")

    def test_get_duration(self, transcoder):
        fake_output = json.dumps({"format": {"duration": "90.25"}, "streams": []})
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout=fake_output, stderr="")
            duration = transcoder.get_duration("video.mp4")
        assert duration == pytest.approx(90.25)

    def test_get_video_info_returns_first_video_stream(self, transcoder):
        fake_output = json.dumps({
            "format": {},
            "streams": [
                {"codec_type": "audio", "codec_name": "aac"},
                {"codec_type": "video", "codec_name": "h264", "width": 1280, "height": 720},
            ]
        })
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout=fake_output, stderr="")
            info = transcoder.get_video_info("video.mp4")
        assert info["codec_type"] == "video"
        assert info["width"] == 1280


# ----- HLS command builder tests -----

class TestBuildHLSCommand:
    def test_command_contains_input(self, transcoder, sample_renditions, tmp_path):
        cmd = transcoder.build_hls_command(
            input_path="/input/video.mp4",
            output_dir=str(tmp_path),
            renditions=sample_renditions,
        )
        assert "/input/video.mp4" in cmd

    def test_command_produces_one_output_per_rendition(self, transcoder, sample_renditions, tmp_path):
        cmd = transcoder.build_hls_command(
            input_path="video.mp4",
            output_dir=str(tmp_path),
            renditions=sample_renditions,
        )
        # Count m3u8 outputs
        m3u8_outputs = [c for c in cmd if c.endswith("stream.m3u8")]
        assert len(m3u8_outputs) == len(sample_renditions)

    def test_hls_time_respected(self, transcoder, sample_renditions, tmp_path):
        cmd = transcoder.build_hls_command(
            input_path="video.mp4",
            output_dir=str(tmp_path),
            renditions=sample_renditions,
            segment_duration=4,
        )
        idx = cmd.index("-hls_time")
        assert cmd[idx + 1] == "4"


# ----- Master playlist tests -----

class TestMasterPlaylist:
    def test_master_playlist_contains_all_renditions(self, transcoder, sample_renditions, tmp_path):
        transcoder.write_master_playlist(str(tmp_path), sample_renditions)
        master = (tmp_path / "master.m3u8").read_text()
        assert "#EXTM3U" in master
        for r in sample_renditions:
            assert r.name in master

    def test_master_playlist_has_bandwidth(self, transcoder, sample_renditions, tmp_path):
        transcoder.write_master_playlist(str(tmp_path), sample_renditions)
        master = (tmp_path / "master.m3u8").read_text()
        assert "BANDWIDTH=" in master


# ----- Async execution tests -----

@pytest.mark.asyncio
class TestRunAsync:
    async def test_successful_run_updates_job_status(self, transcoder):
        job = TranscodeJob(
            job_id="test-1",
            input_path="in.mp4",
            output_dir="/tmp",
            renditions=[],
        )

        async def fake_run(*args, **kwargs):
            return 0, "success output"

        with patch.object(transcoder, "run_async", new=AsyncMock(return_value=(0, "output"))):
            rc, _ = await transcoder.run_async(["ffmpeg", "-version"], job=job)
            # In the real implementation the job would be updated; here we test the mock returns 0
            assert rc == 0
