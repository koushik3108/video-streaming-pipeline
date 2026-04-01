"""
Shared utility helpers for the video streaming pipeline.
"""

import hashlib
import time
import uuid
from pathlib import Path
from typing import Optional


def generate_stream_id(prefix: str = "stream") -> str:
    """Generate a unique stream/job identifier."""
    unique = f"{prefix}-{uuid.uuid4().hex[:12]}"
    return unique


def generate_file_hash(file_path: str, algorithm: str = "sha256") -> str:
    """Compute a hash of a file for integrity checking."""
    h = hashlib.new(algorithm)
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def sanitize_stream_key(key: str) -> str:
    """
    Sanitize a stream key so it's safe to use as a directory name.
    Allows only alphanumerics, dashes, and underscores.
    """
    import re
    return re.sub(r"[^a-zA-Z0-9_\-]", "_", key)


def human_readable_size(size_bytes: int) -> str:
    """Convert bytes to a human-readable string."""
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size_bytes < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes //= 1024
    return f"{size_bytes:.1f} PB"


def format_duration(seconds: float) -> str:
    """Format seconds as HH:MM:SS string."""
    total = int(seconds)
    h = total // 3600
    m = (total % 3600) // 60
    s = total % 60
    return f"{h:02d}:{m:02d}:{s:02d}"


def ensure_dir(path: str) -> Path:
    """Create a directory (and parents) if it doesn't exist, then return a Path object."""
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p


def get_file_extension(filename: str) -> str:
    """Return lowercased file extension including the dot."""
    return Path(filename).suffix.lower()


def build_hls_url(base_url: str, stream_id: str, filename: str = "master.m3u8") -> str:
    """Construct the public HLS playlist URL for a given stream."""
    base = base_url.rstrip("/")
    return f"{base}/streams/{stream_id}/{filename}"


def timestamp_ms() -> int:
    """Current time in milliseconds."""
    return int(time.time() * 1000)
