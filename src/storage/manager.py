"""
Storage Manager - abstracts local disk vs. Amazon S3.

Usage:
    manager = StorageManager(backend="local", local_base_dir=".")
    url = manager.get_url("streams/vod/abc/master.m3u8")
"""

import os
import shutil
from pathlib import Path
from typing import Optional

from src.utils import logger


class StorageManager:
    """
    Thin abstraction over storage backends.

    Backends:
      - "local": Serve files directly from disk via FastAPI StaticFiles.
      - "s3": Upload finished stream output to S3 and return public URLs.
    """

    def __init__(
        self,
        backend: str = "local",
        local_base_dir: str = ".",
        s3_bucket: Optional[str] = None,
        s3_region: str = "us-east-1",
        s3_prefix: str = "streams/",
        base_url: str = "http://localhost:8080",
    ):
        self.backend = backend.lower()
        self.local_base_dir = Path(local_base_dir)
        self.s3_bucket = s3_bucket
        self.s3_region = s3_region
        self.s3_prefix = s3_prefix.rstrip("/") + "/"
        self.base_url = base_url.rstrip("/")

        if self.backend == "s3":
            self._init_s3()

    # Public API

    def get_url(self, relative_path: str) -> str:
        """Return the public URL for a stream file (local or S3)."""
        if self.backend == "s3":
            key = self.s3_prefix + relative_path.lstrip("/")
            return f"https://{self.s3_bucket}.s3.{self.s3_region}.amazonaws.com/{key}"
        return f"{self.base_url}/{relative_path.lstrip('/')}"

    def local_path(self, relative_path: str) -> Path:
        """Resolve a relative path to its absolute local location."""
        return self.local_base_dir / relative_path

    def copy(self, src: str, dest_relative: str) -> str:
        """
        Copy a file into the storage location.
        Returns the public URL of the destination.
        """
        dest = self.local_base_dir / dest_relative
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dest)
        if self.backend == "s3":
            self._upload_to_s3(str(dest), dest_relative)
        return self.get_url(dest_relative)

    async def upload_directory_to_s3(self, local_dir: str, prefix: str) -> int:
        """
        Recursively upload all files in local_dir to S3.
        Returns the number of files uploaded.
        """
        if self.backend != "s3":
            logger.debug("upload_directory_to_s3 called but backend is not S3.")
            return 0

        count = 0
        for root, _, files in os.walk(local_dir):
            for fname in files:
                full_path = os.path.join(root, fname)
                rel = os.path.relpath(full_path, local_dir)
                key = f"{self.s3_prefix}{prefix}/{rel}".replace("\\", "/")
                self._upload_to_s3(full_path, key)
                count += 1

        logger.info(f"Uploaded {count} files to s3://{self.s3_bucket}/{self.s3_prefix}{prefix}/")
        return count

    def delete(self, relative_path: str) -> None:
        """Delete a file from local storage (and S3 if applicable)."""
        local = self.local_base_dir / relative_path
        if local.exists():
            local.unlink()
        if self.backend == "s3":
            self._delete_from_s3(relative_path)

    # Internal S3 helpers

    def _init_s3(self) -> None:
        try:
            import boto3
            self._s3 = boto3.client("s3", region_name=self.s3_region)
            logger.info(f"S3 client initialized for bucket '{self.s3_bucket}'")
        except ImportError:
            raise RuntimeError("boto3 is required for S3 storage. Run: pip install boto3")

    def _upload_to_s3(self, local_path: str, s3_key: str) -> None:
        content_type = self._guess_content_type(local_path)
        extra = {"ContentType": content_type}
        if local_path.endswith(".m3u8"):
            extra["CacheControl"] = "no-cache, no-store"  # playlists must not be cached
        self._s3.upload_file(local_path, self.s3_bucket, s3_key, ExtraArgs=extra)
        logger.debug(f"S3 upload: {local_path} to s3://{self.s3_bucket}/{s3_key}")

    def _delete_from_s3(self, relative_path: str) -> None:
        key = self.s3_prefix + relative_path.lstrip("/")
        self._s3.delete_object(Bucket=self.s3_bucket, Key=key)
        logger.debug(f"S3 delete: s3://{self.s3_bucket}/{key}")

    @staticmethod
    def _guess_content_type(path: str) -> str:
        ext = Path(path).suffix.lower()
        return {
            ".m3u8": "application/vnd.apple.mpegurl",
            ".ts": "video/mp2t",
            ".mp4": "video/mp4",
            ".jpg": "image/jpeg",
            ".png": "image/png",
        }.get(ext, "application/octet-stream")
