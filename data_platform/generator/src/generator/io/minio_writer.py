"""Write immutable source files to the MinIO landing bucket.

Reads: MinIO endpoint and credentials from environment variables.
Writes: checksum-protected Parquet objects to the configured landing bucket.
Runs: batch generator commands inside or outside Docker Compose.
"""

from __future__ import annotations

import hashlib
from io import BytesIO
from urllib.parse import urlparse

from minio import Minio
from minio.error import S3Error


class MinioObjectWriter:
    """Store immutable objects and reject conflicting reruns."""

    def __init__(self, endpoint: str, access_key: str, secret_key: str, bucket: str) -> None:
        """Create a MinIO object writer.

        Args:
            endpoint: HTTP or HTTPS MinIO API endpoint.
            access_key: MinIO access key.
            secret_key: MinIO secret key.
            bucket: Existing landing bucket name.
        """
        parsed = urlparse(endpoint)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("MINIO_ENDPOINT must be a complete HTTP or HTTPS URL")
        self._client = Minio(
            parsed.netloc,
            access_key=access_key,
            secret_key=secret_key,
            secure=parsed.scheme == "https",
        )
        self._bucket = bucket

    @property
    def bucket(self) -> str:
        """Return the configured landing bucket name."""
        return self._bucket

    def write(self, object_name: str, data: bytes, content_type: str) -> bool:
        """Upload an object once, allowing checksum-identical reruns.

        Args:
            object_name: Object key relative to the landing bucket.
            data: Complete object bytes.
            content_type: MIME type stored with the object.

        Returns:
            ``True`` when bytes were uploaded and ``False`` for an identical object.

        Raises:
            RuntimeError: When an existing object has a different checksum.
        """
        checksum = hashlib.sha256(data).hexdigest()
        try:
            stat = self._client.stat_object(self._bucket, object_name)
        except S3Error as exc:
            if exc.code not in {"NoSuchKey", "NoSuchObject", "NoSuchBucket"}:
                raise
        else:
            stored_checksum = (
                stat.metadata.get("x-amz-meta-sha256") if stat.metadata is not None else None
            )
            if stored_checksum == checksum:
                return False
            raise RuntimeError(f"Refusing to overwrite object with different bytes: {object_name}")

        self._client.put_object(
            self._bucket,
            object_name,
            BytesIO(data),
            length=len(data),
            content_type=content_type,
            metadata={"sha256": checksum},
        )
        return True
