"""
Cloudflare R2 storage client for uploading export files.
Uses boto3 (S3-compatible API) to interact with R2.
"""

import os
import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError

from config import (
    R2_ACCESS_KEY_ID,
    R2_SECRET_ACCESS_KEY,
    R2_ENDPOINT_URL,
    R2_BUCKET_NAME,
    R2_EXPORT_PREFIX,
)
from utils.logger import get_logger

logger = get_logger(__name__)


class R2Storage:
    """Cloudflare R2 client for file uploads and management."""

    def __init__(self):
        self._client = None

    @property
    def client(self):
        """Lazy-initialize the S3 client for R2."""
        if self._client is None:
            if not R2_ACCESS_KEY_ID or not R2_SECRET_ACCESS_KEY:
                logger.error("R2 credentials not configured. Set R2_ACCESS_KEY_ID and R2_SECRET_ACCESS_KEY in .env")
                return None

            self._client = boto3.client(
                "s3",
                endpoint_url=R2_ENDPOINT_URL,
                aws_access_key_id=R2_ACCESS_KEY_ID,
                aws_secret_access_key=R2_SECRET_ACCESS_KEY,
                config=BotoConfig(
                    signature_version="s3v4",
                    retries={"max_attempts": 3, "mode": "adaptive"},
                ),
                region_name="auto",
            )
        return self._client

    def upload_file(
        self,
        local_path: str,
        r2_key: str | None = None,
        content_type: str = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    ) -> str | None:
        """
        Upload a file to R2.

        Args:
            local_path: Absolute path to the local file
            r2_key: Object key in R2 (default: auto-generated from prefix + filename)
            content_type: MIME type for the file

        Returns:
            The R2 object URL on success, None on failure
        """
        if not self.client:
            return None

        if not os.path.exists(local_path):
            logger.error("File not found: %s", local_path)
            return None

        # Build the R2 key (path inside bucket)
        if not r2_key:
            filename = os.path.basename(local_path)
            r2_key = f"{R2_EXPORT_PREFIX}/{filename}"

        file_size = os.path.getsize(local_path)
        size_mb = file_size / (1024 * 1024)

        try:
            logger.info(
                "Uploading %s (%.1f MB) → r2://%s/%s",
                os.path.basename(local_path),
                size_mb,
                R2_BUCKET_NAME,
                r2_key,
            )

            self.client.upload_file(
                Filename=local_path,
                Bucket=R2_BUCKET_NAME,
                Key=r2_key,
                ExtraArgs={"ContentType": content_type},
            )

            url = f"{R2_ENDPOINT_URL}/{R2_BUCKET_NAME}/{r2_key}"
            logger.info("Upload complete: %s", r2_key)
            return url

        except ClientError as e:
            logger.error("R2 upload failed: %s", e)
            return None

    def list_exports(self, prefix: str | None = None) -> list[dict]:
        """
        List export files in R2.

        Returns:
            List of dicts with 'key', 'size', 'last_modified'
        """
        if not self.client:
            return []

        prefix = prefix or R2_EXPORT_PREFIX

        try:
            response = self.client.list_objects_v2(
                Bucket=R2_BUCKET_NAME,
                Prefix=prefix,
            )

            files = []
            for obj in response.get("Contents", []):
                files.append({
                    "key": obj["Key"],
                    "size_mb": round(obj["Size"] / (1024 * 1024), 2),
                    "last_modified": obj["LastModified"].isoformat(),
                })

            return files

        except ClientError as e:
            logger.error("R2 list failed: %s", e)
            return []

    def get_presigned_url(self, r2_key: str, expires_in: int = 3600) -> str | None:
        """
        Generate a presigned URL for downloading a file.

        Args:
            r2_key: Object key in R2
            expires_in: URL expiration in seconds (default: 1 hour)

        Returns:
            Presigned URL string or None
        """
        if not self.client:
            return None

        try:
            url = self.client.generate_presigned_url(
                "get_object",
                Params={"Bucket": R2_BUCKET_NAME, "Key": r2_key},
                ExpiresIn=expires_in,
            )
            return url

        except ClientError as e:
            logger.error("Failed to generate presigned URL: %s", e)
            return None

    def delete_file(self, r2_key: str) -> bool:
        """Delete a file from R2."""
        if not self.client:
            return False

        try:
            self.client.delete_object(Bucket=R2_BUCKET_NAME, Key=r2_key)
            logger.info("Deleted from R2: %s", r2_key)
            return True
        except ClientError as e:
            logger.error("R2 delete failed: %s", e)
            return False


# Module-level singleton
r2_storage = R2Storage()
