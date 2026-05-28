"""
Azure Data Lake Storage Gen2 / Azure Blob Storage connector.
Local dev: uses MinIO as a drop-in replacement.
Production: uses azure-storage-blob SDK with DefaultAzureCredential.
"""
import io
import os
from pathlib import Path
from typing import BinaryIO, Generator, Optional

from azure.identity import DefaultAzureCredential, ManagedIdentityCredential
from azure.storage.blob import (
    BlobClient,
    BlobServiceClient,
    ContainerClient,
    ContentSettings,
)
from loguru import logger
from minio import Minio
from minio.error import S3Error

from src.config import get_settings

settings = get_settings()


class DataLakeConnector:
    """
    Abstracted connector for Azure Data Lake Storage Gen2.
    Falls back to MinIO in non-production environments.

    Zones follow the Medallion Architecture:
        raw/       → Landing zone – raw files as-is
        processed/ → Bronze → Silver transformations
        curated/   → Gold layer – analytics-ready datasets
    """

    ZONES = {
        "raw": settings.storage.adls_container_raw,
        "processed": settings.storage.adls_container_processed,
        "curated": settings.storage.adls_container_curated,
    }

    def __init__(self):
        self._azure_client: Optional[BlobServiceClient] = None
        self._minio_client: Optional[Minio] = None
        self._use_azure = settings.is_production
        self._init_client()

    def _init_client(self) -> None:
        if self._use_azure:
            logger.info("Initializing Azure Data Lake Storage Gen2 client")
            # DefaultAzureCredential: tries EnvVar → Managed Identity → CLI
            credential = DefaultAzureCredential()
            account_url = (
                f"https://{settings.storage.azure_storage_account_name}"
                ".blob.core.windows.net"
            )
            self._azure_client = BlobServiceClient(
                account_url=account_url, credential=credential
            )
            self._ensure_containers_exist()
        else:
            logger.info("Initializing MinIO client (local Azure Blob equivalent)")
            self._minio_client = Minio(
                endpoint=settings.storage.minio_endpoint,
                access_key=settings.storage.minio_access_key,
                secret_key=settings.storage.minio_secret_key,
                secure=settings.storage.minio_secure,
            )
            self._ensure_buckets_exist()

    # ─── Upload ──────────────────────────────────────────────────────────────

    def upload_file(
        self,
        local_path: str | Path,
        blob_name: str,
        zone: str = "raw",
        content_type: str = "application/octet-stream",
        metadata: Optional[dict] = None,
    ) -> str:
        """Upload a local file to the data lake. Returns the blob URI."""
        container = self.ZONES[zone]
        local_path = Path(local_path)

        with open(local_path, "rb") as data:
            return self.upload_bytes(
                data.read(), blob_name, zone, content_type, metadata
            )

    def upload_bytes(
        self,
        data: bytes,
        blob_name: str,
        zone: str = "raw",
        content_type: str = "application/octet-stream",
        metadata: Optional[dict] = None,
    ) -> str:
        """Upload raw bytes to the data lake. Returns blob URI."""
        container = self.ZONES[zone]

        if self._use_azure:
            blob_client: BlobClient = self._azure_client.get_blob_client(
                container=container, blob=blob_name
            )
            blob_client.upload_blob(
                data,
                overwrite=True,
                content_settings=ContentSettings(content_type=content_type),
                metadata=metadata or {},
            )
            uri = blob_client.url
        else:
            self._minio_client.put_object(
                bucket_name=container,
                object_name=blob_name,
                data=io.BytesIO(data),
                length=len(data),
                content_type=content_type,
                metadata=metadata or {},
            )
            uri = f"s3://{container}/{blob_name}"

        logger.info(f"Uploaded {blob_name} → {uri}")
        return uri

    # ─── Download ────────────────────────────────────────────────────────────

    def download_bytes(self, blob_name: str, zone: str = "raw") -> bytes:
        """Download blob content as bytes."""
        container = self.ZONES[zone]

        if self._use_azure:
            blob_client = self._azure_client.get_blob_client(
                container=container, blob=blob_name
            )
            return blob_client.download_blob().readall()
        else:
            response = self._minio_client.get_object(container, blob_name)
            return response.read()

    def download_to_file(
        self, blob_name: str, local_path: str | Path, zone: str = "raw"
    ) -> Path:
        """Download blob to a local file."""
        local_path = Path(local_path)
        local_path.parent.mkdir(parents=True, exist_ok=True)
        data = self.download_bytes(blob_name, zone)
        local_path.write_bytes(data)
        logger.info(f"Downloaded {blob_name} → {local_path}")
        return local_path

    # ─── List ────────────────────────────────────────────────────────────────

    def list_blobs(
        self, prefix: str = "", zone: str = "raw"
    ) -> Generator[dict, None, None]:
        """Yield blob metadata dicts for all objects in a zone."""
        container = self.ZONES[zone]

        if self._use_azure:
            container_client: ContainerClient = (
                self._azure_client.get_container_client(container)
            )
            for blob in container_client.list_blobs(name_starts_with=prefix):
                yield {
                    "name": blob.name,
                    "size": blob.size,
                    "last_modified": blob.last_modified,
                    "content_type": blob.content_settings.content_type,
                }
        else:
            objects = self._minio_client.list_objects(
                container, prefix=prefix, recursive=True
            )
            for obj in objects:
                yield {
                    "name": obj.object_name,
                    "size": obj.size,
                    "last_modified": obj.last_modified,
                    "content_type": None,
                }

    def blob_exists(self, blob_name: str, zone: str = "raw") -> bool:
        container = self.ZONES[zone]
        try:
            if self._use_azure:
                blob_client = self._azure_client.get_blob_client(
                    container=container, blob=blob_name
                )
                blob_client.get_blob_properties()
                return True
            else:
                self._minio_client.stat_object(container, blob_name)
                return True
        except Exception:
            return False

    # ─── Helpers ────────────────────────────────────────────────────────────

    def _ensure_containers_exist(self) -> None:
        existing = {c.name for c in self._azure_client.list_containers()}
        for zone, container in self.ZONES.items():
            if container not in existing:
                self._azure_client.create_container(container)
                logger.info(f"Created Azure container: {container}")

    def _ensure_buckets_exist(self) -> None:
        for zone, bucket in self.ZONES.items():
            if not self._minio_client.bucket_exists(bucket):
                self._minio_client.make_bucket(bucket)
                logger.info(f"Created MinIO bucket: {bucket}")


# Singleton
_data_lake: Optional[DataLakeConnector] = None


def get_data_lake() -> DataLakeConnector:
    global _data_lake
    if _data_lake is None:
        _data_lake = DataLakeConnector()
    return _data_lake
