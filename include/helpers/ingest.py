import logging
from typing import IO, Any, Self

import requests
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import Variable

PART_SIZE = 8 * 1024 * 1024

log = logging.getLogger(__name__)


def get_and_store_file(
    file_path: str,
    s3_key: str,
) -> str:
    client, bucket = _get_s3_client_and_bucket()

    with requests.get(
        file_path,
        stream=True,
        timeout=(10.0, 60.0),
        # response.raw is not decoded, so ask for the file as-is to keep landing byte-exact
        headers={'Accept-Encoding': 'identity'},
    ) as response:
        response.raise_for_status()

        _store_stream_in_s3(response.raw, s3_key, (client, bucket))

    return f's3a://{bucket}/{s3_key}'


def _store_stream_in_s3(
    data_stream: IO[bytes],
    key: str,
    s3_config: tuple[Any, str],
) -> None:
    client, bucket_name = s3_config

    with _MultipartUpload(client, bucket_name, key) as multipart_upload:
        while part := data_stream.read(PART_SIZE):
            multipart_upload.upload_part(part)


class _MultipartUpload:
    def __init__(
        self,
        client: Any,
        bucket: str,
        key: str,
    ) -> None:
        self.client, self.bucket, self.key = client, bucket, key
        self.upload_id: str | None = None
        self.parts: list[dict] = []

    def __enter__(self) -> Self:
        resp = self.client.create_multipart_upload(Bucket=self.bucket, Key=self.key)
        self.upload_id = resp['UploadId']
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        if exc_type is not None:
            self._abort()
            return False

        try:
            if not self.parts:
                raise ValueError(f'No data for s3://{self.bucket}/{self.key}')
            self._complete()
        except Exception:
            self._abort()
            raise
        return False

    def upload_part(self, body: bytes) -> None:
        number = len(self.parts) + 1
        resp = self.client.upload_part(
            Bucket=self.bucket,
            Key=self.key,
            UploadId=self.upload_id,
            PartNumber=number,
            Body=body,
        )
        self.parts.append({'PartNumber': number, 'ETag': resp['ETag']})

    def _complete(self) -> None:
        self.client.complete_multipart_upload(
            Bucket=self.bucket,
            Key=self.key,
            UploadId=self.upload_id,
            MultipartUpload={'Parts': self.parts},
        )

    def _abort(self) -> None:
        try:
            self.client.abort_multipart_upload(
                Bucket=self.bucket,
                Key=self.key,
                UploadId=self.upload_id,
            )
        except Exception:
            log.exception(
                'Failed to abort multipart upload %s for s3://%s/%s',
                self.upload_id,
                self.bucket,
                self.key,
            )


def _get_s3_client_and_bucket() -> tuple[Any, str]:
    return (
        S3Hook(aws_conn_id='lake_s3').get_conn(),
        Variable.get('bucket_landing'),
    )
