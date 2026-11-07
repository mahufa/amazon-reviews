import io
from unittest.mock import MagicMock

import pytest
from pytest import mark

from include.helpers import ingest

BUCKET = 'landing'
KEY = 'amazon_reviews/review_categories/All_Beauty.jsonl'
PART_SIZE = 4


@pytest.fixture(autouse=True)
def small_parts(monkeypatch):
    monkeypatch.setattr(ingest, 'PART_SIZE', PART_SIZE)


@pytest.fixture
def client() -> MagicMock:
    client = MagicMock()
    client.create_multipart_upload.return_value = {'UploadId': 'upload-1'}
    client.upload_part.side_effect = lambda PartNumber, **_: {'ETag': f'etag-{PartNumber}'}
    return client


@mark.parametrize(
    ('data', 'expected_parts'),
    [
        (b'abc', [b'abc']),
        (b'abcd', [b'abcd']),
        (b'abcdefghij', [b'abcd', b'efgh', b'ij']),
    ],
    ids=['smaller_than_part', 'exactly_one_part', 'several_parts_with_shorter_last'],
)
def test_multipart_upload(client, data, expected_parts):
    _upload(client, data)

    uploaded = [call.kwargs['Body'] for call in client.upload_part.call_args_list]
    assert uploaded == expected_parts
    assert [call.kwargs['PartNumber'] for call in client.upload_part.call_args_list] == list(
        range(1, len(expected_parts) + 1)
    )
    client.complete_multipart_upload.assert_called_once_with(
        Bucket=BUCKET,
        Key=KEY,
        UploadId='upload-1',
        MultipartUpload={
            'Parts': [
                {'PartNumber': number, 'ETag': f'etag-{number}'}
                for number in range(1, len(expected_parts) + 1)
            ]
        },
    )
    client.abort_multipart_upload.assert_not_called()


def test_empty_stream_aborts_and_fails(client):
    with pytest.raises(ValueError, match='No data'):
        _upload(client, b'')

    client.complete_multipart_upload.assert_not_called()
    client.abort_multipart_upload.assert_called_once_with(
        Bucket=BUCKET, Key=KEY, UploadId='upload-1'
    )


def test_failed_abort_keeps_original_error(client):
    client.upload_part.side_effect = ConnectionError('upload failed')
    client.abort_multipart_upload.side_effect = RuntimeError('abort failed')

    with pytest.raises(ConnectionError, match='upload failed'):
        _upload(client, b'abc')

    client.abort_multipart_upload.assert_called_once()


def test_get_and_store_file_returns_landing_uri(client, monkeypatch):
    response = MagicMock()
    response.__enter__.return_value.raw = io.BytesIO(b'abc')
    monkeypatch.setattr(ingest.requests, 'get', MagicMock(return_value=response))
    monkeypatch.setattr(ingest, '_get_s3_client_and_bucket', lambda: (client, BUCKET))

    uri = ingest.get_and_store_file('https://example.com/All_Beauty.jsonl', KEY)

    assert uri == f's3a://{BUCKET}/{KEY}'
    client.complete_multipart_upload.assert_called_once()


def _upload(
    client: MagicMock,
    data: bytes,
) -> None:
    ingest._store_stream_in_s3(io.BytesIO(data), KEY, (client, BUCKET))
