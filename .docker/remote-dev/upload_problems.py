"""
Uploads problem directories to the S3-compatible bucket remote grading downloads from.

Each `<problems dir>/<problem>/` (one containing init.yml) is stored in the layout described in
`dmoj/remote/storage.py`: the directory without its test archive as `files.zip`, and the archive named by `archive:`
as is, as `archive.zip`. Creates the bucket if needed. Uses only the judge's own presigning code, so it runs inside
the judge image.

Usage: python upload_problems.py <problems dir> <endpoint> <bucket> <access key> <secret key> [prefix]
"""

import io
import os
import sys
import urllib.error
import urllib.request
import zipfile
from typing import Optional

import yaml

from dmoj.remote.storage import ARCHIVE_OBJECT, FILES_OBJECT, object_key, presign


def zip_directory(path: str, exclude: Optional[str]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, 'w', zipfile.ZIP_DEFLATED) as archive:
        for root, _, files in os.walk(path):
            for name in files:
                full = os.path.join(root, name)
                relative = os.path.relpath(full, path)
                if relative != exclude:
                    archive.write(full, relative)
    return buffer.getvalue()


def put(url: str, data: bytes = b'') -> int:
    request = urllib.request.Request(url, data=data, method='PUT')
    try:
        with urllib.request.urlopen(request) as response:
            return response.status
    except urllib.error.HTTPError as error:
        return error.code


def main() -> None:
    problems_dir, endpoint, bucket, access_key, secret_key = sys.argv[1:6]
    prefix = sys.argv[6] if len(sys.argv) > 6 else ''

    def signed_put(key: str) -> str:
        return presign('PUT', endpoint, bucket, key, access_key, secret_key, 'us-east-1', 300)

    # Creating the bucket is a PUT on the bucket itself; 409 means it already exists.
    status = put(presign('PUT', endpoint, '', bucket, access_key, secret_key, 'us-east-1', 300))
    if status not in (200, 409):
        raise SystemExit(f'creating bucket {bucket} failed with HTTP {status}')

    count = 0
    for problem in sorted(os.listdir(problems_dir)):
        path = os.path.join(problems_dir, problem)
        if not os.path.isfile(os.path.join(path, 'init.yml')):
            continue
        with open(os.path.join(path, 'init.yml')) as f:
            archive_name = (yaml.safe_load(f) or {}).get('archive')
        uploads = {FILES_OBJECT: zip_directory(path, archive_name)}
        if archive_name and os.path.isfile(os.path.join(path, archive_name)):
            with open(os.path.join(path, archive_name), 'rb') as f:
                uploads[ARCHIVE_OBJECT] = f.read()
        for name, data in uploads.items():
            status = put(signed_put(object_key(prefix, problem, None, name)), data)
            if status != 200:
                raise SystemExit(f'uploading {problem}/{name} failed with HTTP {status}')
        count += 1
    print(f'Uploaded {count} problems to {endpoint}/{bucket}')


if __name__ == '__main__':
    main()
