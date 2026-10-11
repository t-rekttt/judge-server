"""
Uploads problem directories to the S3-compatible bucket remote grading downloads from.

Each `<problems dir>/<problem>/` (one containing init.yml) is stored file by file under `<prefix>/<problem>/`, the
layout described in `dmoj/remote/storage.py`. Creates the bucket if needed. Uses only the judge's own presigning code,
so it runs inside the judge image.

Usage: python upload_problems.py <problems dir> <endpoint> <bucket> <access key> <secret key> [prefix]
       python upload_problems.py <problems dir> <endpoint> <bucket> @<key file> - [prefix]
       (the key file holds the access key and the secret on two lines)

"""

import os
import sys
import urllib.error
import urllib.request

from dmoj.remote.storage import object_key, presign


def put(url: str, data: bytes = b'') -> int:
    request = urllib.request.Request(url, data=data, method='PUT')
    try:
        with urllib.request.urlopen(request) as response:
            return response.status
    except urllib.error.HTTPError as error:
        return error.code


def head(url: str) -> int:
    try:
        with urllib.request.urlopen(urllib.request.Request(url, method='HEAD')) as response:
            return response.status
    except urllib.error.HTTPError as error:
        return error.code


def main() -> None:
    problems_dir, endpoint, bucket, access_key, secret_key = sys.argv[1:6]
    prefix = sys.argv[6] if len(sys.argv) > 6 else ''
    if access_key.startswith('@'):
        # `@<file>`: read the key pair from a file of two lines, keeping the secret off the command line.
        with open(access_key[1:]) as f:
            access_key, secret_key = f.read().split()[:2]

    def signed_put(key: str) -> str:
        return presign('PUT', endpoint, bucket, key, access_key, secret_key, 'us-east-1', 300)

    # Create the bucket only if it doesn't exist (a HEAD on it answers 404), so an existing bucket is never written to
    # beyond the objects below. Creating it is a PUT on the bucket itself.
    if head(presign('HEAD', endpoint, '', bucket, access_key, secret_key, 'us-east-1', 300)) == 404:
        status = put(presign('PUT', endpoint, '', bucket, access_key, secret_key, 'us-east-1', 300))
        if status not in (200, 409):
            raise SystemExit(f'creating bucket {bucket} failed with HTTP {status}')

    count = 0
    for problem in sorted(os.listdir(problems_dir)):
        path = os.path.join(problems_dir, problem)
        if not os.path.isfile(os.path.join(path, 'init.yml')):
            continue
        for root, _, files in os.walk(path):
            for filename in files:
                full = os.path.join(root, filename)
                name = os.path.relpath(full, path)
                with open(full, 'rb') as f:
                    status = put(signed_put(object_key(prefix, problem, None, name)), f.read())
                if status != 200:
                    raise SystemExit(f'uploading {problem}/{name} failed with HTTP {status}')
        count += 1
    print(f'Uploaded {count} problems to {endpoint}/{bucket}')


if __name__ == '__main__':
    main()
