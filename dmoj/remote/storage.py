"""
Where VMs get problem data: an S3-compatible bucket (AWS S3, DigitalOcean Spaces, RustFS...).

Each problem is stored as two objects under `<prefix>/<namespace or "default">/<problem id>/`:

- `files.zip`: the problem directory (init.yml, checkers, generators...) without its test archive. VMs download and
  unpack it whole, so it should stay small.
- `archive.zip`: the test archive named by `archive:` in init.yml, stored as is. VMs never download it whole: they read
  its central directory and then only the members of the cases they grade, with HTTP range requests.

VMs get short-lived presigned GET URLs for exactly these objects, so they never hold storage credentials.

Presigning is AWS Signature Version 4 query signing, implemented here to avoid adding an SDK to the judge image.
"""

import datetime
import hashlib
import hmac
import urllib.parse
from typing import Any, Dict, Optional

FILES_OBJECT = 'files.zip'
ARCHIVE_OBJECT = 'archive.zip'


def object_key(prefix: str, problem_id: str, namespace: Optional[str], name: str) -> str:
    parts = [part for part in (prefix.strip('/'), namespace or 'default', problem_id, name) if part]
    return '/'.join(parts)


def problem_urls(config: Any, problem_id: str, namespace: Optional[str]) -> Dict[str, str]:
    """Fresh presigned URLs for both objects. The archive URL is used only if init.yml names an archive."""
    return {
        name: presign_get(
            endpoint=config.endpoint,
            bucket=config.bucket,
            key=object_key(config.prefix or '', problem_id, namespace, name),
            access_key=config.access_key,
            secret_key=config.secret_key,
            region=config.region or 'us-east-1',
            expires=int(config.url_expiry or 900),
        )
        for name in (FILES_OBJECT, ARCHIVE_OBJECT)
    }


def _sign(key: bytes, message: str) -> bytes:
    return hmac.new(key, message.encode(), hashlib.sha256).digest()


def presign_get(*args, **kwargs) -> str:
    return presign('GET', *args, **kwargs)


def presign(
    method: str,
    endpoint: str,
    bucket: str,
    key: str,
    access_key: str,
    secret_key: str,
    region: str,
    expires: int,
    now: Optional[datetime.datetime] = None,
) -> str:
    """Presigned path-style URL for `method` on `<endpoint>/<bucket>/<key>`, valid for `expires` seconds."""
    now = now or datetime.datetime.now(datetime.timezone.utc)
    amz_date = now.strftime('%Y%m%dT%H%M%SZ')
    date = now.strftime('%Y%m%d')
    scope = f'{date}/{region}/s3/aws4_request'

    parsed = urllib.parse.urlsplit(endpoint)
    host = parsed.netloc
    # An empty bucket means the endpoint already addresses the bucket (virtual-hosted style).
    path = '/' + '/'.join(urllib.parse.quote(part, safe='') for part in [bucket, *key.split('/')] if part)
    base_path = parsed.path.rstrip('/')

    query = {
        'X-Amz-Algorithm': 'AWS4-HMAC-SHA256',
        'X-Amz-Credential': f'{access_key}/{scope}',
        'X-Amz-Date': amz_date,
        'X-Amz-Expires': str(expires),
        'X-Amz-SignedHeaders': 'host',
    }
    canonical_query = '&'.join(
        f'{urllib.parse.quote(k, safe="")}={urllib.parse.quote(v, safe="")}' for k, v in sorted(query.items())
    )
    canonical_request = '\n'.join(
        [method, base_path + path, canonical_query, f'host:{host}\n', 'host', 'UNSIGNED-PAYLOAD']
    )
    string_to_sign = '\n'.join(
        ['AWS4-HMAC-SHA256', amz_date, scope, hashlib.sha256(canonical_request.encode()).hexdigest()]
    )

    signing_key = _sign(_sign(_sign(_sign(f'AWS4{secret_key}'.encode(), date), region), 's3'), 'aws4_request')
    signature = hmac.new(signing_key, string_to_sign.encode(), hashlib.sha256).hexdigest()
    return f'{parsed.scheme}://{host}{base_path}{path}?{canonical_query}&X-Amz-Signature={signature}'
