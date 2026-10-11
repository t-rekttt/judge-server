"""
Where VMs get problem data: an S3-compatible bucket (AWS S3, DigitalOcean Spaces, RustFS...).

Problems are stored exactly as they sit in a judge's problem directory, under
`<prefix>/<storage namespace, if any>/<problem id>/`: `init.yml`, checkers, generators, and the test archive that
`archive:` in init.yml names. This is the layout judges already sync from, so nothing is converted or uploaded twice.

A VM lists the problem's objects, downloads the small ones (everything except zip archives) and reads the archive in
place: only its central directory, then the members of the cases it grades, with HTTP range requests.

The orchestrating judge holds the storage credentials. VMs get presigned URLs for exactly the objects they need, valid
for `url_expiry` seconds, re-sent with every request so grading never outlives them.

Presigning is AWS Signature Version 4 query signing, implemented here to avoid adding an SDK to the judge image.
"""

import datetime
import hashlib
import hmac
import urllib.parse
import xml.etree.ElementTree as ET
from typing import Any, Dict, List, Optional, Tuple

import requests

LIST_NAMESPACE = {'s3': 'http://s3.amazonaws.com/doc/2006-03-01/'}


def problem_prefix(prefix: str, problem_id: str, namespace: Optional[str]) -> str:
    parts = [part for part in (prefix.strip('/'), namespace, problem_id) if part]
    return '/'.join(parts) + '/'


def object_key(prefix: str, problem_id: str, namespace: Optional[str], name: str) -> str:
    return problem_prefix(prefix, problem_id, namespace) + name


def list_problem_objects(config: Any, problem_id: str, namespace: Optional[str]) -> List[Tuple[str, int]]:
    """Names (relative to the problem) and sizes of the problem's objects, through a presigned ListObjectsV2."""
    prefix = problem_prefix(config.prefix or '', problem_id, namespace)
    objects: List[Tuple[str, int]] = []
    token: Optional[str] = None
    while True:
        params = {'list-type': '2', 'prefix': prefix, 'max-keys': '1000'}
        if token:
            params['continuation-token'] = token
        url = _presign_config(config, 'GET', '', params)
        response = requests.get(url, timeout=60)
        response.raise_for_status()
        root = ET.fromstring(response.content)
        for item in root.findall('s3:Contents', LIST_NAMESPACE):
            key = item.findtext('s3:Key', namespaces=LIST_NAMESPACE) or ''
            name = key[len(prefix) :]
            if name and not name.endswith('/'):
                objects.append((name, int(item.findtext('s3:Size', default='0', namespaces=LIST_NAMESPACE))))
        token = root.findtext('s3:NextContinuationToken', namespaces=LIST_NAMESPACE)
        if not token:
            return objects


def problem_urls(config: Any, problem_id: str, namespace: Optional[str], names: List[str]) -> Dict[str, str]:
    """Fresh presigned GET URLs for the named objects of a problem."""
    return {
        name: _presign_config(config, 'GET', object_key(config.prefix or '', problem_id, namespace, name))
        for name in names
    }


def _presign_config(config: Any, method: str, key: str, params: Optional[Dict[str, str]] = None) -> str:
    return presign(
        method,
        endpoint=config.endpoint,
        bucket=config.bucket,
        key=key,
        access_key=config.access_key,
        secret_key=config.secret_key,
        region=config.region or 'us-east-1',
        expires=int(config.url_expiry or 900),
        params=params,
    )


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
    params: Optional[Dict[str, str]] = None,
) -> str:
    """
    Presigned path-style URL for `method` on `<endpoint>/<bucket>/<key>`, valid for `expires` seconds. `params` are
    extra query parameters that become part of the signed request (e.g. a ListObjectsV2 query on the bucket).
    """
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
        **(params or {}),
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
