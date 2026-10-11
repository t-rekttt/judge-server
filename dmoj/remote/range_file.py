"""
Reads members of a remote zip archive with HTTP range requests, downloading only the bytes they occupy.

`open_remote_zip` opens the archive from its tail (end record and central directory). The central directory gives
every member's offset and size, so opening a member announces its exact byte range, and reading it then takes
one request per `MAX_BLOCK`. Reads outside an announced range fall back to `MIN_BLOCK`-sized requests.
"""

import io
import zipfile
from typing import Optional, Tuple

import requests


class RangeRequestError(OSError):
    pass


class HTTPRangeFile(io.RawIOBase):
    MIN_BLOCK = 1 << 20
    MAX_BLOCK = 64 << 20
    ATTEMPTS = 3

    def __init__(self, url: str, session: Optional[requests.Session] = None, timeout: float = 60) -> None:
        super().__init__()
        # May be replaced with a fresh presigned URL for the same object at any time.
        self.url = url
        self._http = session or requests.Session()
        self._timeout = timeout
        self._pos = 0
        # Byte range of the member being read, announced by `RemoteZipFile.open`.
        self._expected: Optional[Tuple[int, int]] = None
        self.bytes_fetched = 0
        self.requests = 0

        # The tail holds the zip's end record and usually its whole central directory, and gives us the file size.
        response = self._get(f'bytes=-{self.MIN_BLOCK}')
        if response.status_code == 200:
            self.size = len(response.content)
        else:
            self.size = int(response.headers['Content-Range'].rsplit('/', 1)[1])
        self._block = response.content
        self._block_start = self.size - len(self._block)

    def _get(self, byte_range: str) -> requests.Response:
        for attempt in range(1, self.ATTEMPTS + 1):
            try:
                response = self._http.get(self.url, headers={'Range': byte_range}, timeout=self._timeout)
            except requests.RequestException as error:
                if attempt == self.ATTEMPTS:
                    raise RangeRequestError(f'range request {byte_range} failed: {error}') from error
                continue
            if response.status_code in (200, 206):
                self.requests += 1
                self.bytes_fetched += len(response.content)
                return response
            if response.status_code < 500 or attempt == self.ATTEMPTS:
                raise RangeRequestError(f'range request {byte_range} failed with HTTP {response.status_code}')
        raise AssertionError('unreachable')

    def expect(self, start: int, end: int) -> None:
        """Announces that bytes [start, end) are about to be read, so they are fetched without overshooting."""
        self._expected = (start, min(end, self.size))

    def _fetch(self, start: int) -> None:
        if self._expected is not None and self._expected[0] <= start < self._expected[1]:
            end = min(self._expected[1], start + self.MAX_BLOCK)
        else:
            end = min(start + self.MIN_BLOCK, self.size)
        response = self._get(f'bytes={start}-{end - 1}')
        # A server ignoring the range sends the whole file with 200.
        content = response.content[start:end] if response.status_code == 200 else response.content
        self._block_start, self._block = start, content

    def readinto(self, buffer) -> int:  # type: ignore[override]
        # Always fill the buffer (up to EOF): zipfile treats short reads of headers as a truncated archive.
        view = memoryview(buffer).cast('B')
        filled = 0
        while filled < len(view) and self._pos < self.size:
            offset = self._pos - self._block_start
            if not 0 <= offset < len(self._block):
                self._fetch(self._pos)
                offset = 0
            count = min(len(view) - filled, len(self._block) - offset)
            view[filled : filled + count] = self._block[offset : offset + count]
            filled += count
            self._pos += count
        return filled

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        if whence == io.SEEK_SET:
            position = offset
        elif whence == io.SEEK_CUR:
            position = self._pos + offset
        elif whence == io.SEEK_END:
            position = self.size + offset
        else:
            raise ValueError(f'invalid whence: {whence}')
        if position < 0:
            raise OSError('negative seek position')
        self._pos = position
        return position

    def tell(self) -> int:
        return self._pos

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True


# Local file header: fixed part, then the file name and an extra field that may differ from the central directory's.
_LOCAL_HEADER_SIZE = 30
_LOCAL_EXTRA_MARGIN = 1 << 16


class RemoteZipFile(zipfile.ZipFile):
    def __init__(self, raw: HTTPRangeFile) -> None:
        self.raw = raw
        super().__init__(io.BufferedReader(raw, buffer_size=1 << 16))

    def open(self, name, mode='r', pwd=None, *, force_zip64=False):  # type: ignore[override]
        if mode == 'r':
            info = name if isinstance(name, zipfile.ZipInfo) else self.getinfo(name)
            start = info.header_offset
            header = _LOCAL_HEADER_SIZE + len(info.filename.encode()) + _LOCAL_EXTRA_MARGIN
            self.raw.expect(start, start + header + info.compress_size)
        return super().open(name, mode, pwd, force_zip64=force_zip64)


def open_remote_zip(url: str, session: Optional[requests.Session] = None) -> RemoteZipFile:
    return RemoteZipFile(HTTPRangeFile(url, session))
