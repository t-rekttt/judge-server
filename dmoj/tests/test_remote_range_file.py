import io
import os
import random
import threading
import unittest
import zipfile
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from dmoj.remote.range_file import HTTPRangeFile, RangeRequestError, open_remote_zip


class RangeServer:
    """Serves one in-memory file with Range support, like S3 does, and records the bytes sent."""

    def __init__(self, data: bytes, honor_ranges: bool = True) -> None:
        self.data = data
        self.bytes_sent = 0
        server = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_GET(self):
                if self.path != '/file':
                    self.send_response(403)
                    self.end_headers()
                    return
                start, end = 0, len(server.data) - 1
                header = self.headers.get('Range')
                if header and honor_ranges:
                    first, _, last = header[len('bytes=') :].partition('-')
                    if first == '':
                        start = max(0, len(server.data) - int(last))
                    else:
                        start, end = int(first), min(int(last), end)
                    self.send_response(206)
                    self.send_header('Content-Range', f'bytes {start}-{end}/{len(server.data)}')
                else:
                    self.send_response(200)
                body = server.data[start : end + 1]
                server.bytes_sent += len(body)
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        self.httpd = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.url = f'http://127.0.0.1:{self.httpd.server_address[1]}/file'
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()

    def close(self) -> None:
        self.httpd.shutdown()
        self.httpd.server_close()


def make_archive(members: dict, compression=zipfile.ZIP_DEFLATED) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, 'w', compression) as archive:
        for name, data in members.items():
            archive.writestr(name, data)
    return buffer.getvalue()


class HTTPRangeFileTest(unittest.TestCase):
    def setUp(self):
        rng = random.Random(7)
        # 40 cases of 2 MiB incompressible input: a 80+ MiB archive.
        self.members = {}
        for case in range(1, 41):
            self.members[f'{case}.in'] = rng.randbytes(2 << 20)
            self.members[f'{case}.out'] = f'{case}\n'.encode()
        self.archive = make_archive(self.members)

    def open(self, server):
        archive = open_remote_zip(server.url)
        return archive.raw, archive

    def test_reads_one_case_without_downloading_the_archive(self):
        server = RangeServer(self.archive)
        try:
            raw, archive = self.open(server)
            self.assertEqual(sorted(archive.namelist()), sorted(self.members))
            self.assertEqual(archive.read('17.in'), self.members['17.in'])
            self.assertEqual(archive.read('17.out'), self.members['17.out'])
            # The tail block (central directory), then each member's own bytes plus at most the header margin.
            members_size = len(self.members['17.in']) + len(self.members['17.out'])
            self.assertLessEqual(server.bytes_sent, HTTPRangeFile.MIN_BLOCK + members_size + 2 * (64 << 10) + 200)
            self.assertLessEqual(raw.requests, 3)
            self.assertLess(raw.requests, 10)
        finally:
            server.close()

    def test_reading_every_member_fetches_the_archive_about_once(self):
        server = RangeServer(self.archive)
        try:
            _, archive = self.open(server)
            for name in sorted(self.members):
                archive.read(name)
            self.assertLess(server.bytes_sent, len(self.archive) * 1.05 + HTTPRangeFile.MIN_BLOCK)
        finally:
            server.close()

    def test_every_member_reads_back_exactly(self):
        server = RangeServer(self.archive)
        try:
            _, archive = self.open(server)
            for name in random.Random(1).sample(sorted(self.members), len(self.members)):
                self.assertEqual(archive.read(name), self.members[name], name)
        finally:
            server.close()

    def test_small_and_stored_archives(self):
        for compression in (zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED):
            data = make_archive({'1.in': b'1 2\n', '1.out': b'3\n'}, compression)
            server = RangeServer(data)
            try:
                _, archive = self.open(server)
                self.assertEqual(archive.read('1.in'), b'1 2\n')
                self.assertEqual(archive.read('1.out'), b'3\n')
            finally:
                server.close()

    def test_server_ignoring_ranges_still_works(self):
        data = make_archive({'1.in': os.urandom(3 << 20), '1.out': b'x\n'})
        server = RangeServer(data, honor_ranges=False)
        try:
            _, archive = self.open(server)
            self.assertEqual(archive.read('1.out'), b'x\n')
        finally:
            server.close()

    def test_url_can_be_replaced(self):
        server = RangeServer(self.archive)
        try:
            raw, archive = self.open(server)
            raw.url = server.url + '-expired'
            with self.assertRaises(RangeRequestError):
                archive.read('3.in')
            raw.url = server.url
            self.assertEqual(archive.read('3.in'), self.members['3.in'])
        finally:
            server.close()
