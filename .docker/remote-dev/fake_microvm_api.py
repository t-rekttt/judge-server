"""
A local stand-in for DigitalOcean's MicroVM API (`/v2/microvms`, public preview), to test the judge's MicroVM backend
end to end without a DigitalOcean account.

It implements the parts the judge uses, following the published OpenAPI spec (digitalocean/openapi,
`specification/resources/microvms`): create (from an image or a checkpoint), get, list with `tag_name` filtering and
pagination, delete, and checkpoint create/get. Each "MicroVM" is a runner container started through the Docker Engine
API with MicroVM-like restrictions. Like the real API, a new VM is `creating` with a PENDING endpoint for a while
before it is `running`, and requests to the VM's endpoint must carry the API token; this server proxies them.

A checkpoint here only records the source VM's image and environment (no memory snapshot), which is what a VM created
from it inherits.

Usage: python fake_microvm_api.py <port> <api token> <docker network> <host name others reach this server by>
"""

import json
import os
import socket
import sys
import threading
import time
import urllib.parse
import uuid
from http.client import HTTPConnection
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PROVISION_SECONDS = 1.5


class UnixHTTPConnection(HTTPConnection):
    def __init__(self) -> None:
        super().__init__('localhost', timeout=60)

    def connect(self) -> None:
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.connect('/var/run/docker.sock')


def docker(method: str, path: str, body=None):
    connection = UnixHTTPConnection()
    try:
        connection.request(
            method, path, json.dumps(body) if body is not None else None, {'Content-Type': 'application/json'}
        )
        response = connection.getresponse()
        data = response.read()
    finally:
        connection.close()
    if response.status >= 300 and response.status != 404:
        raise RuntimeError(f'docker {method} {path}: {response.status} {data[:300]!r}')
    return json.loads(data) if data else None


class State:
    def __init__(self, token: str, network: str, port: int, host: str) -> None:
        self.token = token
        self.network = network
        self.port = port
        self.host = host
        self.lock = threading.Lock()
        self.vms: dict = {}
        self.checkpoints: dict = {}
        self.created = 0
        # Rate limit like DigitalOcean's: a budget per hour window, reported in every response, 429 once spent.
        # FAKE_API_HOURLY_LIMIT lowers it so a testsuite run exercises the judge's pacing.
        self.hourly_limit = int(os.environ.get('FAKE_API_HOURLY_LIMIT') or 5000)
        self.window_start = time.time()
        self.used = 0
        self.rejected = 0

    def take_request(self) -> tuple:
        """Counts one request; returns (allowed, headers)."""
        with self.lock:
            now = time.time()
            if now >= self.window_start + 3600:
                self.window_start, self.used = now, 0
            allowed = self.used < self.hourly_limit
            if allowed:
                self.used += 1
            else:
                self.rejected += 1
            headers = {
                'ratelimit-limit': str(self.hourly_limit),
                'ratelimit-remaining': str(max(0, self.hourly_limit - self.used)),
                'ratelimit-reset': str(int(self.window_start + 3600)),
            }
            if not allowed:
                headers['retry-after'] = str(int(self.window_start + 3600 - now))
            return allowed, headers


def error(handler, status: int, code: str, message: str) -> None:
    handler.reply(status, {'id': code.lower(), 'code': code, 'message': message})


def make_handler(state: State):
    class Handler(BaseHTTPRequestHandler):
        protocol_version = 'HTTP/1.1'

        def log_message(self, *args):
            pass

        def reply(self, status: int, body=None, raw: bytes = None, headers=None) -> None:
            data = raw if raw is not None else (json.dumps(body).encode() if body is not None else b'')
            self.send_response(status)
            for key, value in {**getattr(self, 'rate_headers', {}), **(headers or {})}.items():
                self.send_header(key, value)
            if raw is None and body is not None:
                self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def body(self) -> bytes:
            return self.request_body

        def authorized(self) -> bool:
            if self.headers.get('Authorization') == f'Bearer {state.token}':
                return True
            error(self, 401, 'UNAUTHORIZED', 'Unable to authenticate you')
            return False

        def do_GET(self):
            self.route('GET')

        def do_POST(self):
            self.route('POST')

        def do_DELETE(self):
            self.route('DELETE')

        def route(self, method: str) -> None:
            # Read the body up front: on a kept-alive connection, unread bytes would corrupt the next request.
            self.request_body = self.rfile.read(int(self.headers.get('Content-Length') or 0))
            if not self.authorized():
                return
            parsed = urllib.parse.urlsplit(self.path)
            parts = [p for p in parsed.path.split('/') if p]
            # Endpoint traffic: /vm/<id>/... is proxied to the VM's runner, like <id>.<region>.microvm.ondigitalocean.com
            if parts[:1] == ['vm'] and len(parts) >= 2:
                return self.proxy(method, parts[1], '/' + '/'.join(parts[2:]), parsed.query)
            if parts[:2] != ['v2', 'microvms']:
                return error(self, 404, 'NOT_FOUND', 'not found')
            allowed, self.rate_headers = state.take_request()
            if not allowed:
                return error(self, 429, 'TOO_MANY_REQUESTS', 'API Rate limit exceeded.')
            rest = parts[2:]
            if method == 'POST' and not rest:
                return self.create()
            if method == 'GET' and not rest:
                return self.list(urllib.parse.parse_qs(parsed.query))
            if rest[:1] == ['checkpoints'] and len(rest) == 2 and method == 'GET':
                return self.get_checkpoint(rest[1])
            if len(rest) == 2 and rest[1] == 'checkpoints' and method == 'POST':
                return self.create_checkpoint(rest[0])
            if len(rest) == 1 and method == 'GET':
                return self.get(rest[0])
            if len(rest) == 1 and method == 'DELETE':
                return self.delete(rest[0])
            error(self, 404, 'NOT_FOUND', 'not found')

        def view(self, vm: dict) -> dict:
            ready = time.monotonic() >= vm['ready_at']
            return {
                'id': vm['id'],
                'name': vm['name'],
                'region': vm['region'],
                'size': vm['size'],
                'source': vm['source'],
                'state': 'running' if ready else 'creating',
                'networking': 'public',
                'auto_pause': {'enabled': True, 'idle_timeout': '10m0s'},
                'auto_resume': True,
                'http_protocol': 'http',
                'ports': [8080],
                'tags': vm['tags'],
                'urls': [
                    {
                        # Real endpoints are <id>.<region>.microvm.ondigitalocean.com; here, a path on this server.
                        'hostname': f'{state.host}:{state.port}/vm/{vm["id"]}' if ready else '',
                        'port': 8080,
                        'default': True,
                        'status': 'ACTIVE' if ready else 'PENDING',
                    }
                ],
                'created_at': vm['created_at'],
            }

        def create(self) -> None:
            request = json.loads(self.body())
            source = request.get('source') or {}
            if bool(source.get('oci_ref')) == bool(source.get('checkpoint_id')):
                return error(self, 400, 'BAD_REQUEST', 'set exactly one of source.oci_ref or source.checkpoint_id')
            if source.get('checkpoint_id'):
                if any(key in request for key in ('region', 'size', 'environment')):
                    return error(self, 400, 'BAD_REQUEST', 'region, size and environment come from the checkpoint')
                checkpoint = state.checkpoints.get(source['checkpoint_id'])
                if checkpoint is None:
                    return error(self, 404, 'NOT_FOUND', 'checkpoint not found')
                image, environment, size = checkpoint['image'], checkpoint['environment'], checkpoint['size']
                region, delay = checkpoint['region'], 0
            else:
                size = request.get('size') or {}
                if (size.get('cpu'), size.get('memory')) not in ((1, 2048), (2, 4096), (8, 16384), (16, 32768)):
                    return error(self, 400, 'BAD_REQUEST', 'invalid size')
                if not request.get('region'):
                    return error(self, 400, 'BAD_REQUEST', 'region is required')
                image, environment, region = source['oci_ref'], request.get('environment') or {}, request['region']
                delay = PROVISION_SECONDS
            vm_id = str(uuid.uuid4())
            container = f'fake-microvm-{vm_id[:8]}'
            docker(
                'POST',
                f'/containers/create?name={container}',
                {
                    'Image': image,
                    'User': '101',
                    'Env': [f'{k}={v}' for k, v in environment.items()],
                    'Labels': {'dmoj.remote-runner': '1', 'fake-microvm': vm_id},
                    'HostConfig': {
                        'AutoRemove': True,
                        'NetworkMode': state.network,
                        'CapDrop': ['ALL'],
                        'SecurityOpt': ['seccomp=unconfined', 'no-new-privileges'],
                        'NanoCpus': int(size['cpu'] * 1e9),
                        'Memory': size['memory'] * 1024 * 1024,
                    },
                },
            )
            docker('POST', f'/containers/{container}/start')
            vm = {
                'id': vm_id,
                'name': request['name'],
                'region': region,
                'size': {**size, 'disk': 50},
                'source': source,
                'tags': request.get('tags') or [],
                'image': image,
                'environment': environment,
                'container': container,
                'ready_at': time.monotonic() + delay,
                'created_at': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
            }
            with state.lock:
                state.vms[vm_id] = vm
                state.created += 1
            self.reply(201, {'microvm': self.view(vm)})

        def get(self, vm_id: str) -> None:
            vm = state.vms.get(vm_id)
            if vm is None:
                return error(self, 404, 'NOT_FOUND', 'MicroVM not found')
            self.reply(200, {'microvm': self.view(vm)})

        def list(self, query: dict) -> None:
            tag = (query.get('tag_name') or [None])[0]
            per_page = int((query.get('per_page') or ['25'])[0])
            page = int((query.get('page') or ['1'])[0])
            with state.lock:
                matching = [vm for vm in state.vms.values() if tag is None or tag in vm['tags']]
            chunk = matching[(page - 1) * per_page : page * per_page]
            links = {'pages': {'next': f'?page={page + 1}'}} if page * per_page < len(matching) else {}
            self.reply(
                200, {'microvms': [self.view(vm) for vm in chunk], 'links': links, 'meta': {'total': len(matching)}}
            )

        def delete(self, vm_id: str) -> None:
            with state.lock:
                vm = state.vms.get(vm_id)
                if vm is not None and time.monotonic() < vm['ready_at']:
                    # DigitalOcean refuses to delete a VM that is still provisioning.
                    return error(self, 400, 'FAILED_PRECONDITION', f'MicroVM {vm_id} is still provisioning')
                vm = state.vms.pop(vm_id, None)
            if vm is None:
                return error(self, 404, 'NOT_FOUND', 'MicroVM not found')
            docker('DELETE', f'/containers/{vm["container"]}?force=true')
            self.reply(204)

        def create_checkpoint(self, vm_id: str) -> None:
            vm = state.vms.get(vm_id)
            if vm is None:
                return error(self, 404, 'NOT_FOUND', 'MicroVM not found')
            checkpoint_id = str(uuid.uuid4())
            state.checkpoints[checkpoint_id] = {
                'id': checkpoint_id,
                'microvm_id': vm_id,
                'image': vm['image'],
                'region': vm['region'],
                'environment': vm['environment'],
                'size': {k: vm['size'][k] for k in ('cpu', 'memory')},
                'ready_at': time.monotonic() + 1,
            }
            self.get_checkpoint(checkpoint_id, status=201)

        def get_checkpoint(self, checkpoint_id: str, status: int = 200) -> None:
            checkpoint = state.checkpoints.get(checkpoint_id)
            if checkpoint is None:
                return error(self, 404, 'NOT_FOUND', 'checkpoint not found')
            ready = time.monotonic() >= checkpoint['ready_at']
            self.reply(
                status,
                {
                    'checkpoint': {
                        'id': checkpoint_id,
                        'microvm_id': checkpoint['microvm_id'],
                        'region': checkpoint['region'],
                        'size': checkpoint['size'],
                        'status': 'CHECKPOINT_AVAILABLE' if ready else 'CHECKPOINT_CREATING',
                    }
                },
            )

        def proxy(self, method: str, vm_id: str, path: str, query: str) -> None:
            vm = state.vms.get(vm_id)
            if vm is None or time.monotonic() < vm['ready_at']:
                return self.reply(502, raw=b'no such MicroVM endpoint')
            body = self.body() if method == 'POST' else None
            headers = {k: v for k, v in self.headers.items() if k.lower() in ('content-type', 'x-runner-token')}
            connection = HTTPConnection(vm['container'], 8080, timeout=600)
            try:
                connection.request(method, path + (f'?{query}' if query else ''), body, headers)
                response = connection.getresponse()
                data = response.read()
            except OSError:
                return self.reply(502, raw=b'MicroVM workload is not answering')
            finally:
                connection.close()
            content_type = response.getheader('Content-Type')
            self.reply(response.status, raw=data, headers={'Content-Type': content_type} if content_type else None)

    return Handler


def main() -> None:
    port, token, network, host = int(sys.argv[1]), sys.argv[2], sys.argv[3], sys.argv[4]
    state = State(token, network, port, host)
    server = ThreadingHTTPServer(('0.0.0.0', port), make_handler(state))
    print(f'Fake MicroVM API listening on {port}', flush=True)
    try:
        server.serve_forever()
    finally:
        for vm in list(state.vms.values()):
            docker('DELETE', f'/containers/{vm["container"]}?force=true')


if __name__ == '__main__':
    main()
