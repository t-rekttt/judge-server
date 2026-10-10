"""
Creates and deletes the VMs that runners live in.

`create_vm` blocks until the runner answers its health check. If `cancelled` is set first, it deletes the VM and raises
`CreationCancelled` promptly. `delete_vm` must be safe to call on a VM that is already gone. `delete_orphans` removes VMs a previous run of this judge left behind (e.g. after a crash).

Backends: `microvm` (DigitalOcean MicroVMs, `microvm_backend.py`) and `docker`, which runs runner containers with the
same restrictions MicroVMs impose (arbitrary uid in group 0, no capabilities, no_new_privs) as a local stand-in.
"""

import http.client
import json
import logging
import secrets
import socket
import threading
import time
import urllib.parse
from typing import Any, Dict, NamedTuple, Optional

import requests

logger = logging.getLogger(__name__)

HEALTH_TIMEOUT = 120  # seconds


class VM(NamedTuple):
    id: str
    url: str
    # The runner's own token, sent as `X-Runner-Token`.
    token: str
    # Extra headers the platform requires on every request to the VM (e.g. DigitalOcean's endpoint token).
    headers: Dict[str, str] = {}


class CreationCancelled(Exception):
    pass


class VMBackend:
    def create_vm(self, label: str, cancelled: Optional[threading.Event] = None) -> VM:
        raise NotImplementedError

    def delete_vm(self, vm: VM) -> None:
        raise NotImplementedError

    def delete_orphans(self) -> None:
        raise NotImplementedError


def wait_until_healthy(vm: VM, timeout: float = HEALTH_TIMEOUT, cancelled: Optional[threading.Event] = None) -> None:
    deadline = time.monotonic() + timeout
    while True:
        if cancelled is not None and cancelled.is_set():
            raise CreationCancelled(f'creating runner {vm.id} was cancelled')
        try:
            if requests.get(vm.url + '/health', headers=vm.headers, timeout=5).status_code == 200:
                return
        except requests.RequestException:
            pass
        if time.monotonic() > deadline:
            raise TimeoutError(f'runner {vm.id} did not become healthy within {timeout} seconds')
        time.sleep(0.2)


class _UnixHTTPConnection(http.client.HTTPConnection):
    def __init__(self, path: str, timeout: float) -> None:
        super().__init__('localhost', timeout=timeout)
        self._path = path

    def connect(self) -> None:
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        self.sock.connect(self._path)


class DockerBackend(VMBackend):
    """Runs each runner as a container through the Docker Engine API, so no Docker CLI is needed."""

    def __init__(self, config: Any, judge_id: str) -> None:
        self.judge_id = judge_id
        self.image: str = config.image
        self.network: str = config.network
        self.socket_path: str = config.socket or '/var/run/docker.sock'
        self.user: str = str(config.user or 101)
        self.memory_mb: Optional[int] = config.memory_mb
        self.cpus: Optional[float] = config.cpus

    def _call(self, method: str, path: str, body: Optional[dict] = None, ok=(200, 201, 204)) -> Any:
        connection = _UnixHTTPConnection(self.socket_path, timeout=60)
        try:
            payload = json.dumps(body) if body is not None else None
            connection.request(method, path, payload, {'Content-Type': 'application/json'})
            response = connection.getresponse()
            data = response.read()
        finally:
            connection.close()
        if response.status not in ok:
            raise RuntimeError(f'docker {method} {path} failed with {response.status}: {data[:500]!r}')
        return json.loads(data) if data else None

    def create_vm(self, label: str, cancelled: Optional[threading.Event] = None) -> VM:
        token = secrets.token_hex(32)
        name = f'dmoj-runner-{label}-{secrets.token_hex(4)}'
        host_config: dict = {
            'AutoRemove': True,
            'NetworkMode': self.network,
            'CapDrop': ['ALL'],
            # MicroVMs have no container seccomp filter and set no_new_privs; cptbox installs its own filter.
            'SecurityOpt': ['seccomp=unconfined', 'no-new-privileges'],
        }
        if self.memory_mb:
            host_config['Memory'] = self.memory_mb * 1024 * 1024
        if self.cpus:
            host_config['NanoCpus'] = int(self.cpus * 1e9)
        self._call(
            'POST',
            f'/containers/create?name={name}',
            {
                'Image': self.image,
                'User': self.user,
                'Env': ['JUDGE_MODE=runner', f'RUNNER_TOKEN={token}'],
                'Labels': {'dmoj.remote-runner': '1', 'dmoj.judge': self.judge_id},
                'HostConfig': host_config,
            },
        )
        vm = VM(name, f'http://{name}:8080', token)
        try:
            self._call('POST', f'/containers/{name}/start')
            wait_until_healthy(vm, cancelled=cancelled)
        except BaseException:
            self.delete_vm(vm)
            raise
        return vm

    def delete_vm(self, vm: VM) -> None:
        try:
            self._call('DELETE', f'/containers/{vm.id}?force=true', ok=(204, 404, 409))
        except Exception:
            logger.exception('Failed to delete runner %s', vm.id)

    def delete_orphans(self) -> None:
        filters = urllib.parse.quote(json.dumps({'label': [f'dmoj.judge={self.judge_id}']}))
        for container in self._call('GET', f'/containers/json?all=true&filters={filters}') or []:
            logger.warning('Deleting runner %s left behind by a previous run of this judge', container['Id'][:12])
            self.delete_vm(VM(container['Id'], '', ''))


def make_backend(config: Any, judge_id: str) -> VMBackend:
    if config.backend == 'docker':
        return DockerBackend(config.docker, judge_id)
    if config.backend == 'microvm':
        from dmoj.remote.microvm_backend import MicroVMBackend

        return MicroVMBackend(config.microvm, judge_id)
    raise ValueError(f'unknown remote grading backend: {config.backend!r}')
