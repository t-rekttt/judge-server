"""
Runs each runner in a DigitalOcean MicroVM (public preview API, `/v2/microvms`).

VMs are created either from the runner image (`image`) or, much faster, from a checkpoint of a runner that has
already started (`checkpoint_id`, see `.docker/microvm/create_checkpoint.py`). A VM created from a checkpoint inherits
its environment, so its runner token is the one baked into the checkpoint and must be configured as
`checkpoint_runner_token`; VMs created from the image get a fresh token each.

Requests to a VM's endpoint must carry a DigitalOcean token with the `microvm:access` scope in `Authorization`; the
runner's own token goes in `X-Runner-Token`.

Every VM is tagged with the judge it belongs to, so a restarted judge deletes VMs it left behind (`delete_orphans`).
"""

import datetime
import logging
import random
import re
import secrets
import threading
import time
from typing import Any, Dict, List, Optional, Set

import requests
import requests.adapters

from dmoj.remote.backend import CreationCancelled, VM, VMBackend, wait_until_healthy

logger = logging.getLogger(__name__)

RUNNER_TAG = 'dmoj-remote-runner'
# Seconds to wait for a VM to reach `running` with an active endpoint, then for its runner to answer.
DEFAULT_READY_TIMEOUT = 300
API_ATTEMPTS = 5
# Deleting is retried much longer: a VM that isn't deleted keeps costing money. With many judges in one process,
# bursts of deletions run into the API's rate limit (HTTP 429) for a while.
DELETE_ATTEMPTS = 30
# How often the sweeper looks for VMs of this judge that no session owns anymore.
SWEEP_INTERVAL = 180


class MicroVMError(Exception):
    pass


def judge_tag(judge_id: str) -> str:
    # Tags allow letters, digits, colons, dashes and underscores.
    return 'dmoj-judge-' + re.sub(r'[^A-Za-z0-9_-]', '-', judge_id)


class MicroVMBackend(VMBackend):
    def __init__(self, config: Any, judge_id: str) -> None:
        self.api_url: str = (config.api_url or 'https://api.digitalocean.com').rstrip('/')
        self.api_token: str = config.api_token
        # Token for VM endpoints; needs only the `microvm:access` scope. Defaults to the API token.
        self.access_token: str = config.access_token or self.api_token
        self.region: str = config.region or 'mkc1'
        self.image: Optional[str] = config.image
        self.checkpoint_id: Optional[str] = config.checkpoint_id
        self.checkpoint_runner_token: Optional[str] = config.checkpoint_runner_token
        # Smallest size by default: Go, the slowest compiler, builds in 9-13 s on one vCPU of a real MicroVM.
        self.cpus: int = int(config.cpus or 1)
        self.memory_mb: int = int(config.memory_mb or 2048)
        self.vpc_uuid: Optional[str] = config.vpc_uuid
        # The local test environment points this at a fake API that serves runners over plain HTTP.
        self.endpoint_scheme: str = config.endpoint_scheme or 'https'
        self.ready_timeout: float = float(config.ready_timeout or DEFAULT_READY_TIMEOUT)
        self.judge_tag = judge_tag(judge_id)

        if bool(self.image) == bool(self.checkpoint_id):
            raise ValueError('remote.microvm needs exactly one of `image` or `checkpoint_id`')
        if self.checkpoint_id and not self.checkpoint_runner_token:
            raise ValueError('remote.microvm.checkpoint_runner_token is required with `checkpoint_id`')

        self._http = requests.Session()
        self._http.headers['Authorization'] = f'Bearer {self.api_token}'
        # Many VMs are created and polled at once; the default pool of 10 connections would be discarded and
        # reopened constantly.
        adapter = requests.adapters.HTTPAdapter(pool_connections=4, pool_maxsize=64)
        self._http.mount('https://', adapter)
        self._http.mount('http://', adapter)

    def _api(
        self, method: str, path: str, ok=(200, 201, 204), attempts: int = API_ATTEMPTS, **kwargs
    ) -> requests.Response:
        """Calls the API, retrying rate limiting and server errors with backoff."""
        delay = 1.0
        for attempt in range(1, attempts + 1):
            try:
                response = self._http.request(method, self.api_url + path, timeout=30, **kwargs)
            except requests.RequestException as error:
                if attempt == attempts:
                    raise MicroVMError(f'{method} {path} failed: {error}') from error
            else:
                if response.status_code in ok:
                    return response
                retryable = response.status_code == 429 or response.status_code >= 500
                if not retryable or attempt == attempts:
                    raise MicroVMError(f'{method} {path} failed with {response.status_code}: {response.text[:500]}')
                if response.status_code == 429:
                    # `ratelimit-reset` is a Unix timestamp; wait until then (plus jitter so a burst of callers
                    # doesn't retry in the same instant), but never more than a minute.
                    reset = response.headers.get('ratelimit-reset')
                    until = float(reset) - time.time() if reset and reset.isdigit() else delay
                    delay = max(delay, min(until, 60.0)) + random.uniform(0, 2)
            time.sleep(delay)
            delay = min(delay * 2, 60.0)
        raise AssertionError('unreachable')

    def _create_request(self, name: str) -> Dict[str, Any]:
        request: Dict[str, Any] = {
            'name': name,
            'tags': [RUNNER_TAG, self.judge_tag],
            # A VM is deleted when its submission is graded; pausing only matters for one that leaked.
            'auto_pause': {'idle_timeout': '10m'},
        }
        if self.checkpoint_id:
            # Region, size, image and environment come from the checkpoint and may not be set.
            request['source'] = {'checkpoint_id': self.checkpoint_id}
        else:
            request['source'] = {'oci_ref': self.image}
            request['region'] = self.region
            request['size'] = {'cpu': self.cpus, 'memory': self.memory_mb}
            request['http_port'] = 8080
        if self.vpc_uuid:
            request['networking'] = 'vpc'
            request['vpc_uuid'] = self.vpc_uuid
        return request

    def create_vm(self, label: str, cancelled: Optional[threading.Event] = None) -> VM:
        # Names: unique per region, 1-63 of [a-z0-9-].
        name = re.sub(r'[^a-z0-9-]', '-', f'dmoj-{label}'.lower())[:54] + '-' + secrets.token_hex(4)
        request = self._create_request(name)
        if self.checkpoint_id:
            token = self.checkpoint_runner_token
        else:
            token = secrets.token_hex(32)
            request['environment'] = {'JUDGE_MODE': 'runner', 'RUNNER_TOKEN': token}
        assert token is not None

        microvm = self._api('POST', '/v2/microvms', json=request).json()['microvm']
        vm_id = microvm['id']
        try:
            hostname = self._wait_until_running(vm_id, microvm, cancelled)
            vm = VM(
                vm_id, f'{self.endpoint_scheme}://{hostname}', token, {'Authorization': f'Bearer {self.access_token}'}
            )
            wait_until_healthy(vm, self.ready_timeout, cancelled)
            return vm
        except BaseException:
            self._delete(vm_id)
            raise

    def _wait_until_running(
        self, vm_id: str, microvm: Dict[str, Any], cancelled: Optional[threading.Event] = None
    ) -> str:
        deadline = time.monotonic() + self.ready_timeout
        delay = 0.25
        while True:
            if cancelled is not None and cancelled.is_set():
                raise CreationCancelled(f'creating MicroVM {vm_id} was cancelled')
            state = microvm.get('state')
            if state in ('failed', 'terminating', 'terminated'):
                reason = microvm.get('failure_reason') or state
                raise MicroVMError(f'MicroVM {vm_id} failed to start: {reason}')
            endpoint = next((url for url in microvm.get('urls') or [] if url.get('default')), None)
            if state == 'running' and endpoint and endpoint.get('status') == 'ACTIVE' and endpoint.get('hostname'):
                return endpoint['hostname']
            if time.monotonic() > deadline:
                raise MicroVMError(f'MicroVM {vm_id} not ready after {self.ready_timeout} seconds (state {state})')
            time.sleep(delay)
            delay = min(delay * 2, 2.0)
            microvm = self._api('GET', f'/v2/microvms/{vm_id}').json()['microvm']

    def _delete(self, vm_id: str) -> None:
        try:
            self._api('DELETE', f'/v2/microvms/{vm_id}', ok=(204, 404), attempts=DELETE_ATTEMPTS)
        except Exception:
            # The sweeper will find it by tag and try again.
            logger.exception('Failed to delete MicroVM %s; it will be swept later', vm_id)

    def delete_vm(self, vm: VM) -> None:
        self._delete(vm.id)

    def list_vms(self) -> List[Dict[str, Any]]:
        """Every VM carrying this judge's tag."""
        vms: List[Dict[str, Any]] = []
        page = 1
        while True:
            data = self._api(
                'GET', '/v2/microvms', params={'tag_name': self.judge_tag, 'per_page': 200, 'page': page}
            ).json()
            vms += data.get('microvms') or []
            if not (data.get('links') or {}).get('pages', {}).get('next'):
                break
            page += 1
        return vms

    def delete_orphans(self, keep: Optional[Set[str]] = None, min_age: float = 0) -> int:
        """
        Deletes this judge's VMs except those in `keep` (ids of VMs sessions still own) and those younger than
        `min_age` seconds, which may belong to a submission that hasn't recorded them yet. Returns how many.
        """
        now = datetime.datetime.now(datetime.timezone.utc)
        orphans = []
        for vm in self.list_vms():
            if vm['id'] in (keep or set()):
                continue
            created = vm.get('created_at')
            if min_age and created:
                age = (now - datetime.datetime.fromisoformat(created.replace('Z', '+00:00'))).total_seconds()
                if age < min_age:
                    continue
            orphans.append(vm['id'])
        if orphans:
            logger.warning('Deleting %d MicroVMs of this judge that no submission owns', len(orphans))
        for vm_id in orphans:
            self._delete(vm_id)
        return len(orphans)
