"""
Creates the MicroVM checkpoint remote grading starts its VMs from.

A VM created from the runner image boots the container and starts the runner, which loads every executor: tens of
seconds per VM. A VM created from a checkpoint of a runner that has already started resumes with the runner listening,
so it's ready almost at once. This boots one runner from the image, waits until it answers, checkpoints it, and
deletes it. Configure the printed checkpoint ID and runner token in the judge (`remote.microvm.checkpoint_id`,
`remote.microvm.checkpoint_runner_token`). Recreate the checkpoint whenever the image changes.

The runner token is baked into the checkpoint's environment, so every VM created from it shares that token. Keep it
secret like the API token.

Usage: python create_checkpoint.py <image> [--region mkc1] [--cpus 1] [--memory-mb 2048] [--api-url URL]
                                   [--endpoint-scheme https]
The DigitalOcean token is read from DIGITALOCEAN_TOKEN (scopes: microvm:create, microvm:read, microvm:delete,
microvm:access).
"""

import argparse
import os
import secrets
import sys
import time

import requests

from dmoj.remote.backend import VM, wait_until_healthy

CHECKPOINT_TIMEOUT = 900  # seconds


def main() -> None:
    parser = argparse.ArgumentParser(description='Creates the MicroVM checkpoint remote grading starts VMs from.')
    parser.add_argument('image')
    parser.add_argument('--region', default='mkc1')
    parser.add_argument('--cpus', type=int, default=1)
    parser.add_argument('--memory-mb', type=int, default=2048)
    parser.add_argument('--api-url', default='https://api.digitalocean.com')
    parser.add_argument('--endpoint-scheme', default='https')
    args = parser.parse_args()

    token = os.environ.get('DIGITALOCEAN_TOKEN')
    if not token:
        raise SystemExit('DIGITALOCEAN_TOKEN must be set')
    api = requests.Session()
    api.headers['Authorization'] = f'Bearer {token}'

    def call(method: str, path: str, **kwargs) -> dict:
        response = api.request(method, args.api_url + path, timeout=60, **kwargs)
        if response.status_code >= 300:
            raise SystemExit(f'{method} {path} failed with {response.status_code}: {response.text[:500]}')
        return response.json() if response.content else {}

    runner_token = secrets.token_hex(32)
    microvm = call(
        'POST',
        '/v2/microvms',
        json={
            'name': f'dmoj-runner-checkpoint-{secrets.token_hex(4)}',
            'region': args.region,
            'size': {'cpu': args.cpus, 'memory': args.memory_mb},
            'source': {'oci_ref': args.image},
            'http_port': 8080,
            'environment': {'JUDGE_MODE': 'runner', 'RUNNER_TOKEN': runner_token},
            'tags': ['dmoj-remote-runner-checkpoint'],
        },
    )['microvm']
    vm_id = microvm['id']
    print(f'Created MicroVM {vm_id}, waiting for the runner...', file=sys.stderr)
    try:
        deadline = time.monotonic() + CHECKPOINT_TIMEOUT
        while True:
            endpoint = next((url for url in microvm.get('urls') or [] if url.get('default')), {})
            if microvm['state'] == 'running' and endpoint.get('status') == 'ACTIVE' and endpoint.get('hostname'):
                break
            if microvm['state'] == 'failed' or time.monotonic() > deadline:
                raise SystemExit(f'MicroVM did not start: {microvm.get("failure_reason") or microvm["state"]}')
            time.sleep(2)
            microvm = call('GET', f'/v2/microvms/{vm_id}')['microvm']

        url = f'{args.endpoint_scheme}://{endpoint["hostname"]}'
        wait_until_healthy(VM(vm_id, url, runner_token, {'Authorization': f'Bearer {token}'}), CHECKPOINT_TIMEOUT)

        checkpoint = call('POST', f'/v2/microvms/{vm_id}/checkpoints', json={'name': f'dmoj-runner-{int(time.time())}'})
        checkpoint_id = checkpoint['checkpoint']['id']
        print(f'Capturing checkpoint {checkpoint_id}...', file=sys.stderr)
        while True:
            status = call('GET', f'/v2/microvms/checkpoints/{checkpoint_id}')['checkpoint']['status']
            if status == 'CHECKPOINT_AVAILABLE':
                break
            if status != 'CHECKPOINT_CREATING' or time.monotonic() > deadline:
                raise SystemExit(f'checkpoint failed: {status}')
            time.sleep(2)
    finally:
        api.delete(f'{args.api_url}/v2/microvms/{vm_id}', timeout=60)

    print(f'checkpoint_id: {checkpoint_id}')
    print(f'checkpoint_runner_token: {runner_token}')


if __name__ == '__main__':
    main()
