"""
Downloads every problem's init.yml from the judge's storage into a directory, one `<problem>/init.yml` each, so the
orchestrating judge can announce those problems to the site (its `problem_storage_globs`). The VMs read the actual
problem data from storage; these copies only say what the judges grade.

    python3 fetch_problem_inits.py -c judge.yml problems/

Problems removed from storage are removed from the directory too.
"""

import argparse
import os
import shutil
import sys
import xml.etree.ElementTree as ET
from types import SimpleNamespace

import requests
import yaml

from dmoj.remote.storage import LIST_NAMESPACE, _presign_config, problem_urls


def storage_config(path: str) -> SimpleNamespace:
    with open(path) as f:
        config = yaml.safe_load(f)
    storage = dict(config['remote']['storage'])
    storage.setdefault('prefix', '')
    storage.setdefault('region', 'us-east-1')
    storage.setdefault('url_expiry', 900)
    return SimpleNamespace(**storage)


def list_problems(config: SimpleNamespace) -> list:
    """Problem ids: the directories right under the storage prefix."""
    prefix = config.prefix.strip('/')
    prefix = prefix + '/' if prefix else ''
    problems, token = [], None
    while True:
        params = {'list-type': '2', 'prefix': prefix, 'delimiter': '/', 'max-keys': '1000'}
        if token:
            params['continuation-token'] = token
        response = requests.get(_presign_config(config, 'GET', '', params), timeout=60)
        response.raise_for_status()
        root = ET.fromstring(response.content)
        for item in root.findall('s3:CommonPrefixes', LIST_NAMESPACE):
            folder = item.findtext('s3:Prefix', default='', namespaces=LIST_NAMESPACE)
            problems.append(folder[len(prefix) :].strip('/'))
        token = root.findtext('s3:NextContinuationToken', namespaces=LIST_NAMESPACE)
        if not token:
            return sorted(p for p in problems if p)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
    parser.add_argument('-c', '--config', required=True, help='judge configuration (remote.storage is used)')
    parser.add_argument('directory', help='where <problem>/init.yml files go')
    args = parser.parse_args()

    config = storage_config(args.config)
    os.makedirs(args.directory, exist_ok=True)
    found, skipped = [], []
    for problem in list_problems(config):
        url = problem_urls(config, problem, None, ['init.yml'])['init.yml']
        response = requests.get(url, timeout=60)
        if response.status_code != 200:
            skipped.append(problem)
            continue
        os.makedirs(os.path.join(args.directory, problem), exist_ok=True)
        with open(os.path.join(args.directory, problem, 'init.yml'), 'wb') as f:
            f.write(response.content)
        found.append(problem)

    for entry in os.listdir(args.directory):
        if entry not in found and os.path.isdir(os.path.join(args.directory, entry)):
            shutil.rmtree(os.path.join(args.directory, entry))
            print(f'removed {entry}: no longer in storage')
    print(f'{len(found)} problems announced from {args.directory}')
    if skipped:
        print(f'{len(skipped)} folders without init.yml skipped: {" ".join(skipped)}', file=sys.stderr)
    return 0


if __name__ == '__main__':
    sys.exit(main())
