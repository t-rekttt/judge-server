"""
Packs and restores compiled programs, so only the first VM of a submission compiles.

Uses the executor cache that already exists for checkers and interactors (`cached=True`): a compiled executor lives in
`compiled_binary_cache_dir` and is indexed by `<tempdir>/<sha384>.txt` files holding its executable path and
directory. If those index files point at existing files, constructing the same executor skips compilation. All VMs
run the same image with the same paths, so copying both to another VM makes it reuse the compiled programs. If
anything is missing, the executor just compiles again, so correctness never depends on the artifact.
"""

import glob
import io
import os
import re
import shutil
import tarfile
import tempfile
from typing import Optional

from dmoj.judgeenv import env

# Index files are named after a sha384 hex digest.
_INDEX_NAME = re.compile(r'^[0-9a-f]{96}\.txt$')


def _index_files():
    return [
        path
        for path in glob.glob(os.path.join(tempfile.gettempdir(), '*.txt'))
        if _INDEX_NAME.match(os.path.basename(path))
    ]


def _without_links(info: tarfile.TarInfo) -> Optional[tarfile.TarInfo]:
    # Graders leave per-case scratch links next to compiled programs (the bridged interactor's log file is a link to
    # /dev/fd/4). They aren't part of any program, and each VM recreates them when it grades.
    return None if info.issym() or info.islnk() else info


def pack() -> bytes:
    cache_dir = env.compiled_binary_cache_dir
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode='w:gz') as tar:
        for index_file in _index_files():
            tar.add(index_file, arcname=os.path.join('index', os.path.basename(index_file)))
        for entry in os.listdir(cache_dir):
            tar.add(os.path.join(cache_dir, entry), arcname=os.path.join('cache', entry), filter=_without_links)
    return buffer.getvalue()


def clear() -> None:
    """Deletes every compiled program and its index entry, so a VM reused for another submission compiles afresh."""
    for index_file in _index_files():
        os.remove(index_file)
    cache_dir = env.compiled_binary_cache_dir
    for entry in os.listdir(cache_dir):
        path = os.path.join(cache_dir, entry)
        if os.path.isdir(path) and not os.path.islink(path):
            shutil.rmtree(path, ignore_errors=True)
        else:
            os.remove(path)


def unpack(data: bytes) -> None:
    destinations = {'index': tempfile.gettempdir(), 'cache': env.compiled_binary_cache_dir}
    with tarfile.open(fileobj=io.BytesIO(data), mode='r:gz') as tar:
        for member in tar.getmembers():
            kind, _, name = member.name.partition('/')
            if kind not in destinations or not name or (kind == 'index' and not _INDEX_NAME.match(name)):
                raise ValueError(f'unexpected entry in artifact: {member.name}')
            member.name = name
            # The `data` filter rejects absolute paths, `..` and links leaving the destination directory.
            tar.extract(member, destinations[kind], filter='data')
