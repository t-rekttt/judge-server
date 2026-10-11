import os
import shutil
import tempfile
import unittest
from unittest import mock

from dmoj.remote import artifact


class RemoteArtifactTest(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.root)
        self.cache = os.path.join(self.root, 'cache')
        self.index = os.path.join(self.root, 'index')
        os.makedirs(self.cache)
        os.makedirs(self.index)
        mock.patch.object(artifact.env, 'compiled_binary_cache_dir', self.cache).start()
        mock.patch('dmoj.remote.artifact.tempfile.gettempdir', return_value=self.index).start()
        self.addCleanup(mock.patch.stopall)

    def reset(self):
        for path in (self.cache, self.index):
            shutil.rmtree(path)
            os.makedirs(path)

    def test_round_trip_skips_scratch_links(self):
        program_dir = os.path.join(self.cache, 'tmpabc')
        os.makedirs(program_dir)
        with open(os.path.join(program_dir, 'interactor'), 'wb') as f:
            f.write(b'\x7fELF')
        # Left behind by the bridged interactor grader after a case.
        os.symlink('/dev/fd/4', os.path.join(program_dir, 'k3j9x2ab'))
        index_name = 'a' * 96 + '.txt'
        with open(os.path.join(self.index, index_name), 'w') as f:
            f.write(f'{program_dir}/interactor\n{program_dir}')
        with open(os.path.join(self.index, 'unrelated.txt'), 'w') as f:
            f.write('not an executor index')

        data = artifact.pack()
        self.reset()
        artifact.unpack(data)

        self.assertEqual(sorted(os.listdir(program_dir)), ['interactor'])
        self.assertEqual(sorted(os.listdir(self.index)), [index_name])

    def test_rejects_unexpected_entries(self):
        import io
        import tarfile

        buffer = io.BytesIO()
        with tarfile.open(fileobj=buffer, mode='w:gz') as tar:
            info = tarfile.TarInfo('index/../../etc/evil.txt')
            tar.addfile(info, io.BytesIO(b''))
        with self.assertRaises(ValueError):
            artifact.unpack(buffer.getvalue())
