import os
import shutil
import tempfile
import unittest
from typing import List
from unittest import mock

from dmoj.executors.compiled_executor import CompiledExecutor


class CompiledCacheTest(unittest.TestCase):
    """The executor cache restores compiled programs, for the judge's cache_all_compiled_binaries and for checkers."""

    def setUp(self) -> None:
        self.root = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.root, True)
        self.cache = os.path.join(self.root, 'cache')
        self.index = os.path.join(self.root, 'index')
        os.makedirs(self.cache)
        os.makedirs(self.index)
        env = mock.MagicMock()
        env.compiled_binary_cache_dir = self.cache
        env.cache_all_compiled_binaries = True
        mock.patch('dmoj.executors.compiled_executor.env', env).start()
        mock.patch('dmoj.executors.compiled_executor.tempfile.gettempdir', return_value=self.index).start()
        self.addCleanup(mock.patch.stopall)
        self.compiles = 0
        test = self

        class FakeCompiled(CompiledExecutor):
            """Like Java: `create_files` sets state the program needs at run time."""

            ext = 'fake'
            name = 'FAKE'

            def create_files(self, problem_id: str, source_code: bytes, *args, **kwargs) -> None:
                super().create_files(problem_id, source_code, *args, **kwargs)
                self._class_name = source_code.decode().split()[0]

            def compile(self) -> str:
                test.compiles += 1
                self._executable = self._file('program')
                with open(self._executable, 'wb') as f:
                    f.write(b'compiled ' + self.source)
                return self._executable

            def get_cmdline(self, **kwargs) -> List[str]:
                return [self._executable, self._class_name]

        self.executor_class = FakeCompiled

    def test_hit_restores_state_set_while_creating_files(self):
        first = self.executor_class('p', b'Main class')
        self.assertEqual(self.compiles, 1)
        second = self.executor_class('p', b'Main class')
        self.assertEqual(self.compiles, 1)  # a cache hit
        self.assertEqual(second._dir, first._dir)
        self.assertEqual(second.get_cmdline(), [first._executable, 'Main'])
        with open(second._executable, 'rb') as f:
            self.assertEqual(f.read(), b'compiled Main class')

    def test_hit_from_another_process_image(self):
        # Remote grading copies the cache directory and the index files to another VM, where nothing was built.
        self.executor_class('p', b'Main class')
        self.assertEqual(self.compiles, 1)
        restored = self.executor_class('p', b'Main class')
        self.assertEqual(self.compiles, 1)
        self.assertEqual(restored._class_name, 'Main')

    def test_different_source_compiles_again(self):
        self.executor_class('p', b'Main class')
        self.executor_class('p', b'Other class')
        self.assertEqual(self.compiles, 2)

    def test_missing_program_compiles_again(self):
        first = self.executor_class('p', b'Main class')
        os.remove(first._executable)
        self.executor_class('p', b'Main class')
        self.assertEqual(self.compiles, 2)

    def test_executor_without_an_executable_file_is_restored(self):
        # Java's "executable" is the empty string: the program is the class files in the directory.
        test = self

        class FakeJava(self.executor_class):
            def compile(self) -> str:
                test.compiles += 1
                with open(self._file('Main.class'), 'wb') as f:
                    f.write(b'class')
                self._executable = ''
                return self._executable

        first = FakeJava('p', b'Main class')
        second = FakeJava('p', b'Main class')
        self.assertEqual(self.compiles, 1)
        self.assertEqual(second._dir, first._dir)
        self.assertEqual(second._executable, '')
        self.assertTrue(os.path.isfile(os.path.join(second._dir, 'Main.class')))
