import asyncio
import os
import sys
import tempfile
import unittest
from pathlib import Path

from mage_ai.cache.mem_lru import (
    MemorySizeLRUCache,
    file_cache,
    read_yaml_file,
    read_yaml_file_async,
)


class FileCacheTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = Path(directory.name) / 'metadata.yaml'
        self.addCleanup(file_cache.pop, self.path, None)

    def readers(self):
        for read in (read_yaml_file, lambda path: asyncio.run(read_yaml_file_async(path))):
            file_cache.pop(self.path, None)
            yield read

    def test_reload_when_mtime_moves_backwards(self):
        for read in self.readers():
            with self.subTest(reader=read):
                self.path.write_text('version: old\n')
                self.assertEqual(read(self.path), {'version': 'old'})
                timestamp = self.path.stat().st_mtime_ns - 60_000_000_000
                self.path.write_text('version: new\n')
                os.utime(self.path, ns=(timestamp, timestamp))
                self.assertEqual(read(self.path), {'version': 'new'})

    def test_reload_after_replacement_with_same_mtime_and_size(self):
        for read in self.readers():
            with self.subTest(reader=read):
                self.path.write_text('version: old\n')
                timestamp = self.path.stat().st_mtime_ns
                self.assertEqual(read(self.path), {'version': 'old'})
                replacement = self.path.with_suffix('.tmp')
                replacement.write_text('version: new\n')
                os.utime(replacement, ns=(timestamp, timestamp))
                replacement.replace(self.path)
                self.assertEqual(read(self.path), {'version': 'new'})

    def test_reload_after_submicrosecond_timestamp_change(self):
        for read in self.readers():
            with self.subTest(reader=read):
                self.path.write_text('version: old\n')
                timestamp = 1_780_000_000_000_000_100
                os.utime(self.path, ns=(timestamp, timestamp))
                self.assertEqual(read(self.path), {'version': 'old'})
                self.path.write_text('version: new\n')
                os.utime(self.path, ns=(timestamp + 100, timestamp + 100))
                self.assertEqual(read(self.path), {'version': 'new'})

    def test_missing_file_is_evicted(self):
        for read in self.readers():
            with self.subTest(reader=read):
                self.path.write_text('version: old\n')
                read(self.path)
                self.path.unlink()
                self.assertIsNone(read(self.path))
                self.assertNotIn(self.path, file_cache)

    def test_cached_values_are_not_shared_with_callers(self):
        self.path.write_text('blocks: [loader]\n')
        first = read_yaml_file(self.path)
        first['blocks'].append('exporter')
        self.assertEqual(read_yaml_file(self.path), {'blocks': ['loader']})


class MemorySizeLRUCacheTest(unittest.TestCase):
    def test_replacement_does_not_accumulate_memory(self):
        value = {'content': 'value'}
        size = sys.getsizeof(value['content'])
        cache = MemorySizeLRUCache(max_memory_size=size * 2)
        cache['first'] = value
        cache['second'] = value
        for _ in range(5):
            cache['second'] = value
            self.assertEqual(cache.current_memory_size, size * 2)
            self.assertIn('first', cache)
        cache.clear()
        self.assertEqual(cache.current_memory_size, 0)

    def test_memory_eviction_removes_least_recently_used_entry(self):
        value = {'content': 'value'}
        size = sys.getsizeof(value['content'])
        cache = MemorySizeLRUCache(max_memory_size=size * 2)
        cache['first'] = value
        cache['second'] = value
        cache['first']
        cache['third'] = value
        self.assertNotIn('second', cache)
        self.assertEqual(cache.current_memory_size, size * 2)
