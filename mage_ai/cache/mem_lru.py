import copy
import os
import sys
import traceback
from functools import wraps

import aiofiles
from cachetools import LRUCache

from mage_ai.shared.yaml import load_yaml


class MemorySizeLRUCache(LRUCache):
    def __init__(
        self,
        maxsize: int = 1000,
        max_memory_size: int = 100 * 1024 * 1024,
    ):
        super().__init__(maxsize=maxsize)
        self.max_memory_size = max_memory_size
        self.current_memory_size = 0

    def __setitem__(self, key, value):
        size = self._get_size(value)
        if size > self.max_memory_size:
            raise ValueError('Item size exceeds cache maximum memory size')

        if key in self:
            del self[key]

        while self.current_memory_size + size > self.max_memory_size:
            self.popitem()

        super().__setitem__(key, value)
        self.current_memory_size += size

    def __delitem__(self, key):
        value = self[key]
        self.current_memory_size -= self._get_size(value)
        super().__delitem__(key)

    def clear(self):
        super().clear()
        self.current_memory_size = 0

    def _get_size(self, value):
        return sys.getsizeof(value['content'])


def file_version(file_path):
    info = os.stat(file_path)
    return info.st_dev, info.st_ino, info.st_mtime_ns, info.st_ctime_ns, info.st_size


def cache_file_read(cache):
    def decorator(func):
        @wraps(func)
        def wrapper(file_path, *args, **kwargs):
            try:
                version = file_version(file_path)
            except FileNotFoundError:
                cache.pop(file_path, None)
                return None

            if file_path in cache:
                cache_entry = cache[file_path]
                if cache_entry.get('version') == version:
                    return copy.deepcopy(cache_entry['content'])

            try:
                content = func(file_path, *args, **kwargs)
                if file_version(file_path) == version:
                    cache[file_path] = {'content': content, 'version': version}
                return copy.deepcopy(content)
            except Exception:
                traceback.print_exc()
                return None
        return wrapper
    return decorator


def async_cache_file_read(cache):
    def decorator(func):
        @wraps(func)
        async def wrapper(file_path, *args, **kwargs):
            try:
                version = file_version(file_path)
            except FileNotFoundError:
                cache.pop(file_path, None)
                return None

            if file_path in cache:
                cache_entry = cache[file_path]
                if cache_entry.get('version') == version:
                    return copy.deepcopy(cache_entry['content'])

            try:
                content = await func(file_path, *args, **kwargs)
                if file_version(file_path) == version:
                    cache[file_path] = {'content': content, 'version': version}
                return copy.deepcopy(content)
            except Exception:
                traceback.print_exc()
                return None
        return wrapper
    return decorator


file_cache = MemorySizeLRUCache()


@cache_file_read(file_cache)
def read_yaml_file(file_path, mode: str = 'r', **kwargs):
    with open(file_path, mode, **kwargs) as file:
        return load_yaml(file.read()) or {}


@async_cache_file_read(file_cache)
async def read_yaml_file_async(file_path, mode: str = 'r', **kwargs):
    async with aiofiles.open(file_path, mode, **kwargs) as file:
        return load_yaml(await file.read()) or {}
