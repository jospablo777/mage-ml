"""
Checkpoints inside long blocks: `kwargs['checkpoint']` keeps a block's progress across the
attempts of one block run, so a retried attempt (after a crash, a restart, a timeout) or a
retried run resumes where the last one saved instead of starting over.

    checkpoint = kwargs['checkpoint']
    start = checkpoint.load('page', 0)
    for page in range(start, pages):
        write(fetch(page))
        checkpoint.save('page', page + 1)

Values are JSON (numbers, strings, lists, mappings), at most 1 MB each: progress such as an
offset, a cursor or the files done, not data. Each save is atomic (a temporary file, fsync,
then a rename), so an attempt killed mid-save leaves the value saved before. The
checkpoints of a block run are removed when it completes, so a new run starts fresh.

MageML's scheduler keeps them under the project's variables, by pipeline run and block;
exported services under the run's directory. Without a pipeline run (the editor) they last
for the one execution.

Standard library only: the services' worker imports it.
"""
import json
import os
import shutil
import tempfile
from typing import Any, Dict, Optional

MAX_BYTES = 1024 * 1024
# A key is a file name: letters, digits and a few separators.
_KEY_CHARACTERS = set('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_.')


class CheckpointError(ValueError):
    pass


def _check_key(key: str) -> str:
    if not isinstance(key, str) or not key or len(key) > 200 \
            or not set(key) <= _KEY_CHARACTERS or key.startswith('.'):
        raise CheckpointError(
            f'A checkpoint key is 1 to 200 letters, digits, -, _ or . (not first), not '
            f'{key!r}.'
        )
    return key


def block_folder(block_uuid: str) -> str:
    """A block's folder under a run's checkpoints, as the engine names it (/ as __)."""
    return block_uuid.replace('/', '__').replace('\\', '__')


class Checkpoint:
    """A block run's checkpoints in `directory`; in memory when it is None."""

    def __init__(self, directory: Optional[str], block: Optional[str] = None):
        self.directory = directory
        # The block it belongs to, so one block's checkpoints never reach another.
        self.block = block
        self._memory: Dict[str, str] = {}

    def __repr__(self) -> str:
        return f'Checkpoint({self.directory or "in memory"})'

    def _path(self, key: str) -> str:
        return os.path.join(self.directory, f'{_check_key(key)}.json')

    def load(self, key: str, default: Any = None) -> Any:
        """The value last saved under `key` in this block run, or `default`."""
        if self.directory is None:
            text = self._memory.get(_check_key(key))
        else:
            try:
                with open(self._path(key), encoding='utf-8') as file:
                    text = file.read()
            except FileNotFoundError:
                text = None
        return default if text is None else json.loads(text)

    def save(self, key: str, value: Any) -> None:
        """Keeps `value` under `key` for the next attempt of this block run."""
        try:
            text = json.dumps(value, allow_nan=False)
        except (TypeError, ValueError) as error:
            raise CheckpointError(
                f'Checkpoint {key!r} takes a JSON value (numbers, strings, lists, '
                f'mappings), such as an offset or the files done: {error}.'
            ) from None
        if len(text.encode('utf-8')) > MAX_BYTES:
            raise CheckpointError(
                f'Checkpoint {key!r} is over 1 MB: keep progress in it (an offset, a '
                'cursor), and the data in the block\'s output or a store.'
            )
        if self.directory is None:
            self._memory[_check_key(key)] = text
            return
        path = self._path(key)
        os.makedirs(self.directory, exist_ok=True)
        handle, temporary = tempfile.mkstemp(dir=self.directory, prefix='.', suffix='.tmp')
        try:
            with os.fdopen(handle, 'w', encoding='utf-8') as file:
                file.write(text)
                file.flush()
                os.fsync(file.fileno())
            os.replace(temporary, path)
        except BaseException:
            try:
                os.unlink(temporary)
            except OSError:
                pass
            raise
        _sync_directory(self.directory)

    def keys(self) -> list:
        if self.directory is None:
            return sorted(self._memory)
        try:
            names = os.listdir(self.directory)
        except FileNotFoundError:
            return []
        return sorted(n[:-5] for n in names if n.endswith('.json') and not n.startswith('.'))

    def clear(self) -> None:
        """Forgets every checkpoint of the block run: it completed."""
        self._memory.clear()
        if self.directory is not None:
            shutil.rmtree(self.directory, ignore_errors=True)


def _sync_directory(directory: str) -> None:
    # The rename is durable once the directory's entry is.
    try:
        handle = os.open(directory, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(handle)
    except OSError:
        pass
    finally:
        os.close(handle)
