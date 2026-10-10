import json
import os
import shutil
import uuid
from contextlib import contextmanager
from typing import Dict, Iterator, List, Optional, Tuple, Union

import aiofiles
import pandas as pd
import polars as pl
import simplejson

from mage_ai.data_preparation.storage.base_storage import (
    BaseStorage,
    read_pandas_parquet,
    write_pandas_parquet,
)
from mage_ai.settings.server import DEBUG_FILE_IO
from mage_ai.shared.environments import is_debug
from mage_ai.shared.parsers import encode_complex


class LocalStorage(BaseStorage):
    @contextmanager
    def writing(self, file_path: str) -> Iterator[str]:
        """
        A temporary path next to file_path, moved over file_path when the block exits
        without an error. Readers see the old file or the new one, never a partial file
        left by a failed or killed write.
        """
        dirname = os.path.dirname(file_path)
        if dirname:
            os.makedirs(dirname, exist_ok=True)
        temporary = os.path.join(
            dirname, f'.{os.path.basename(file_path)}.{uuid.uuid4().hex[:8]}.tmp',
        )
        try:
            yield temporary
            os.replace(temporary, file_path)
        finally:
            if os.path.isfile(temporary):
                os.remove(temporary)

    def isdir(self, path: str) -> bool:
        return os.path.isdir(path)

    def listdir(
        self,
        path: str,
        suffix: str = None,
        max_results: int = None,
    ) -> List[str]:
        paths = []
        if not os.path.exists(path) or not os.path.isdir(path):
            return paths

        if max_results is not None:
            with os.scandir(path) as it:
                for idx, entry in enumerate(it):
                    paths.append(entry.name)
                    if idx >= max_results - 1:
                        break
        else:
            paths = os.listdir(path)
        if suffix is not None:
            paths = [p for p in paths if p.endswith(suffix)]

        return paths

    def makedirs(self, path: str, **kwargs) -> None:
        os.makedirs(path, exist_ok=True)

    def path_exists(self, path: str) -> bool:
        return os.path.exists(path)

    def remove(self, path: str) -> None:
        os.remove(path)

    def remove_dir(self, path: str) -> None:
        shutil.rmtree(path, ignore_errors=True)

    def read_json_file(
        self,
        file_path: str,
        default_value: Optional[Union[Dict, List]] = None,
        raise_exception: bool = False,
    ) -> Dict:
        if DEBUG_FILE_IO and '.variables' in file_path:
            print(f'[READ JSON FILE]: {file_path}')
        if not self.path_exists(file_path):
            if raise_exception:
                # A missing file used to return the default even when the caller asked
                # for an error, so a deleted block output reached the next block as {}.
                raise FileNotFoundError(file_path)
            return {} if default_value is None else default_value
        with open(file_path) as file:
            try:
                return json.load(file)
            except Exception:
                if raise_exception:
                    raise
                return {} if default_value is None else default_value

    async def read_json_file_async(
        self,
        file_path: str,
        default_value: Dict = None,
        raise_exception: bool = False,
    ) -> Dict:
        if not self.path_exists(file_path):
            if raise_exception:
                raise FileNotFoundError(file_path)
            return {} if default_value is None else default_value
        async with aiofiles.open(file_path, mode='r') as file:
            try:
                return json.loads(await file.read())
            except Exception:
                if raise_exception:
                    raise
                return {} if default_value is None else default_value

    def write_json_file(self, file_path: str, data) -> None:
        dirname = os.path.dirname(file_path)
        if not os.path.isdir(dirname):
            os.makedirs(dirname, exist_ok=True)

        # Serialize before opening the file. Dumping straight into the handle left a
        # truncated file behind when encoding failed, and the next read of that
        # variable failed with a JSON decode error far from the cause.
        try:
            contents = simplejson.dumps(
                data,
                default=encode_complex,
                ignore_nan=True,
            )
        except (TypeError, ValueError) as err:
            raise ValueError(
                f'Cannot write {file_path}: a value of type {type(data).__name__} is '
                'not JSON serializable. Store it as a dataframe, a model or another '
                'supported variable type.'
            ) from err

        with self.writing(file_path) as temporary:
            with open(temporary, 'w') as file:
                file.write(contents)

    async def write_json_file_async(self, file_path: str, data) -> None:
        # Same ordering as write_json_file: opening the file truncates it, so encode
        # first and never leave a partial file behind.
        fcontent = simplejson.dumps(
            data,
            default=encode_complex,
            ignore_nan=True,
        )

        with self.writing(file_path) as temporary:
            async with aiofiles.open(temporary, mode='w') as file:
                await file.write(fcontent)

    def read_parquet(self, file_path: str, **kwargs) -> pd.DataFrame:
        return read_pandas_parquet(file_path, **kwargs)

    def read_polars_parquet(self, file_path: str, **kwargs) -> pl.DataFrame:
        return pl.read_parquet(file_path, **kwargs)

    def write_csv(self, df: pd.DataFrame, file_path: str) -> None:
        with self.writing(file_path) as temporary:
            df.to_csv(temporary, index=False)

    def write_parquet(self, df: pd.DataFrame, file_path: str) -> None:
        with self.writing(file_path) as temporary:
            write_pandas_parquet(df, temporary)

    def write_polars_dataframe(self, df: pl.DataFrame, file_path: str) -> None:
        with self.writing(file_path) as temporary:
            df.write_parquet(temporary)

    @contextmanager
    def open_to_write(
        self,
        file_path: str,
        append: bool = False,
    ) -> None:
        if append:
            dirname = os.path.dirname(file_path)
            if not os.path.isdir(dirname):
                os.mkdir(dirname)
            with open(file_path, 'a') as file:
                yield file
            return

        with self.writing(file_path) as temporary:
            with open(temporary, 'w') as file:
                yield file

    def polars_location(self, path: str) -> Tuple[str, None]:
        return path, None

    async def read_async(self, file_path: str) -> str:
        dirname = os.path.dirname(file_path)
        if not os.path.isdir(dirname):
            os.mkdir(dirname)

        async with aiofiles.open(file_path, mode='r') as file:
            try:
                return await file.read()
            except Exception as err:
                if is_debug():
                    print(f'[ERROR] LocalStorage.read_async: {err}')

    def read(self, file_path: str) -> str:
        dirname = os.path.dirname(file_path)
        if not os.path.isdir(dirname):
            os.mkdir(dirname)

        with open(file_path, mode='r') as file:
            try:
                return file.read()
            except Exception as err:
                if is_debug():
                    print(f'[ERROR] LocalStorage.read: {err}')
