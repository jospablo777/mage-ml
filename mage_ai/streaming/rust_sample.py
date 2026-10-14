"""
Runs a stream's Rust blocks in the editor, on samples and in Execute pipeline. Each block's
program (its
stream main, mage::serve_stream) is built as MageML builds Rust blocks, cached by its code,
and gets the messages over the protocol the service engine uses: a Unix socket, frames of a
little-endian u32 length and bytes (see the block SDK's stream module).
"""
import json
import os
import socket
import struct
import subprocess
import tempfile
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

PROTOCOL = 1
START_SECONDS = 30


class RustSampleError(Exception):
    pass


def encode(message: Any) -> bytes:
    """
    A message as the bytes a broker would carry: text and bytes as they are, a RabbitMQ
    Payload as its body, others as JSON.
    """
    if isinstance(message, (bytes, bytearray)):
        return bytes(message)
    if isinstance(message, str):
        return message.encode('utf-8')
    body = getattr(message, 'body', None)
    if isinstance(body, (bytes, bytearray)):
        return bytes(body)
    return json.dumps(message, default=str).encode('utf-8')


def decode(data: bytes) -> Any:
    try:
        return json.loads(data)
    except ValueError:
        return data.decode('utf-8', errors='replace')


class RustStreamProgram:
    """One block's program, running while the sample runs."""

    def __init__(self, binary: Path, block_uuid: str, pipeline_uuid: str, variables: Dict,
                 cwd: str, env: Dict[str, str], timeout: float):
        self.timeout = timeout
        self.lines: List[str] = []
        self.directory = tempfile.mkdtemp(prefix='mage-rust-sample-')
        # A short path: Unix socket paths hold about 100 bytes.
        path = os.path.join(tempfile.gettempdir(), f'mage-s-{uuid.uuid4().hex[:12]}.sock')
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        listener.bind(path)
        listener.listen(1)
        listener.settimeout(START_SECONDS)
        Path(self.directory, 'stream.json').write_text(json.dumps(dict(
            api_version=1,
            socket=path,
            pipeline_uuid=pipeline_uuid,
            variables=variables,
            chain=[dict(block_uuid=block_uuid, block_type='transformer', entry='block',
                        send=True)],
        ), default=str))
        self.process = subprocess.Popen(
            [str(binary), self.directory], cwd=cwd, env=env, stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, errors='replace',
            start_new_session=True,
        )
        threading.Thread(target=self._read_output, daemon=True).start()
        try:
            self.connection, _ = listener.accept()
        except socket.timeout:
            self.close()
            raise RustSampleError(self._stopped(f'did not connect within {START_SECONDS} s'))
        finally:
            listener.close()
            os.unlink(path)
        self.connection.settimeout(timeout)
        ready = json.loads(self._read_frame() or b'{}')
        if ready.get('protocol') != PROTOCOL:
            self.close()
            raise RustSampleError(
                f'The block speaks stream protocol {ready.get("protocol")}, this MageML '
                f'{PROTOCOL}; run it again to rebuild it.',
            )

    def _read_output(self) -> None:
        for line in self.process.stdout:
            self.lines.append(line.rstrip('\n'))

    def _stopped(self, what: str) -> str:
        output = '\n'.join(self.lines[-30:])
        return f'The Rust block {what}.\n{output}'.rstrip()

    def _read_exact(self, size: int) -> Optional[bytes]:
        chunks, left = [], size
        while left:
            chunk = self.connection.recv(min(left, 1 << 20))
            if not chunk:
                return None
            chunks.append(chunk)
            left -= len(chunk)
        return b''.join(chunks)

    def _read_frame(self) -> Optional[bytes]:
        head = self._read_exact(4)
        if head is None:
            return None
        return self._read_exact(struct.unpack('<I', head)[0])

    def run(self, messages: List[bytes]) -> Dict:
        """The block's reply to one batch, with its messages (`messages`) as bytes."""
        header = json.dumps(dict(id=1, count=len(messages))).encode()
        frames = [struct.pack('<I', len(header)), header]
        for message in messages:
            frames += [struct.pack('<I', len(message)), message]
        try:
            self.connection.sendall(b''.join(frames))
            reply_bytes = self._read_frame()
            if reply_bytes is None:
                raise RustSampleError(self._stopped('stopped'))
            reply = json.loads(reply_bytes)
            reply['messages'] = []
            for output in reply.get('outputs') or []:
                for _ in range(output['count']):
                    frame = self._read_frame()
                    if frame is None:
                        raise RustSampleError(self._stopped('stopped in its reply'))
                    reply['messages'].append(frame)
        except socket.timeout:
            self.close()
            raise RustSampleError(
                f'The blocks ran past {int(self.timeout)} seconds; the block was stopped.',
            )
        except OSError as error:
            self.close()
            raise RustSampleError(self._stopped(f'stopped ({error})'))
        # What the block printed arrives on another pipe; let the last lines land.
        time.sleep(0.05)
        return reply

    def take_lines(self) -> List[str]:
        lines, self.lines = self.lines, []
        return lines

    def close(self) -> None:
        connection = getattr(self, 'connection', None)
        if connection is not None:
            connection.close()
        if self.process.poll() is None:
            try:
                self.process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                os.killpg(self.process.pid, 9)
                self.process.wait(timeout=10)


def build_stream_program(block, repo_path: str) -> Path:
    """The block's program, from the binary cache or built now."""
    from mage_ai.data_preparation.models.block.rust import build as rust_build

    prepared = rust_build.prepare(
        Path(block.file_path).read_text(), block.type, block.uuid, repo_path, stream=True,
    )
    try:
        return rust_build.build(prepared, stream=False).binary
    except rust_build.RustBuildError as error:
        located = [
            f'line {d.line}: {d.message}' if d.line else d.message
            for d in error.diagnostics if d.level == 'error'
        ]
        raise RustSampleError('\n'.join([str(error), *located[:10]]))


def program_env(block) -> Dict[str, str]:
    from mage_ai.data_preparation.models.block.rust import (
        _process_env,
        block_io_config,
    )

    env = _process_env()
    io_config = block_io_config(block)
    if io_config:
        env['MAGE_IO_CONFIG'] = json.dumps(io_config, default=str)
    return env
