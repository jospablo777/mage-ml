from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Tuple

import psutil

from mage_ai.kernels.default.utils import (
    find_ipykernel_launchers_info,
    is_kernel_process_active,
    terminate_process,
)
from mage_ai.kernels.models import Kernel as KernelBase
from mage_ai.kernels.models import KernelProcess as KernelProcessBase
from mage_ai.kernels.models import request_usage
from mage_ai.server.active_kernel import interrupt_kernel, restart_kernel, start_kernel


def server_process_ids() -> set:
    """This server's process and the processes it started, which start its kernels."""
    ids = {os.getpid()}
    try:
        ids.update(child.pid for child in psutil.Process().children(recursive=True))
    except psutil.Error:
        pass
    return ids


@dataclass
class KernelProcess(KernelProcessBase):
    @classmethod
    def load_all(cls, check_active_status: bool = False) -> Sequence['KernelProcess']:
        return [
            cls.load(**d)
            for d in find_ipykernel_launchers_info(check_active_status=check_active_status)
        ]

    @classmethod
    def terminate_inactive(
        cls,
        process_dicts: Optional[List[Dict]] = None,
    ) -> Tuple[int, int]:
        print('Terminating inactive kernels...')
        memory_usage = []

        if process_dicts:
            arr = [cls.load(**d) for d in process_dicts]
        else:
            arr = cls.load_all(check_active_status=True)
        if len(arr) >= 2:
            ours = server_process_ids()
            for kernel_process in arr:
                # Kernels of other programs, such as an editor's notebooks, are not ours to
                # stop.
                if not kernel_process.active and kernel_process.ppid in ours:
                    print(f'Terminating process {kernel_process.pid}...')
                    if kernel_process.terminate():
                        memory_usage.append(kernel_process.memory)

        if len(memory_usage) == 0:
            print('No inactive kernels found.')
            return 0, 0

        print(
            f'{sum(memory_usage)} bytes of memory freed '
            f'from {len(memory_usage)} inactive kernel(s).'
        )
        return len(memory_usage), sum(memory_usage)

    def check_active_status(self) -> bool:
        if not self.pid or not self.connection_file:
            return False

        self.active = is_kernel_process_active(self.pid, self.connection_file) or False
        return self.active

    def terminate(self) -> bool:
        return self.pid is not None and terminate_process(self.pid)


class KernelWrapper(KernelBase):
    def __init__(self, kernel):
        self.kernel = kernel
        self.usage = None
        self.active_kernels = None
        self.inactive_kernels = None

    async def prepare_usage(self):
        self.usage = await request_usage(self.kernel)

    def is_ready(self) -> bool:
        return self.usage is not None

    def is_alive(self) -> bool:
        return self.kernel.is_alive()

    @property
    def kernel_id(self) -> str:
        return self.kernel.kernel_id

    @property
    def kernel_name(self) -> str:
        return self.kernel.kernel_name

    def interrupt(self):
        interrupt_kernel()
        return True

    def restart(self):
        restart_kernel()
        return True

    def start(self):
        start_kernel()
        return True
