import multiprocessing
import queue

from mage_ai.settings.server import KERNEL_MAGIC

EmptyModule = queue.Empty
QueueModule = multiprocessing.Queue

try:
    if KERNEL_MAGIC:
        import faster_fifo

        EmptyModule = faster_fifo.Empty
        QueueModule = faster_fifo.Queue
except ImportError:
    pass


Empty = EmptyModule
Queue = QueueModule
