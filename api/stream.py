"""Stdout capture and SSE helpers for streaming scan/profile progress."""

import io
import json
import sys
import threading
from queue import Queue
from typing import Any, Callable, Generator

# Prevents two long-running operations from stomping each other's stdout capture.
# Single-user local tool — returning 429 is preferable to corrupted output.
_op_lock = threading.Lock()

_SENTINEL = object()


class _LineBufferedQueue(io.TextIOBase):
    """Buffers writes into complete lines before putting them on a queue.

    Handles click's nl=False pattern: partial writes are held until a newline
    arrives, then emitted as a single complete line.
    """

    def __init__(self, queue: Queue) -> None:
        self._queue = queue
        self._buf = ""

    def write(self, text: str) -> int:
        self._buf += text
        while "\n" in self._buf:
            line, self._buf = self._buf.split("\n", 1)
            if line.strip():
                self._queue.put(line)
        return len(text)

    def flush(self) -> None:
        if self._buf.strip():
            self._queue.put(self._buf)
            self._buf = ""


def sse(data: dict) -> str:
    return f"data: {json.dumps(data)}\n\n"


def stream_operation(
    fn: Callable[[], Any],
) -> Generator[str, None, None]:
    """Run fn in a background thread, stream stdout lines as SSE progress events.

    Yields:
        SSE events with shape:
          {"type": "progress", "msg": str}   — one per output line
          {"type": "done",     "result": ...} — fn's return value on success
          {"type": "error",    "msg": str}   — on exception
    """
    if not _op_lock.acquire(blocking=False):
        yield sse({"type": "error", "msg": "Another scan or profile is already running. Please wait."})
        return

    queue: Queue = Queue()

    def worker() -> None:
        try:
            old_stdout = sys.stdout
            sys.stdout = _LineBufferedQueue(queue)
            try:
                result = fn()
            finally:
                writer = sys.stdout
                sys.stdout = old_stdout
                writer.flush()  # type: ignore[attr-defined]
            queue.put((_SENTINEL, None, result))
        except Exception as exc:
            queue.put((_SENTINEL, exc, None))
        finally:
            _op_lock.release()

    threading.Thread(target=worker, daemon=True).start()

    while True:
        item = queue.get()
        if isinstance(item, tuple) and len(item) == 3 and item[0] is _SENTINEL:
            _, exc, result = item
            if exc is not None:
                yield sse({"type": "error", "msg": str(exc)})
            else:
                yield sse({"type": "done", "result": result})
            break
        else:
            yield sse({"type": "progress", "msg": str(item)})
