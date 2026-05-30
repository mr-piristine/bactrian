import queue
import threading
import time
from typing import NamedTuple
from .core import EventEngine, Exchange

class QueuedEvent(NamedTuple):
    """Encapsulates a queued work item."""
    event_type: str
    exchange: Exchange

class QueuedBactrianEngine:
    """
    Wraps the core Bactrian EventEngine to process incoming events asynchronously 
    using a background thread and a thread-safe Queue buffer.
    """
    def __init__(self, max_queue_size: int = 0) -> None:
        self.core_engine = EventEngine()
        # 1. Initialize the thread-safe shared FIFO buffer
        self._event_queue: queue.Queue = queue.Queue(maxsize=max_queue_size)
        self._worker_thread: threading.Thread | None = None
        self._running: bool = False

    def start(self) -> None:
        """Spins up the background consumer thread."""
        if self._running:
            return
        
        self._running = True
        self._worker_thread = threading.Thread(target=self._consumer_loop, daemon=True)
        self._worker_thread.start()
        print("[Queued Engine] Background consumer thread initialized and listening...")

    def publish(self, event_type: str, exchange: Exchange) -> None:
        """
        Non-blocking execution step. Dropping an event here puts it in the buffer 
        and immediately yields execution back to the caller (e.g. your market feed socket).
        """
        if not self._running:
            raise RuntimeError("Engine is stopped. Call start() before publishing events.")
            
        event_item = QueuedEvent(event_type=event_type, exchange=exchange)
        self._event_queue.put(event_item)  # Blocks only if maxsize is hit (Backpressure)

    def _consumer_loop(self) -> None:
        """The infinite background execution loop handling sequential task extractions."""
        while self._running:
            try:
                # Blocks until an item is available, checks every 1 second for shutdown flags
                event_item = self._event_queue.get(timeout=1.0)
            except queue.Empty:
                continue

            # Poison Pill tracking pattern for clean disengagements
            if event_item is None:
                self._event_queue.task_done()
                break

            # Execute the core Bactrian pipeline safely on the worker thread
            try:
                self.core_engine.route(event_item.event_type, event_item.exchange)
            except Exception as e:
                print(f"[Queued Engine] [Error] Failed executing pipeline payload: {e}")
            finally:
                # Mark item task complete to clear queue space
                self._event_queue.task_done()

        print("[Queued Engine] Background consumer thread stopped safely.")

    def stop(self) -> None:
        """Gracefully unblocks the system and waits for queue finalization."""
        if not self._running:
            return
            
        print("\n[Queued Engine] Shutting down framework pipeline...")
        self._running = False
        
        # Inject Poison Pill to unblock get() if queue is empty
        self._event_queue.put(None)
        
        # Block main thread until worker flushes pending items
        if self._worker_thread:
            self._worker_thread.join()