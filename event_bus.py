import queue
import threading

class EventBus:
    def __init__(self):
        self._queue = queue.Queue()
        self._lock  = threading.Lock()

    def publish(self, event):
        self._queue.put(event)

    def consume(self, timeout=0.1):
        try:
            return self._queue.get(timeout=timeout)
        except queue.Empty:
            return None

    def has_events(self):
        return not self._queue.empty()

    def reset(self):
        while not self._queue.empty():
            try:
                self._queue.get_nowait()
            except queue.Empty:
                break

bus = EventBus()
