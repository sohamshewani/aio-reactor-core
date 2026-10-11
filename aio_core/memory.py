import math
import sys
import threading
import time
from typing import Optional, Tuple, List, Any

class MemoryError(Exception):
    """Raised when memory allocation or buffer slicing violates safety boundaries."""
    pass

class TimerWheelError(Exception):
    """Raised when timer wheel scheduling or cancellation fails."""
    pass

class BufferPool:
    """
    Manages contiguous memory blocks using bytearray and memoryview to 
    eliminate dynamic allocation overhead during hot-path I/O.
    Provides zero-copy packet slicing.
    """
    def __init__(self, block_size: int = 65536, pool_capacity: int = 1024) -> None:
        self.block_size = int(block_size)
        self.pool_capacity = int(pool_capacity)
        self._pool: List[bytearray] = [bytearray(self.block_size) for _ in range(self.pool_capacity)]
        self._available: List[int] = list(range(self.pool_capacity))
        self._lock = threading.Lock()
        self._allocated_count = 0

    def acquire(self) -> memoryview:
        with self._lock:
            if not self._available:
                # Fallback allocation if pool is exhausted, though violates amortization if frequent
                buf = bytearray(self.block_size)
                self._allocated_count += 1
                return memoryview(buf)
            idx = self._available.pop()
            self._allocated_count += 1
            return memoryview(self._pool[idx])

    def release(self, view: memoryview) -> None:
        if view is None:
            return
        # Release releases the underlying object back if it matches pool configuration
        # For strict zero-copy, we release the reference back to available trackers if owned.
        # In this implementation, we release by tracking the base object.
        obj = view.obj
        if isinstance(obj, bytearray) and len(obj) == self.block_size:
            with self._lock:
                # Find index or maintain references. To avoid O(N) lookup, we use object identity mapping 
                # or simple structural check. Since blocks are fixed instances in self._pool:
                try:
                    # O(N) worst case if searching pool, optimize via identity check if desired
                    for i, block in enumerate(self._pool):
                        if block is obj:
                            if i not in self._available:
                                self._available.append(i)
                                self._allocated_count = max(0, self._allocated_count - 1)
                            break
                except Exception:
                    pass

    def slice_buffer(self, view: memoryview, start: int, end: int) -> memoryview:
        if start < 0 or end > len(view) or start > end:
            raise MemoryError(f"Invalid slice bounds: start={start}, end={end}, len={len(view)}")
        return view[start:end]

    @property
    f_allocated(self) -> int:
        with self._lock:
            return self._allocated_count


class TimerTask:
    __slots__ = ('expires_at', 'callback', 'args', 'kwargs', 'wheel_index', 'slot_index', 'next', 'prev', 'cancelled')

    def __init__(self, expires_at: int, callback: Any, args: tuple, kwargs: dict) -> None:
        self.expires_at = int(expires_at)
        self.callback = callback
        self.args = args
        self.kwargs = kwargs
        self.wheel_index = -1
        self.slot_index = -1
        self.next: Optional['TimerTask'] = None
        self.prev: Optional['TimerTask'] = None
        self.cancelled = False


class TimerWheel:
    """
    O(1) Hierarchical Timer Wheels inspired by Varghese and Lauck.
    Utilizes multi-level power-of-two cyclic arrays for deterministic timeout scheduling and cancellation.
    """
    def __init__(self, resolution_ms: int = 10, levels: int = 4, slots_per_level: int = 256) -> None:
        self.resolution_ms = int(resolution_ms)
        self.levels = int(levels)
        self.slots_per_level = int(slots_per_level)
        
        # Calculate intervals per level (powers of two)
        self.intervals = [self.resolution_ms * (self.slots_per_level ** i) for i in range(self.levels)]
        
        # Initialize wheels: list of lists of doubly linked list heads (represented as sentinel nodes)
        self.wheels: List[List[Optional[TimerTask]]] = [
            [None for _ in range(self.slots_per_level)] for _ in range(self.levels)
        ]
        self.current_ticks = [0 for _ in range(self.levels)]
        self._lock = threading.Lock()
        self._current_time_ms = int(time.time() * 1000)

    def schedule(self, delay_ms: int, callback: Any, *args: Any, **kwargs: Any) -> TimerTask:
        with self._lock:
            expires_at = self._current_time_ms + max(0, int(delay_ms))
            task = TimerTask(expires_at, callback, args, kwargs)
            self._add_task(task)
            return task

    def cancel(self, task: TimerTask) -> bool:
        with self._lock:
            if task.cancelled:
                return False
            task.cancelled = True
            self._remove_task(task)
            return True

    def _add_task(self, task: TimerTask) -> None:
        delta = task.expires_at - self._current_time_ms
        if delta < 0:
            delta = 0

        # Determine appropriate wheel level based on delta
        level = 0
        for i in range(self.levels - 1):
            if delta >= self.intervals[i + 1]:
                level = i + 1
            else:
                break

        # If delta exceeds maximum range of highest wheel, cap it to the last wheel's max capacity
        if level >= self.levels:
            level = self.levels - 1

        interval = self.intervals[level]
        ticks = delta // interval
        slot = (self.current_ticks[level] + ticks) % self.slots_per_level

        task.wheel_index = level
        task.slot_index = slot

        # Insert into doubly linked list at wheels[level][slot]
        head = self.wheels[level][slot]
        if head is None:
            self.wheels[level][slot] = task
            task.next = None
            task.prev = None
        else:
            task.next = head
            task.prev = None
            head.prev = task
            self.wheels[level][slot] = task

    def _remove_task(self, task: TimerTask) -> None:
        level = task.wheel_index
        slot = task.slot_index
        if level < 0 or slot < 0:
            return

        if task.prev is not None:
            task.prev.next = task.next
        else:
            # It was the head of the slot list
            if self.wheels[level][slot] is task:
                self.wheels[level][slot] = task.next

        if task.next is not None:
            task.next.prev = task.prev

        task.next = None
        task.prev = None
        task.wheel_index = -1
        task.slot_index = -1

    def tick(self, current_time_ms: int) -> List[TimerTask]:
        with self._lock:
            self._current_time_ms = int(current_time_ms)
            expired_tasks: List[TimerTask] = []

            # Advance level 0
            level = 0
            self.current_ticks[level] = (self.current_ticks[level] + 1) % self.slots_per_level
            slot = self.current_ticks[level]

            head = self.wheels[level][slot]
            self.wheels[level][slot] = None

            current = head
            while current is not None:
                nxt = current.next
                current.next = None
                current.prev = None
                if not current.cancelled:
                    if current.expires_at <= self._current_time_ms:
                        expired_tasks.append(current)
                    else:
                        # Re-hash if it landed early due to granularity
                        self._add_task(current)
                current = nxt

            # Cascading ticks for higher levels
            for l in range(1, self.levels):
                if self.current_ticks[l - 1] == 0:
                    self.current_ticks[l] = (self.current_ticks[l] + 1) % self.slots_per_level
                    l_slot = self.current_ticks[l]
                    l_head = self.wheels[l][l_slot]
                    self.wheels[l][l_slot] = None

                    curr = l_head
                    while curr is not None:
                        nxt_curr = curr.next
                        curr.next = None
                        curr.prev = None
                        if not curr.cancelled:
                            self._add_task(curr)
                        curr = nxt_curr

            return expired_tasks


class SPSCQueue:
    """
    Lock-free Single-Producer Single-Consumer (SPSC) ring buffer 
    using atomic indices and bounded spinlocks with exponential backoff.
    """
    def __init__(self, capacity: int = 4096) -> None:
        # Capacity must be a power of two for efficient bitwise masking
        if not (capacity > 0 and (capacity & (capacity - 1)) == 0):
            # Round up to next power of two
            capacity = 1 << (capacity - 1).bit_length()
        self.capacity = capacity
        self.mask = capacity - 1
        self.buffer: List[Any] = [None] * capacity
        self._head = 0
        self._tail = 0
        self._lock = threading.Lock()

    def push(self, item: Any) -> bool:
        backoff = 1
        while True:
            head = self._head
            tail = self._tail
            if ((tail - head) & self.mask) == self.capacity - 1:
                # Queue is full, use bounded spinlock with exponential backoff
                if backoff > 64:
                    with self._lock:
                        head = self._head
                        tail = self._tail
                        if ((tail - head) & self.mask) == self.capacity - 1:
                            return False
                        self.buffer[tail & self.mask] = item
                        self._tail = (tail + 1) & 0xFFFFFFFF
                        return True
                for _ in range(backoff):
                    pass
                backoff <<= 1
                continue

            self.buffer[tail & self.mask] = item
            self._tail = (tail + 1) & 0xFFFFFFFF
            return True

    def pop(self) -> Tuple[bool, Any]:
        backoff = 1
        while True:
            head = self._head
            tail = self._tail
            if head == tail:
                # Queue is empty
                if backoff > 64:
                    with self._lock:
                        head = self._head
                        tail = self._tail
                        if head == tail:
                            return False, None
                        item = self.buffer[head & self.mask]
                        self.buffer[head & self.mask] = None
                        self._head = (head + 1) & 0xFFFFFFFF
                        return True, item
                for _ in range(backoff):
                    pass
                backoff <<= 1
                continue

            item = self.buffer[head & self.mask]
            self.buffer[head & self.mask] = None
            self._head = (head + 1) & 0xFFFFFFFF
            return True, item

    @property
    def size(self) -> int:
        head = self._head
        tail = self._tail
        return (tail - head) & self.mask


class IntrusiveNode:
    __slots__ = ('next', 'prev', 'data')

    def __init__(self, data: Any = None) -> None:
        self.next: Optional['IntrusiveNode'] = None
        self.prev: Optional['IntrusiveNode'] = None
        self.data = data


class IntrusiveLinkedList:
    """
    Maintains an intrusive doubly-linked list for connection tracking 
    within multiplexers, ensuring zero-allocation state mutations.
    """
    def __init__(self) -> None:
        self.head: Optional[IntrusiveNode] = None
        self.tail: Optional[IntrusiveNode] = None
        self._size = 0
        self._lock = threading.Lock()

    def append(self, node: IntrusiveNode) -> None:
        with self._lock:
            node.next = None
            node.prev = self.tail
            if self.tail is not None:
                self.tail.next = node
            else:
                self.head = node
            self.tail = node
            self._size += 1

    def remove(self, node: IntrusiveNode) -> None:
        with self._lock:
            if node.prev is not None:
                node.prev.next = node.next
            else:
                self.head = node.next

            if node.next is not None:
                node.next.prev = node.prev
            else:
                self.tail = node.prev

            node.next = None
            node.prev = None
            self._size = max(0, self._size - 1)

    @property
    def size(self) -> int:
        with self._lock:
            return self._size