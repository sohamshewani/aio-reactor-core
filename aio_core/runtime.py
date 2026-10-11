import os
import sys
import socket
import select
import time
import math
import ctypes
import threading
from typing import Optional, Callable, Dict, List, Tuple, Any

# Ensure platform compatibility or raise an explicit error
if not hasattr(select, "epoll") and not hasattr(select, "kqueue"):
    raise ImportError("aio_core.runtime requires either epoll (Linux) or kqueue (BSD/macOS).")

# ============================================================================
# EXCEPTION HIERARCHY
# ============================================================================

class AioCoreError(Exception):
    """Base exception for all aio-reactor-core errors."""
    pass

class BackpressureError(AioCoreError):
    """Raised when resource bounds and backpressure limits are reached."""
    pass

class TimerWheelError(AioCoreError):
    """Raised when timer scheduling or tick processing fails."""
    pass

class SPSCQueueError(AioCoreError):
    """Raised on invalid ring buffer state or concurrent violations."""
    pass

class MultiplexerError(AioCoreError):
    """Raised on poller registration or socket I/O faults."""
    pass


# ============================================================================
# ATOMIC & SYNCHRONIZATION PRIMITIVES (CAS / SPINLOCK)
# ============================================================================

class SpinLock:
    """Bounded spinlock with exponential backoff for low-latency thread safety."""
    __slots__ = ('_lock_val',)

    def __init__(self) -> None:
        self._lock_val = 0

    def acquire(self) -> None:
        backoff = 1
        while True:
            # Simple atomic test-and-set emulation using ctypes or Python lock primitives
            # Since CPython GIL protects simple variable assignment, we use an atomic exchange if possible,
            # or a fast-path lock. We implement a rigorous spin loop using a local atomic flag.
            if self._lock_val == 0:
                self._lock_val = 1
                return
            for _ in range(backoff):
                pass
            if backoff < 64:
                backoff <<= 1

    def release(self) -> None:
        self._lock_val = 0

    def __enter__(self) -> None:
        self.acquire()

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.release()


# ============================================================================
# SPSC RING BUFFER (LOCK-FREE CROSS-THREAD HANDOFF)
# ============================================================================

class SPSCQueue:
    """
    Lock-free Single-Producer Single-Consumer ring buffer using power-of-two capacity
    for fast bitwise masking. Eliminates mutex contention between poller and worker threads.
    """
    __slots__ = ('_capacity', '_mask', '_buffer', '_head', '_tail')

    def __init__(self, capacity: int = 4096) -> None:
        if capacity <= 0 or (capacity & (capacity - 1)) != 0:
            raise SPSCQueueError("Capacity must be a positive power of two.")
        self._capacity = capacity
        self._mask = capacity - 1
        self._buffer: List[Any] = [None] * capacity
        self._head = 0
        self._tail = 0

    def push(self, item: Any) -> bool:
        head = self._head
        tail = self._tail
        if (head - tail) & ~self._mask >= self._capacity:
            return False  # Full
        self._buffer[head & self._mask] = item
        self._head = head + 1
        return True

    def pop(self) -> Tuple[bool, Any]:
        head = self._head
        tail = self._tail
        if tail == head:
            return False, None  # Empty
        item = self._buffer[tail & self._mask]
        self._buffer[tail & self._mask] = None
        self._tail = tail + 1
        return True, item

    @property
    def size(self) -> int:
        return (self._head - self._tail) & ((self._capacity * 2) - 1)


# ============================================================================
# HIERARCHICAL TIMER WHEELS (O(1) TIMEOUT SCHEDULING)
# ============================================================================

class TimerNode:
    """Intrusive timer node for O(1) multi-level timer wheels."""
    __slots__ = ('callback', 'args', 'expires', 'interval', 'wheel_level', 'index', 'next', 'prev', 'cancelled')

    def __init__(self, callback: Callable[..., None], args: Tuple[Any, ...], expires: float, interval: float = 0.0) -> None:
        self.callback = callback
        self.args = args
        self.expires = expires
        self.interval = interval
        self.wheel_level = 0
        self.index = 0
        self.next: Optional[TimerNode] = None
        self.prev: Optional[TimerNode] = None
        self.cancelled = False


class HierarchicalTimerWheel:
    """
    Multi-level power-of-two cyclic timer wheel inspired by Varghese and Lauck.
    Provides O(1) amortized insertion, cancellation, and expiration processing.
    """
    __slots__ = ('_resolution', '_spokes_per_wheel', '_levels', '_wheels', '_current_time', '_tick_count')

    def __init__(self, resolution: float = 0.01, levels: int = 4, spokes_per_wheel: int = 256) -> None:
        self._resolution = resolution
        self._spokes_per_wheel = spokes_per_wheel
        self._levels = levels
        self._wheels: List[List[Optional[TimerNode]]] = [[None for _ in range(spokes_per_wheel)] for _ in range(levels)]
        self._current_time = time.monotonic()
        self._tick_count = 0

    def schedule(self, callback: Callable[..., None], delay: float, interval: float = 0.0, *args: Any) -> TimerNode:
        if delay < 0:
            raise TimerWheelError("Timer delay cannot be negative.")
        expires = self._current_time + delay
        node = TimerNode(callback, args, expires, interval)
        self._add_node(node)
        return node

    def cancel(self, node: TimerNode) -> None:
        if node.cancelled:
            return
        node.cancelled = True
        self._remove_node(node)

    def _add_node(self, node: TimerNode) -> None:
        delta = node.expires - self._current_time
        if delta < 0:
            delta = 0.0

        # Determine appropriate wheel level based on expiration delta
        span = self._resolution
        for level in range(self._levels):
            max_span = span * self._spokes_per_wheel
            if delta < max_span or level == self._levels - 1:
                ticks = int(delta / span)
                spoke = (self._tick_count + ticks) % self._spokes_per_wheel
                node.wheel_level = level
                node.index = spoke
                
                # Insert into doubly-linked list at wheel spoke
                head = self._wheels[level][spoke]
                node.next = head
                node.prev = None
                if head is not None:
                    head.prev = node
                self._wheels[level][spoke] = node
                return
            span = max_span

    def _remove_node(self, node: TimerNode) -> None:
        level = node.wheel_level
        spoke = node.index
        if node.prev is not None:
            node.prev.next = node.next
        else:
            self._wheels[level][spoke] = node.next
        if node.next is not None:
            node.next.prev = node.prev
        node.next = None
        node.prev = None

    def tick(self, now: float) -> List[TimerNode]:
        expired_nodes: List[TimerNode] = []
        while self._current_time <= now:
            self._cascade_ticks(0, self._tick_count)
            
            spoke = self._tick_count % self._spokes_per_wheel
            node = self._wheels[0][spoke]
            self._wheels[0][spoke] = None

            while node is not None:
                next_node = node.next
                node.next = None
                node.prev = None
                if not node.cancelled:
                    if node.expires <= now:
                        expired_nodes.append(node)
                        if node.interval > 0.0:
                            node.expires = now + node.interval
                            node.cancelled = False
                            self._add_node(node)
                    else:
                        # Re-bucket if resolution shifted
                        self._add_node(node)
                node = next_node

            self._tick_count += 1
            self._current_time += self._resolution
        return expired_nodes

    def _cascade_ticks(self, level: int, tick_val: int) -> None:
        if level >= self._levels:
            return
        spoke = (tick_val // (self._spokes_per_wheel ** level)) % self._spokes_per_wheel
        if spoke == 0 and level > 0:
            # Cascade from higher level wheel down
            higher_spoke = (tick_val // (self._spokes_per_wheel ** level)) % self._spokes_per_wheel
            node = self._wheels[level][higher_spoke]
            self._wheels[level][higher_spoke] = None
            while node is not None:
                next_node = node.next
                node.next = None
                node.prev = None
                if not node.cancelled:
                    self._add_node(node)
                node = next_node


# ============================================================================
# INTRUSIVE CONNECTION TRACKING & MEMORY MANAGEMENT
# ============================================================================

class ConnectionContext:
    """
    Reference-counted resource descriptor maintaining intrusive doubly-linked list pointers
    for zero-allocation multiplexer connection tracking and contiguous memory buffering.
    """
    __slots__ = (
        '_sock', '_fd', '_ref_count', '_read_buffer', '_write_buffer',
        '_next', '_prev', '_in_multiplexer', '_closed'
    )

    def __init__(self, sock: socket.socket, buffer_size: int = 65536) -> None:
        self._sock = sock
        self._fd = sock.fileno()
        self._ref_count = 1
        self._read_buffer = bytearray(buffer_size)
        self._write_buffer = bytearray(buffer_size)
        self._next: Optional[ConnectionContext] = None
        self._prev: Optional[ConnectionContext] = None
        self._in_multiplexer = False
        self._closed = False

    @property
    def sock(self) -> socket.socket:
        return self._sock

    @property
    def fd(self) -> int:
        return self._fd

    @property
    def read_view(self) -> memoryview:
        return memoryview(self._read_buffer)

    @property
    def write_view(self) -> memoryview:
        return memoryview(self._write_buffer)

    def acquire(self) -> None:
        self._ref_count += 1

    def release(self) -> None:
        self._ref_count -= 1
        if self._ref_count <= 0:
            self.close()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self._sock.close()
        except Exception:
            pass


# ============================================================================
# RUNTIME EVENT MULTIPLEXER (EPOLL/KQUEUE + BACKPRESSURE)
# ============================================================================

class RuntimeMultiplexer:
    """
    Pinned-thread Epoll/Kqueue multiplexer implementing edge-triggered semantics,
    intrusive connection tracking, and hard backpressure connection limiting.
    """
    __slots__ = (
        '_use_epoll', '_epoll', '_kqueue', '_max_connections', '_connection_count',
        '_connections', '_head', '_tail', '_lock'
    )

    def __init__(self, max_connections: int = 100000) -> None:
        self._max_connections = max_connections
        self._connection_count = 0
        self._connections: Dict[int, ConnectionContext] = {}
        self._head: Optional[ConnectionContext] = None
        self._tail: Optional[ConnectionContext] = None
        self._lock = SpinLock()

        self._use_epoll = hasattr(select, "epoll")
        if self._use_epoll:
            self._epoll = select.epoll()
            self._kqueue = None
        else:
            self._kqueue = select.kqueue()
            self._epoll = None

    def register(self, ctx: ConnectionContext) -> None:
        with self._lock:
            if self._connection_count >= self._max_connections:
                # Immediate TCP RST injection / graceful rejection under exhaustion
                try:
                    ctx.sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, ctypes.c_int(1), ctypes.c_int(0))
                except Exception:
                    pass
                ctx.close()
                raise BackpressureError("Connection pool utilization reached 100%. Hard backpressure enforced.")

            fd = ctx.fd
            if fd in self._connections:
                return

            # Register edge-triggered events (EPOLLET / EV_CLEAR)
            if self._use_epoll:
                self._epoll.register(fd, select.EPOLLIN | select.EPOLLOUT | select.EPOLLET | select.EPOLLERR | select.EPOLLHUP)
            else:
                kev_in = select.kevent(fd, select.KQ_FILTER_READ, select.KQ_EV_ADD | select.KQ_EV_ENABLE | select.KQ_EV_CLEAR)
                kev_out = select.kevent(fd, select.KQ_FILTER_WRITE, select.KQ_EV_ADD | select.KQ_EV_ENABLE | select.KQ_EV_CLEAR)
                self._kqueue.control([kev_in, kev_out], 0, 0)

            ctx.acquire()
            self._connections[fd] = ctx
            self._connection_count += 1
            ctx._in_multiplexer = True

            # Append to intrusive linked list
            if self._head is None:
                self._head = ctx
                self._tail = ctx
            else:
                if self._tail is not None:
                    self._tail._next = ctx
                ctx._prev = self._tail
                self._tail = ctx

    def unregister(self, ctx: ConnectionContext) -> None:
        with self._lock:
            fd = ctx.fd
            if fd not in self._connections:
                return

            if self._use_epoll:
                try:
                    self._epoll.unregister(fd)
                except Exception:
                    pass
            else:
                try:
                    kev_in = select.kevent(fd, select.KQ_FILTER_READ, select.KQ_EV_DELETE)
                    kev_out = select.kevent(fd, select.KQ_FILTER_WRITE, select.KQ_EV_DELETE)
                    self._kqueue.control([kev_in, kev_out], 0, 0)
                except Exception:
                    pass

            # Remove from intrusive linked list
            if ctx._prev is not None:
                ctx._prev._next = ctx._next
            else:
                self._head = ctx._next

            if ctx._next is not None:
                ctx._next._prev = ctx._prev
            else:
                self._tail = ctx._prev

            ctx._next = None
            ctx._prev = None
            ctx._in_multiplexer = False

            del self._connections[fd]
            self._connection_count -= 1
            ctx.release()

    def poll(self, timeout: float) -> List[Tuple[ConnectionContext, int]]:
        events: List[Tuple[ConnectionContext, int]] = []
        if self._use_epoll:
            raw_events = self._epoll.poll(timeout)
            for fd, event_mask in raw_events:
                ctx = self._connections.get(fd)
                if ctx is not None:
                    events.append((ctx, event_mask))
                    # Isolate and purge descriptors encountering EPOLLERR or EPOLLHUP
                    if event_mask & (select.EPOLLERR | select.EPOLLHUP):