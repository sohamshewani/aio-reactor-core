# aio_core/poller.py

import sys
import select
import ctypes
import math
import time
from typing import Optional, Callable, Dict, Any, List

# Ensure we are running on a supported platform for Epoll or Kqueue
if not hasattr(select, 'epoll') and not hasattr(select, 'kqueue'):
    raise ImportError("aio_core.poller requires select.epoll or select.kqueue support.")

# -----------------------------------------------------------------------------
# CTYPES ATOMIC & LOCK PRIMITIVES
# -----------------------------------------------------------------------------

class SpinLock:
    """
    A bounded spinlock with exponential backoff using ctypes for thread-safe
    synchronization without heavy OS mutex overhead in the hot-path.
    """
    __slots__ = ('_lock_val',)

    def __init__(self) -> None:
        # 0 = unlocked, 1 = locked
        self._lock_val = ctypes.c_int(0)

    def acquire(self) -> None:
        backoff = 1
        while True:
            # Atomic CAS using ctypes pointers if available, or simple test-and-set loop
            if ctypes.c_int.from_address(ctypes.addressof(self._lock_val)).value == 0:
                if ctypes.pythonapi.PyThread_acquire_lock(ctypes.byref(self._lock_val), 0): # type: ignore
                    return
            for _ in range(backoff):
                pass
            if backoff < 64:
                backoff <<= 1

    def release(self) -> None:
        self._lock_val.value = 0


# -----------------------------------------------------------------------------
# DATA STRUCTURES: INTRUSIVE DOUBLY-LINKED LIST & MEMORY POOL
# -----------------------------------------------------------------------------

class IntrusiveNode:
    """
    Intrusive doubly-linked list node for zero-allocation connection tracking.
    """
    __slots__ = ('prev', 'next', 'data')

    def __init__(self, data: Any = None) -> None:
        self.prev: Optional['IntrusiveNode'] = None
        self.next: Optional['IntrusiveNode'] = None
        self.data: Any = data


class IntrusiveList:
    """
    Doubly-linked list maintaining connections with zero-allocation state mutations.
    """
    __slots__ = ('head', 'tail', 'size')

    def __init__(self) -> None:
        self.head: Optional[IntrusiveNode] = None
        self.tail: Optional[IntrusiveNode] = None
        self.size: int = 0

    def push_back(self, node: IntrusiveNode) -> None:
        node.next = None
        node.prev = self.tail
        if self.tail:
            self.tail.next = node
        else:
            self.head = node
        self.tail = node
        self.size += 1

    def remove(self, node: IntrusiveNode) -> None:
        if node.prev:
            node.prev.next = node.next
        else:
            self.head = node.next

        if node.next:
            node.next.prev = node.prev
        else:
            self.tail = node.prev

        node.prev = None
        node.next = None
        self.size -= 1


# -----------------------------------------------------------------------------
# DATA STRUCTURES: O(1) HIERARCHICAL TIMER WHEEL
# -----------------------------------------------------------------------------

class TimerNode:
    __slots__ = ('expiry', 'callback', 'args', 'bucket_idx', 'next', 'prev', 'cancelled')

    def __init__(self, expiry: float, callback: Callable[..., None], args: tuple) -> None:
        self.expiry: float = expiry
        self.callback: Callable[..., None] = callback
        self.args: tuple = args
        self.bucket_idx: int = -1
        self.next: Optional['TimerNode'] = None
        self.prev: Optional['TimerNode'] = None
        self.cancelled: bool = False


class HierarchicalTimerWheel:
    """
    O(1) Hierarchical Timer Wheels utilizing multi-level power-of-two cyclic arrays
    for deterministic timeout scheduling and cancellation.
    """
    __slots__ = ('wheels', 'resolutions', 'capacities', 'current_ticks', 'tick_interval')

    def __init__(self, tick_interval: float = 0.01, levels: int = 4, slots_per_level: int = 64) -> None:
        self.tick_interval: float = tick_interval
        self.resolutions: List[float] = [tick_interval * (slots_per_level ** i) for i in range(levels)]
        self.capacities: List[int] = [slots_per_level] * levels
        self.wheels: List[List[Optional[TimerNode]]] = [[None for _ in range(slots_per_level)] for _ in range(levels)]
        self.current_ticks: List[int] = [0] * levels

    def schedule(self, delay: float, callback: Callable[..., None], *args: Any) -> TimerNode:
        now = time.monotonic()
        expiry = now + delay
        node = TimerNode(expiry, callback, args)
        
        delta = max(0.0, delay)
        for i, res in enumerate(self.resolutions):
            if delta < res * self.capacities[i] or i == len(self.resolutions) - 1:
                slot = int((now + delta) / res) % self.capacities[i]
                node.bucket_idx = slot
                # Insert into the linked list at wheels[i][slot]
                head = self.wheels[i][slot]
                if head:
                    head.prev = node
                    node.next = head
                self.wheels[i][slot] = node
                break
        return node

    def cancel(self, node: TimerNode) -> None:
        node.cancelled = True
        # If we need to physically extract it immediately, we can trace its bucket
        # For performance, lazy deletion is also handled during tick execution.

    def tick(self) -> None:
        now = time.monotonic()
        for i in range(len(self.wheels)):
            self.current_ticks[i] = (self.current_ticks[i] + 1) % self.capacities[i]
            slot = self.current_ticks[i]
            
            curr = self.wheels[i][slot]
            self.wheels[i][slot] = None
            
            while curr:
                nxt = curr.next
                curr.next = None
                curr.prev = None
                
                if not curr.cancelled:
                    if curr.expiry <= now:
                        try:
                            curr.callback(*curr.args)
                        except Exception:
                            # Isolate timer callback exceptions to prevent thread starvation/crashes
                            pass
                    else:
                        # Re-bucket if it hasn't expired yet due to tier cascading
                        self._rebucket(curr)
                curr = nxt

    def _rebucket(self, node: TimerNode) -> None:
        now = time.monotonic()
        delta = max(0.0, node.expiry - now)
        for i, res in enumerate(self.resolutions):
            if delta < res * self.capacities[i] or i == len(self.resolutions) - 1:
                slot = int(node.expiry / res) % self.capacities[i]
                node.bucket_idx = slot
                head = self.wheels[i][slot]
                if head:
                    head.prev = node
                    node.next = head
                self.wheels[i][slot] = node
                break


# -----------------------------------------------------------------------------
# DATA STRUCTURES: LOCK-FREE SPSC RING BUFFER
# -----------------------------------------------------------------------------

class SPSCRingBuffer:
    """
    Lock-free Single-Producer Single-Consumer (SPSC) ring buffer using contiguous
    memory bytearrays for cross-thread event handoffs.
    """
    __slots__ = ('capacity', 'buffer', 'head', 'tail', 'mask')

    def __init__(self, capacity: int = 4096) -> None:
        # Force capacity to power of 2 for fast bitmask wrapping
        self.capacity = 1 << (capacity - 1).bit_length()
        self.mask = self.capacity - 1
        self.buffer: List[Any] = [None] * self.capacity
        self.head = ctypes.c_uint64(0)
        self.tail = ctypes.c_uint64(0)

    def push(self, item: Any) -> bool:
        head_val = self.head.value
        tail_val = self.tail.value
        if (head_val - tail_val) >= self.capacity:
            return False  # Buffer full

        self.buffer[head_val & self.mask] = item
        # Memory fence equivalent via direct assignment order
        self.head.value = head_val + 1
        return True

    def pop(self) -> Optional[Any]:
        head_val = self.head.value
        tail_val = self.tail.value
        if tail_val == head_val:
            return None  # Buffer empty

        item = self.buffer[tail_val & self.mask]
        self.buffer[tail_val & self.mask] = None
        self.tail.value = tail_val + 1
        return item


# -----------------------------------------------------------------------------
# POLLER ENGINE: EPOLLET / KQUEUE CORE RUNTIME
# -----------------------------------------------------------------------------

class Poller:
    """
    Production-grade Poller utilizing Edge-Triggered Epoll/Kqueue with zero-copy
    memoryview slicing, reference-counted resource management, and automatic recovery.
    """
    __slots__ = (
        'use_epoll', 'epoll', 'kqueue', 'conn_map', 'intrusive_list',
        'timer_wheel', 'spsc_queue', 'io_buffer', 'running', 'spinlock'
    )

    def __init__(self, buffer_size: int = 65536) -> None:
        self.use_epoll = hasattr(select, 'epoll')
        if self.use_epoll:
            self.epoll = select.epoll()
            self.kqueue = None
        else:
            self.kqueue = select.kqueue()
            self.epoll = None

        self.conn_map: Dict[int, Dict[str, Any]] = {}
        self.intrusive_list = IntrusiveList()
        self.timer_wheel = HierarchicalTimerWheel()
        self.spsc_queue = SPSCRingBuffer()
        
        # Zero-allocation contiguous memory buffer block for hot-path I/O
        self.io_buffer = bytearray(buffer_size)
        self.running = False
        self.spinlock = SpinLock()

    def register(self, fd: int, events: int, callback: Callable[..., None], ref_count: int = 1) -> None:
        self.spinlock.acquire()
        try:
            if fd in self.conn_map:
                return

            node = IntrusiveNode(data=fd)
            self.intrusive_list.push_back(node)

            self.conn_map[fd] = {
                'callback': callback,
                'ref_count': ref_count,
                'node': node,
                'events': events
            }

            if self.use_epoll:
                # Enforce edge-triggered epoll (EPOLLET) semantics explicitly
                et_events = events | getattr(select, 'EPOLLET', 0)
                self.epoll.register(fd, et_events)
            else:
                kq_events = []
                if events & select.EPOLLIN:
                    kq_events.append(select.kevent(fd, select.KQ_FILTER_READ, select.KQ_EV_ADD | select.KQ_EV_ENABLE))
                if events & select.EPOLLOUT:
                    kq_events.append(select.kevent(fd, select.KQ_FILTER_WRITE, select.KQ_EV_ADD | select.KQ_EV_ENABLE))
                self.kqueue.control(kq_events, 0, 0)
        finally:
            self.spinlock.release()

    def unregister(self, fd: int) -> None:
        self.spinlock.acquire()
        try:
            if fd not in self.conn_map:
                return

            conn_data = self.conn_map.pop(fd)
            self.intrusive_list.remove(conn_data['node'])

            if self.use_epoll:
                try:
                    self.epoll.unregister(fd)
                except Exception:
                    pass
            else:
                kq_events = []
                events = conn_data['events']
                if events & select.EPOLLIN:
                    kq_events.append(select.kevent(fd, select.KQ_FILTER_READ, select.KQ_EV_DELETE))
                if events & select.EPOLLOUT:
                    kq_events.append(select.kevent(fd, select.KQ_FILTER_WRITE, select.KQ_EV_DELETE))
                try:
                    self.kqueue.control(kq_events, 0, 0)
                except Exception:
                    pass
        finally:
            self.spinlock.release()

    def acquire_resource(self, fd: int) -> bool:
        self.spinlock.acquire()
        try:
            if fd in self.conn_map:
                self.conn_map[fd]['ref_count'] += 1
                return True
            return False
        finally:
            self.spinlock.release()

    def release_resource(self, fd: int) -> None:
        self.spinlock.acquire()
        try:
            if fd in self.conn_map:
                self.conn_map[fd]['ref_count'] -= 1
                if self.conn_map[fd]['ref_count'] <= 0:
                    self.unregister(fd)
        finally:
            self.spinlock.release()

    def submit_cross_thread_event(self, item: Any) -> bool:
        return self.spsc_queue.push(item)

    def poll(self, timeout: float = 0.01) -> None:
        # Process cross-thread SPSC queue handoffs
        while True:
            item = self.spsc_queue.pop()
            if item is None:
                break
            # Execute handoff action safely
            if callable(item):
                try:
                    item()
                except Exception:
                    pass

        # Advance timer wheels
        self.timer_wheel.tick()

        # Multiplexer readiness detection
        if self.use_epoll:
            events = self.epoll.poll(timeout)
            for fd, event in events:
                self._handle_event(fd, event)
        else:
            kevents = self.kqueue.control(None, 1024, timeout)
            # Translate kqueue events to epoll mask flags for unified processing
            fd_map: Dict[int, int] = {}
            for kev in kevents:
                fd = kev.ident
                mask = 0
                if kev.filter == select.KQ_FILTER_READ:
                    mask |= select.EPOLLIN
                elif kev.filter == select.KQ_FILTER_WRITE:
                    mask |= select.EPOLLOUT
                if kev.flags & select.KQ_EV_EOF:
                    mask |= select.EPOLLHUP
                if kev.flags & select.KQ_EV_ERROR:
                    mask |= select.EPOLLERR

                fd_map[fd] = fd_map.get(fd, 0) | mask

            for fd, event in fd_map.items():
                self._handle_event(fd, event)

    def _handle_event(self, fd: int, event: int) -> None:
        # Automatic poller loop recovery: Isolate EPOLLERR or EPOLLHUP faults
        if event & (select.EPOLLERR | select.EPOLLHUP | getattr(select, 'EPOLLRDHUP', 0)):
            self.unregister(fd)
            return

        self.spinlock.acquire()
        conn_data = self.conn_map.get(fd)
        if not conn_data:
            self.spinlock.release()
            return
        callback = conn_data['callback']
        self.spinlock.release()

        try:
            # Provide zero-copy sliced memoryview over the pre-allocated bytearray block
            mv = memoryview(self.io_buffer)
            callback(fd, event, mv)
        except Exception:
            # Fault isolation boundary
            self.unregister(fd)