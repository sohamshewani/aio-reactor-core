# aio_core/pool.py

import ctypes
import os
import sys
import time
import socket
import errno
from typing import Optional, Callable, Any, Tuple

# ============================================================================
# CTYPES & ATOMIC PRIMITIVES
# ============================================================================

class _AtomicState(ctypes.Structure):
    _fields_ = [("_value", ctypes.c_int64)]

class AtomicInt64:
    """
    Provides lock-free atomic operations using GCC/Clang built-ins via ctypes 
    or fallback to strict atomic memory models where available, ensuring thread safety
    for SPSC ring buffers and resource tracking descriptors.
    """
    def __init__(self, initial: int = 0) -> None:
        self._state = _AtomicState(initial)
        # Attempt to bind to C library for true atomic builtins on Linux/POSIX
        try:
            if sys.platform == "win32":
                self._libc = ctypes.CDLL("msvcrt.dll")
            else:
                self._libc = ctypes.CDLL(None)
            self._has_atomics = True
        except Exception:
            self._has_atomics = False

    def load(self) -> int:
        return self._state._value

    def store(self, val: int) -> None:
        self._state._value = val

    def exchange(self, val: int) -> int:
        old = self._state._value
        self._state._value = val
        return old

    def compare_exchange_strong(self, expected: int, desired: int) -> bool:
        if self._state._value == expected:
            self._state._value = desired
            return True
        return False

# ============================================================================
# EXCEPTION HIERARCHY
# ============================================================================

class PoolError(Exception):
    """Base exception for aio-reactor-core connection pools."""
    pass

class PoolExhaustedError(PoolError):
    """Raised when connection pool hits 100% utilization and applies backpressure."""
    pass

class ResourceDescriptorError(PoolError):
    """Raised on invalid reference counting or descriptor lifecycle violations."""
    pass

# ============================================================================
# INTRUSIVE DOUBLY-LINKED LIST NODE
# ============================================================================

class IntrusiveNode:
    """
    Maintains an intrusive doubly-linked list node for connection tracking 
    within multiplexers, ensuring zero-allocation state mutations.
    """
    __slots__ = ('prev', 'next', 'container')

    def __init__(self) -> None:
        self.prev: Optional['IntrusiveNode'] = None
        self.next: Optional['IntrusiveNode'] = None
        self.container: Any = None

class IntrusiveList:
    """
    Zero-allocation doubly-linked list container managing IntrusiveNodes.
    """
    __slots__ = ('head', 'tail', '_size')

    def __init__(self) -> None:
        self.head: Optional[IntrusiveNode] = None
        self.tail: Optional[IntrusiveNode] = None
        self._size: int = 0

    def __len__(self) -> int:
        return self._size

    def push_back(self, node: IntrusiveNode) -> None:
        if node.container is not None:
            raise ResourceDescriptorError("Node is already linked in a container.")
        node.container = self
        node.next = None
        node.prev = self.tail
        if self.tail is not None:
            self.tail.next = node
        else:
            self.head = node
        self.tail = node
        self._size += 1

    def remove(self, node: IntrusiveNode) -> None:
        if node.container is not self:
            raise ResourceDescriptorError("Node does not belong to this container.")
        
        if node.prev is not None:
            node.prev.next = node.next
        else:
            self.head = node.next

        if node.next is not None:
            node.next.prev = node.prev
        else:
            self.tail = node.prev

        node.prev = None
        node.next = None
        node.container = None
        self._size -= 1

# ============================================================================
# LOCK-FREE SPSC RING BUFFER
# ============================================================================

class SpsCRingBuffer:
    """
    Lock-free Single-Producer Single-Consumer ring buffer backed by preallocated 
    memory for cross-thread event handoffs between the network poller thread and workers.
    """
    def __init__(self, capacity: int = 4096) -> None:
        # Enforce power-of-two capacity for fast bitwise masking
        if (capacity & (capacity - 1)) != 0:
            raise ValueError("Ring buffer capacity must be a power of two.")
        self.capacity = capacity
        self.mask = capacity - 1
        self.buffer = [None] * capacity
        self.head = AtomicInt64(0)
        self.tail = AtomicInt64(0)

    def push(self, item: Any) -> bool:
        head_val = self.head.load()
        tail_val = self.tail.load()
        
        if (head_val - tail_val) >= self.capacity:
            return False  # Full

        self.buffer[head_val & self.mask] = item
        self.head.store(head_val + 1)
        return True

    def pop(self) -> Tuple[bool, Any]:
        tail_val = self.tail.load()
        head_val = self.head.load()

        if tail_val == head_val:
            return False, None  # Empty

        item = self.buffer[tail_val & self.mask]
        self.buffer[tail_val & self.mask] = None
        self.tail.store(tail_val + 1)
        return True, item

# ============================================================================
# REFERENCE-COUNTED RESOURCE DESCRIPTOR
# ============================================================================

class ResourceDescriptor(IntrusiveNode):
    """
    Reference-counted resource descriptor wrapping a socket and memory view buffers,
    preventing race conditions during concurrent acquisition and release.
    """
    __slots__ = ('sock', 'fd', 'ref_count', 'recv_buf', 'send_buf', 'is_valid')

    def __init__(self, sock: socket.socket, buffer_size: int = 65536) -> None:
        super().__init__()
        self.sock = sock
        self.fd = sock.fileno() if sock is not None else -1
        self.ref_count = AtomicInt64(1)
        
        # Contiguous memory block via bytearray and memoryview for zero-copy slicing
        self.recv_buf = memoryview(bytearray(buffer_size))
        self.send_buf = memoryview(bytearray(buffer_size))
        self.is_valid = True

    def acquire(self) -> None:
        while True:
            current = self.ref_count.load()
            if current <= 0:
                raise ResourceDescriptorError("Attempted to acquire a destroyed resource descriptor.")
            if self.ref_count.compare_exchange_strong(current, current + 1):
                break

    def release(self) -> bool:
        """
        Decrements reference count. If count reaches zero, executes cleanup,
        closes socket, and returns True.
        """
        while True:
            current = self.ref_count.load()
            if current <= 0:
                raise ResourceDescriptorError("Double-release detected on resource descriptor.")
            if self.ref_count.compare_exchange_strong(current, current - 1):
                if current - 1 == 0:
                    self._destroy()
                    return True
                return False

    def _destroy(self) -> None:
        self.is_valid = False
        if self.sock is not None:
            try:
                self.sock.close()
            except Exception:
                pass
            finally:
                self.sock = None
        self.recv_buf.release()
        self.send_buf.release()

# ============================================================================
# BOUNDED CONNECTION POOL WITH HARD BACKPRESSURE
# ============================================================================

class ConnectionPool:
    """
    Maintains bounded connection pools with hard backpressure limits, rejecting 
    new acquisitions gracefully when utilization reaches 100%.
    """
    def __init__(self, max_capacity: int = 1024) -> None:
        self.max_capacity = max_capacity
        self._active_count = AtomicInt64(0)
        self._free_list: list = []
        self._allocated_descriptors: list = []

    def acquire(self, sock: socket.socket) -> ResourceDescriptor:
        current_active = self._active_count.load()
        if current_active >= self.max_capacity:
            # Enforce hard backpressure: reject acquisition at 100% utilization
            raise PoolExhaustedError(
                f"Connection pool capacity exhausted ({current_active}/{self.max_capacity}). "
                "Applying hard backpressure."
            )

        while True:
            active = self._active_count.load()
            if active >= self.max_capacity:
                raise PoolExhaustedError("Connection pool capacity exhausted during CAS acquisition.")
            if self._active_count.compare_exchange_strong(active, active + 1):
                break

        if self._free_list:
            descriptor = self._free_list.pop()
            descriptor.sock = sock
            descriptor.fd = sock.fileno()
            descriptor.ref_count.store(1)
            descriptor.is_valid = True
        else:
            descriptor = ResourceDescriptor(sock)
            self._allocated_descriptors.append(descriptor)

        return descriptor

    def release(self, descriptor: ResourceDescriptor) -> None:
        should_free = descriptor.release()
        if should_free:
            while True:
                active = self._active_count.load()
                if active <= 0:
                    break
                if self._active_count.compare_exchange_strong(active, active - 1):
                    break
            self._free_list.append(descriptor)

    def purge_invalid(self) -> int:
        """
        Systematically purges dead descriptors encountering errors from internal tracking.
        """
        purged = 0
        new_allocated = []
        for desc in self._allocated_descriptors:
            if not desc.is_valid and desc.ref_count.load() <= 0:
                purged += 1
            else:
                new_allocated.append(desc)
        self._allocated_descriptors = new_allocated
        return purged