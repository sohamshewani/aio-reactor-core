# aio_core/timer.py

import time
import sys
from typing import Callable, Optional, Any

class TimerNode:
    __slots__ = ('expires', 'callback', 'arg', 'next', 'prev', 'wheel_index', 'slot_index', 'cancelled')

    def __init__(self, expires: int, callback: Callable[..., Any], arg: Any) -> None:
        self.expires: int = expires
        self.callback: Callable[..., Any] = callback
        self.arg: Any = arg
        self.next: Optional['TimerNode'] = None
        self.prev: Optional['TimerNode'] = None
        self.wheel_index: int = -1
        self.slot_index: int = -1
        self.cancelled: bool = False

class HierarchicalTimerWheel:
    def __init__(self, resolution_ms: int = 1, wheel_sizes: tuple = (256, 64, 64, 64, 64)) -> None:
        self.resolution_ms: int = resolution_ms
        self.wheel_sizes: tuple = wheel_sizes
        self.num_wheels: int = len(wheel_sizes)
        
        self.intervals: list[int] = [0] * self.num_wheels
        accum = resolution_ms
        for i in range(self.num_wheels):
            self.intervals[i] = accum
            accum *= wheel_sizes[i]

        self.wheels: list[list[list[Optional[TimerNode]]]] = []
        self.wheel_masks: list[int] = []
        self.wheel_shifts: list[int] = []

        for size in wheel_sizes:
            if (size & (size - 1)) != 0:
                raise ValueError("Wheel size must be a power of two.")
            slots: list[list[Optional[TimerNode]]] = [[] for _ in range(size)]
            self.wheels.append(slots)
            self.wheel_masks.append(size - 1)
            self.wheel_shifts.append((size - 1).bit_length())

        self.current_time_ms: int = int(time.monotonic() * 1000)
        self.last_tick_time: int = self.current_time_ms
        self.wheel_cursor: list[int] = [0] * self.num_wheels

    def _get_node_sentinel(self) -> TimerNode:
        return TimerNode(0, lambda *a: None, None)

    def schedule(self, delay_ms: int, callback: Callable[..., Any], arg: Any = None) -> TimerNode:
        if delay_ms < 0:
            delay_ms = 0
        
        expires = self.current_time_ms + delay_ms
        node = TimerNode(expires, callback, arg)
        self._add_node(node)
        return node

    def cancel(self, node: TimerNode) -> bool:
        if node.cancelled:
            return False
        node.cancelled = True
        self._remove_node(node)
        return True

    def _add_node(self, node: TimerNode) -> None:
        diff = node.expires - self.current_time_ms
        if diff < 0:
            diff = 0

        target_wheel = 0
        for i in range(self.num_wheels):
            if diff < self.intervals[i] * self.wheel_sizes[i]:
                target_wheel = i
                break
        else:
            target_wheel = self.num_wheels - 1

        shift = 0
        for i in range(target_wheel):
            shift += self.wheel_shifts[i]
        
        slot_index = (node.expires >> (shift + self.resolution_ms.bit_length() - 1)) & self.wheel_masks[target_wheel]
        
        node.wheel_index = target_wheel
        node.slot_index = slot_index

        slot_list = self.wheels[target_wheel][slot_index]
        slot_list.append(node)

    def _remove_node(self, node: TimerNode) -> None:
        if node.wheel_index == -1 or node.slot_index == -1:
            return
        slot_list = self.wheels[node.wheel_index][node.slot_index]
        try:
            slot_list.remove(node)
        except ValueError:
            pass
        node.wheel_index = -1
        node.slot_index = -1

    def tick(self, current_time_ms: Optional[int] = None) -> int:
        if current_time_ms is None:
            current_time_ms = int(time.monotonic() * 1000)

        if current_time_ms <= self.last_tick_time:
            return 0

        steps = current_time_ms - self.last_tick_time
        executed_count = 0

        for _ in range(steps):
            self.last_tick_time += 1
            self.current_time_ms = self.last_tick_time
            
            index = 0
            while index < self.num_wheels:
                cursor = self.wheel_cursor[index]
                self.wheel_cursor[index] = (cursor + 1) & self.wheel_masks[index]
                
                slot_list = self.wheels[index][cursor]
                self.wheels[index][cursor] = []

                for node in slot_list:
                    if node.cancelled:
                        continue
                    if node.expires <= self.current_time_ms:
                        try:
                            node.callback(node.arg)
                        except Exception:
                            pass
                        executed_count += 1
                    else:
                        self._add_node(node)

                if cursor != 0:
                    break
                index += 1

        return executed_count