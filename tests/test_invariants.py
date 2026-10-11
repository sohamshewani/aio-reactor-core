import asyncio
import concurrent.futures
import math
import random
import threading
import time
import unittest

# ============================================================================
# Implementation of aio-reactor-core (Embedded directly for self-contained execution)
# ============================================================================

class EventLoopStoppedError(Exception):
    """Raised when an operation is attempted on a stopped reactor core."""
    pass

class ReactorError(Exception):
    """Base error for reactor core anomalies."""
    pass

class Task:
    """Represents a discrete unit of work scheduled within the reactor core."""
    __slots__ = ('_coro', '_future', '_cancelled', '_done')

    def __init__(self, coro):
        if not asyncio.iscoroutine(coro):
            raise TypeError(f"A coroutine is required, got {type(coro)}")
        self._coro = coro
        self._future = asyncio.Future()
        self._cancelled = False
        self._done = False

    @property
    def done(self) -> bool:
        return self._done

    @property
    def cancelled(self) -> bool:
        return self._cancelled

    def cancel(self):
        if self._done or self._cancelled:
            return
        self._cancelled = True
        self._done = True
        if not self._future.done():
            self._future.set_exception(asyncio.CancelledError())

    def __await__(self):
        return self._future.__await__()


class ReactorCore:
    """
    High-performance, thread-safe asynchronous reactor core managing task 
    dispatch, timers, event multiplexing, and fault-recovery loops.
    """
    def __init__(self, max_concurrency: int = 1024):
        if max_concurrency <= 0:
            raise ValueError("max_concurrency must be strictly positive")
        
        self._max_concurrency = max_concurrency
        self._tasks = set()
        self._pending_tasks = asyncio.Queue()
        self._timers = []  # Priority queue items: (deadline, seq, callback)
        self._timer_seq = 0
        self._lock = threading.RLock()
        self._running = False
        self._stopped_event = asyncio.Event()
        self._stopped_event.set()
        self._loop = None
        self._metrics = {
            'tasks_submitted': 0,
            'tasks_completed': 0,
            'tasks_failed': 0,
            'tasks_cancelled': 0,
            'timers_fired': 0
        }
        self._error_handlers = []

    @property
    def running(self) -> bool:
        with self._lock:
            return self._running

    @property
    def active_task_count(self) -> int:
        with self._lock:
            return len(self._tasks)

    def register_error_handler(self, handler):
        with self._lock:
            self._error_handlers.append(handler)

    def submit(self, coro) -> Task:
        """Thread-safe task submission to the reactor core."""
        with self._lock:
            if not self._running:
                raise EventLoopStoppedError("Cannot submit tasks to a stopped reactor core.")
            
            task = Task(coro)
            self._metrics['tasks_submitted'] += 1
            
            # Dispatch directly if loop is running, else queue
            if self._loop and self._loop.is_running():
                asyncio.run_coroutine_threadsafe(self._dispatch_task(task), self._loop)
            else:
                self._pending_tasks.put_nowait(task)
            return task

    async def _dispatch_task(self, task: Task):
        with self._lock:
            if len(self._tasks) >= self._max_concurrency:
                # Re-queue if capacity breached
                await self._pending_tasks.put(task)
                return
            self._tasks.add(task)

        try:
            if task.cancelled:
                return
            result = await task._coro
            if not task._future.done():
                task._future.set_result(result)
            with self._lock:
                self._metrics['tasks_completed'] += 1
        except asyncio.CancelledError:
            task.cancel()
            with self._lock:
                self._metrics['tasks_cancelled'] += 1
        except Exception as e:
            if not task._future.done():
                task._future.set_exception(e)
            with self._lock:
                self._metrics['tasks_failed'] += 1
            self._handle_unhandled_error(e)
        finally:
            with self._lock:
                self._tasks.discard(task)
                task._done = True
            # Process any pending backlog
            self._drain_pending()

    def _drain_pending(self):
        while not self._pending_tasks.empty():
            try:
                task = self._pending_tasks.get_nowait()
                if len(self._tasks) < self._max_concurrency:
                    if self._loop and self._loop.is_running():
                        asyncio.run_coroutine_threadsafe(self._dispatch_task(task), self._loop)
                else:
                    self._pending_tasks.put_nowait(task)
                    break
            except asyncio.QueueEmpty:
                break

    def schedule_timer(self, delay: float, callback, *args, **kwargs):
        """Schedule a callback execution after a given delay in seconds."""
        if delay < 0:
            raise ValueError("Timer delay cannot be negative.")
        
        with self._lock:
            if not self._running:
                raise EventLoopStoppedError("Cannot schedule timers on a stopped reactor.")
            
            deadline = time.monotonic() + delay
            self._timer_seq += 1
            
            # Simple list-based sorted insertion for portability
            import bisect
            item = (deadline, self._timer_seq, callback, args, kwargs)
            bisect.insort(self._timers, item, key=lambda x: x[0])

    def _handle_unhandled_error(self, exc: Exception):
        for handler in self._error_handlers:
            try:
                handler(exc)
            except Exception:
                pass

    def get_metrics(self) -> dict:
        with self._lock:
            return dict(self._metrics)

    async def run(self):
        """Starts the reactor execution loop."""
        with self._lock:
            if self._running:
                raise ReactorError("Reactor core is already running.")
            self._running = True
            self._loop = asyncio.get_running_loop()
            self._stopped_event.clear()

        try:
            while True:
                with self._lock:
                    if not self._running:
                        break
                    
                    # Process expired timers
                    now = time.monotonic()
                    while self._timers and self._timers[0][0] <= now:
                        _, _, cb, args, kwargs = self._timers.pop(0)
                        self._metrics['timers_fired'] += 1
                        # Fire timer asynchronously or via thread pool/loop execution
                        if asyncio.iscoroutinefunction(cb):
                            asyncio.create_task(cb(*args, **kwargs))
                        else:
                            try:
                                cb(*args, **kwargs)
                            except Exception as e:
                                self._handle_unhandled_error(e)

                # Small yield to prevent CPU starvation
                await asyncio.sleep(0.001)
        finally:
            with self._lock:
                self._running = False
                # Cancel remaining tasks
                for task in list(self._tasks):
                    task.cancel()
                self._tasks.clear()
                self._stopped_event.set()

    def stop(self):
        """Signals the reactor core to shut down gracefully."""
        with self._lock:
            self._running = False
            if self._loop and self._loop.is_running():
                # Wake up loop
                pass


# ============================================================================
# Exhaustive Test Suite for aio-reactor-core
# ============================================================================

class TestAioReactorCore(unittest.TestCase):

    def setUp(self):
        self.reactor = ReactorCore(max_concurrency=50)

    def tearDown(self):
        # Ensure any lingering loops are cleared or cleaned up
        pass

    # -------------------------------------------------------------------------
    # 1. Boundary and Edge-Case Assertions
    # -------------------------------------------------------------------------
    
    def test_invalid_concurrency_initialization(self):
        with self.assertRaises(ValueError):
            ReactorCore(max_concurrency=0)
        with self.assertRaises(ValueError):
            ReactorCore(max_concurrency=-10)

    def test_submit_to_stopped_reactor(self):
        async def dummy():
            return 42

        # Reactor not started yet
        with self.assertRaises(EventLoopStoppedError):
            self.reactor.submit(dummy())

    def test_invalid_coroutine_submission(self):
        # Start reactor helper loop
        async def run_test():
            await asyncio.wait_for(self.reactor.run(), timeout=0.2)

        async def launch():
            # Start reactor in background
            task = asyncio.create_task(self.reactor.run())
            await asyncio.sleep(0.01)
            
            with self.assertRaises(TypeError):
                self.reactor.submit(not_a_coro := "im not a coroutine")
            
            self.reactor.stop()
            await task

        asyncio.run(launch())

    def test_negative_timer_delay(self):
        async def launch():
            task = asyncio.create_task(self.reactor.run())
            await asyncio.sleep(0.01)
            
            with self.assertRaises(ValueError):
                self.reactor.schedule_timer(-1.5, lambda: None)
                
            self.reactor.stop()
            await task

        asyncio.run(launch())

    def test_task_cancellation_edge_cases(self):
        async def infinite_loop():
            while True:
                await asyncio.sleep(0.1)

        async def launch():
            reactor_task = asyncio.create_task(self.reactor.run())
            await asyncio.sleep(0.01)

            t = self.reactor.submit(infinite_loop())
            self.assertFalse(t.done)
            self.assertFalse(t.cancelled)

            t.cancel()
            self.assertTrue(t.cancelled)
            self.assertTrue(t.done)

            with self.assertRaises(asyncio.CancelledError):
                await t

            self.reactor.stop()
            await reactor_task

        asyncio.run(launch())

    # -------------------------------------------------------------------------
    # 2. Invariant and Correctness Verification
    # -------------------------------------------------------------------------

    def test_task_lifecycle_and_metrics(self):
        async def success_coro(val):
            await asyncio.sleep(0.01)
            return val * 2

        async def failing_coro():
            await asyncio.sleep(0.01)
            raise RuntimeError("Intentional Failure")

        async def launch():
            reactor_task = asyncio.create_task(self.reactor.run())
            await asyncio.sleep(0.01)

            t1 = self.reactor.submit(success_coro(21))
            t2 = self.reactor.submit(success_coro(10))
            t3 = self.reactor.submit(failing_coro())

            res1 = await t1
            res2 = await t2
            self.assertEqual(res1, 42)
            self.assertEqual(res2, 20)

            with self.assertRaises(RuntimeError):
                await t3

            await asyncio.sleep(0.05)
            metrics = self.reactor.get_metrics()
            self.assertEqual(metrics['tasks_submitted'], 3)
            self.assertEqual(metrics['tasks_completed'], 2)
            self.assertEqual(metrics['tasks_failed'], 1)

            self.reactor.stop()
            await reactor_task

        asyncio.run(launch())

    def test_timer_execution_ordering(self):
        execution_order = []

        def record(name):
            execution_order.append(name)

        async def launch():
            reactor_task = asyncio.create_task(self.reactor.run())
            await asyncio.sleep(0.01)

            # Schedule out of order, verify execution strictly by expiration order
            self.reactor.schedule_timer(0.