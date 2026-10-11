"""
A pool of VM slots shared by every submission a judge process grades at once.

The judge grades each submission in a forked worker process, so the pool is built on `multiprocessing` primitives
and must be created in the main process before any worker is forked; forked workers then share it.

Rules:
- A submission's first VM is always granted: it blocks until a slot is free, so a new submission can at least compile
  and start even when the pool is busy.
- Extra VMs are granted only within the submission's fair share, `capacity // active submissions`, so several
  submissions split the pool evenly instead of the first one taking everything. The share is re-evaluated on every
  request, so it grows as other submissions finish.
"""

import multiprocessing
import threading
import time
from typing import Optional, Set

# Bytes per owned-VM id slot: DigitalOcean ids are 36-character UUIDs, Docker ids 64 hex characters.
OWNED_SLOT = 64


class VMPool:
    def __init__(self, capacity: int) -> None:
        self.capacity = capacity
        self._slots = multiprocessing.BoundedSemaphore(capacity)
        self._active = multiprocessing.Value('i', 0)
        # Ids of VMs some submission owns right now, so a sweeper in the main process can tell this judge's leftover
        # VMs (whose submission ended but whose deletion failed, e.g. under API rate limiting) from live ones.
        # Plain shared memory: sessions run in processes forked after this pool is built, and a Manager proxy
        # inherited across the fork proved unreliable there, which let the sweeper delete VMs in use.
        self._owned = multiprocessing.Array('c', OWNED_SLOT * capacity)
        self._owned_lock = multiprocessing.Lock()

    def own(self, vm_id: str) -> None:
        key = vm_id.encode()[:OWNED_SLOT]
        with self._owned_lock:
            free = None
            for i in range(self.capacity):
                slot = self._slot(i)
                if slot == key:
                    return
                if not slot and free is None:
                    free = i
            if free is not None:
                self._owned[free * OWNED_SLOT : free * OWNED_SLOT + len(key)] = key

    def disown(self, vm_id: str) -> None:
        key = vm_id.encode()[:OWNED_SLOT]
        with self._owned_lock:
            for i in range(self.capacity):
                if self._slot(i) == key:
                    self._owned[i * OWNED_SLOT : (i + 1) * OWNED_SLOT] = b'\0' * OWNED_SLOT

    def owned(self) -> Set[str]:
        with self._owned_lock:
            return {self._slot(i).decode() for i in range(self.capacity) if self._slot(i)}

    def _slot(self, i: int) -> bytes:
        return bytes(self._owned[i * OWNED_SLOT : (i + 1) * OWNED_SLOT]).rstrip(b'\0')

    def register(self) -> None:
        """A submission starts grading."""
        with self._active.get_lock():
            self._active.value += 1

    def unregister(self) -> None:
        with self._active.get_lock():
            self._active.value -= 1

    def fair_share(self) -> int:
        with self._active.get_lock():
            active = max(1, self._active.value)
        return max(1, self.capacity // active)

    def acquire_first(self, cancelled: Optional[threading.Event] = None) -> bool:
        """Waits for a slot for a submission's first VM. Returns False only if cancelled while waiting."""
        while True:
            if self._slots.acquire(timeout=0.5):
                return True
            if cancelled is not None and cancelled.is_set():
                return False

    def acquire_extra(self, held: int) -> bool:
        """A slot for an additional VM of a submission already holding `held`, if within its fair share and free."""
        if held >= self.fair_share():
            return False
        return self._slots.acquire(block=False)

    def wait_for_extra(self, held: int, cancelled: threading.Event, poll: float = 1.0) -> bool:
        """Like `acquire_extra`, but keeps trying until it succeeds or `cancelled` is set."""
        while not cancelled.is_set():
            if self.acquire_extra(held):
                return True
            time.sleep(poll)
        return False

    def release(self) -> None:
        self._slots.release()
