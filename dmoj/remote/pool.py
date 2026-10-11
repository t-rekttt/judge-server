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
- Warm VMs: a submission that finishes cleanly parks its VMs instead of deleting them, and the next submission takes
  them (any problem, any user) instead of creating new ones. A parked VM keeps its slot. A reaper in the main process
  deletes VMs parked longer than the idle timeout, since they cost money while idle.
- Standby VMs: the main process keeps a reserve of parked VMs at all times (`warm_reserve_per_judge` per judge), so
  a submission arriving at an idle judge starts on a VM that already runs instead of waiting for one to be created.
  The reaper never deletes the newest `reserve` parked VMs, whatever their age.
"""

import json
import logging
import multiprocessing
import threading
import time
from typing import Any, Dict, List, Optional, Set

logger = logging.getLogger(__name__)

# Bytes per owned-VM id slot: DigitalOcean ids are 36-character UUIDs, Docker ids 64 hex characters.
OWNED_SLOT = 64
# Bytes per warm-VM record: a JSON `VM` (id, url, token and the platform's endpoint headers).
WARM_SLOT = 1024


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
        # Parked VMs, each with the time it was parked. Same shared memory as the ownership record, for the same
        # reason; a parked VM stays in the ownership record until it is deleted.
        self._warm = multiprocessing.Array('c', WARM_SLOT * capacity)
        self._warm_since = multiprocessing.Array('d', capacity)
        self._warm_lock = multiprocessing.Lock()

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
        """Gives a slot back. Giving back more than were taken is a bookkeeping bug elsewhere, which is logged and
        otherwise ignored: a crashed reaper or session thread would cost VMs, a missing slot only capacity."""
        try:
            self._slots.release()
        except ValueError:
            logger.error('VM pool slot released that was never taken', stack_info=True)

    # Warm VMs. Records are JSON so any backend's `VM` fits; a record's slot stays taken while it is parked.
    def park(self, vm: Any) -> bool:
        """Parks a VM for reuse, keeping its slot. Returns False if there is no room (the caller then deletes it)."""
        record = json.dumps(vm._asdict()).encode()
        if len(record) > WARM_SLOT:
            return False
        with self._warm_lock:
            for i in range(self.capacity):
                if not self._warm_record(i):
                    self._warm[i * WARM_SLOT : i * WARM_SLOT + len(record)] = record
                    self._warm_since[i] = time.monotonic()
                    return True
        return False

    def take_warm(self) -> Optional[Dict[str, Any]]:
        """The most recently parked VM, as `VM` fields, with its slot; None if there is none."""
        with self._warm_lock:
            newest = None
            for i in range(self.capacity):
                if self._warm_record(i) and (newest is None or self._warm_since[i] > self._warm_since[newest]):
                    newest = i
            if newest is None:
                return None
            record = self._warm_record(newest)
            self._clear_warm(newest)
        return json.loads(record)

    def reap_warm(self, idle_seconds: float, keep: int = 0) -> List[Dict[str, Any]]:
        """
        Removes VMs parked longer than `idle_seconds` (all of them if 0), except the newest `keep`, which stay as the
        standby reserve. The caller deletes them and releases their slots.
        """
        now = time.monotonic()
        reaped = []
        with self._warm_lock:
            parked = sorted(
                (i for i in range(self.capacity) if self._warm_record(i)), key=lambda i: -self._warm_since[i]
            )
            for i in parked[keep:]:
                if now - self._warm_since[i] >= idle_seconds:
                    reaped.append(json.loads(self._warm_record(i)))
                    self._clear_warm(i)
        return reaped

    def acquire_standby(self) -> bool:
        """A slot for a standby VM, if one is free right now; standby VMs never wait for a slot."""
        return self._slots.acquire(block=False)

    def parked(self) -> List[Dict[str, Any]]:
        """Every parked VM, as `VM` fields, without taking any."""
        with self._warm_lock:
            return [json.loads(self._warm_record(i)) for i in range(self.capacity) if self._warm_record(i)]

    def warm_count(self) -> int:
        with self._warm_lock:
            return sum(1 for i in range(self.capacity) if self._warm_record(i))

    def _warm_record(self, i: int) -> bytes:
        return bytes(self._warm[i * WARM_SLOT : (i + 1) * WARM_SLOT]).rstrip(b'\0')

    def _clear_warm(self, i: int) -> None:
        self._warm[i * WARM_SLOT : (i + 1) * WARM_SLOT] = b'\0' * WARM_SLOT
