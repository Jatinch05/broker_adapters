from __future__ import annotations

import threading
import time
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional


@dataclass
class ForeverOrder:
    id: str
    created_at: str
    status: str  # pending | running | triggered | failed | canceled
    interval_sec: int
    next_attempt_at: float
    attempt_count: int
    last_error: Optional[str]
    order_data: Dict[str, Any]
    result: Optional[Dict[str, Any]] = None
    client_id: str = field(repr=False, default="")
    access_token: str = field(repr=False, default="")


class ForeverOrderManager:
    def __init__(self) -> None:
        self._orders: Dict[str, ForeverOrder] = {}
        self._lock = threading.Lock()
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._rate_limit_times: List[float] = []
        self._rate_limit = 25
        self._rate_window = 1.0

    def _rate_limit_wait(self) -> None:
        now = time.time()
        self._rate_limit_times = [t for t in self._rate_limit_times if now - t < self._rate_window]
        if len(self._rate_limit_times) >= self._rate_limit:
            sleep_time = self._rate_window - (now - self._rate_limit_times[0])
            if sleep_time > 0:
                time.sleep(sleep_time)
        self._rate_limit_times.append(time.time())

    def start(self, orchestrator_factory) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()

        def worker_loop():
            while not self._stop_event.is_set():
                due: List[ForeverOrder] = []
                now = time.time()
                with self._lock:
                    for o in self._orders.values():
                        if o.status in ("pending", "running",):
                            if o.next_attempt_at <= now:
                                due.append(o)

                for order in due:
                    if self._stop_event.is_set():
                        break
                    # Build orchestrator with stored credentials per order
                    try:
                        orchestrator = orchestrator_factory(order.client_id, order.access_token)
                    except Exception as e:
                        # Credentials invalid; mark failed and schedule retry
                        with self._lock:
                            order.status = "failed"
                            order.last_error = f"Credential error: {e}"
                            order.next_attempt_at = time.time() + order.interval_sec
                        continue

                    with self._lock:
                        if order.status in ("canceled", "triggered"):
                            continue
                        order.status = "running"
                        order.attempt_count += 1

                    try:
                        # Respect Dhan rate limits shared in this manager
                        self._rate_limit_wait()
                        result = orchestrator.place_super_order(order.order_data)
                        with self._lock:
                            order.status = "triggered"
                            order.result = result
                            order.last_error = None
                    except Exception as e:
                        with self._lock:
                            order.status = "pending"
                            order.last_error = str(e)
                            order.next_attempt_at = time.time() + order.interval_sec

                # Sleep a bit between cycles to avoid busy loop
                self._stop_event.wait(0.5)

        self._thread = threading.Thread(target=worker_loop, name="ForeverOrderWorker", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=2)

    def create(self, *, client_id: str, access_token: str, order_data: Dict[str, Any], interval_sec: int = 30) -> str:
        oid = uuid.uuid4().hex
        fo = ForeverOrder(
            id=oid,
            created_at=datetime.now().isoformat(),
            status="pending",
            interval_sec=max(5, int(interval_sec)),
            next_attempt_at=time.time(),
            attempt_count=0,
            last_error=None,
            order_data=order_data,
            client_id=client_id,
            access_token=access_token,
        )
        with self._lock:
            self._orders[oid] = fo
        return oid

    def cancel(self, order_id: str) -> bool:
        with self._lock:
            o = self._orders.get(order_id)
            if not o:
                return False
            o.status = "canceled"
            return True

    def get(self, order_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            o = self._orders.get(order_id)
            if not o:
                return None
            d = asdict(o)
            # Do not expose credentials
            d.pop("client_id", None)
            d.pop("access_token", None)
            return d

    def list(self) -> List[Dict[str, Any]]:
        with self._lock:
            out: List[Dict[str, Any]] = []
            for o in self._orders.values():
                d = asdict(o)
                d.pop("client_id", None)
                d.pop("access_token", None)
                out.append(d)
            # Most recent first
            out.sort(key=lambda x: x["created_at"], reverse=True)
            return out
