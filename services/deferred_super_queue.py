import json
import os
import sqlite3
import threading
import time
import uuid
from typing import Dict, List, Optional, Tuple

from adapters.dhan.http_session import get_session
from validator.instruments.dhan_store import DhanStore
from orchestrator.super_order import DhanSuperOrderOrchestrator, DhanSuperOrderError

# Exchange segment mapping for both quote and order placement
API_SEGMENT_MAP = {
    "NSE": "NSE_EQ",
    "BSE": "BSE_EQ",
    "NFO": "NSE_FNO",
    "NSE_FNO": "NSE_FNO",
    "BFO": "BSE_FNO",
    "BSE_FNO": "BSE_FNO",
    "MCX": "MCX_COMM",
}

DB_PATH = os.path.join(os.path.dirname(__file__), "deferred_super.db")
POLL_INTERVAL_SECONDS = 3


class DeferredSuperQueue:
    _lock = threading.Lock()
    _worker_started = False

    @classmethod
    def init_db(cls):
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS deferred_buys (
                    id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    exchange TEXT NOT NULL,
                    exchange_segment TEXT NOT NULL,
                    security_id TEXT NOT NULL,
                    trigger_price REAL NOT NULL,
                    tolerance_pct REAL NOT NULL,
                    last_ltp REAL,
                    payload_json TEXT NOT NULL,
                    error TEXT,
                    order_id TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    client_id TEXT NOT NULL,
                    access_token TEXT NOT NULL
                )
                """
            )

    @classmethod
    def enqueue(cls, *, order_data: Dict, client_id: str, access_token: str, trigger_price: float, tolerance_pct: float, security_id: str, exchange_segment: str):
        now = time.strftime("%Y-%m-%dT%H:%M:%S")
        row_id = str(uuid.uuid4())
        payload_json = json.dumps(order_data)
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute(
                """
                INSERT INTO deferred_buys (id, status, symbol, exchange, exchange_segment, security_id, trigger_price, tolerance_pct, last_ltp, payload_json, error, order_id, created_at, updated_at, client_id, access_token)
                VALUES (?, 'pending', ?, ?, ?, ?, ?, ?, NULL, ?, NULL, NULL, ?, ?, ?, ?)
                """,
                (
                    row_id,
                    order_data.get("symbol"),
                    order_data.get("exchange"),
                    exchange_segment,
                    security_id,
                    float(trigger_price),
                    float(tolerance_pct),
                    payload_json,
                    now,
                    now,
                    client_id,
                    access_token,
                ),
            )
        return row_id

    @classmethod
    def list_pending(cls) -> List[Tuple]:
        with sqlite3.connect(DB_PATH) as conn:
            return list(
                conn.execute(
                    """
                    SELECT id, symbol, exchange_segment, security_id, trigger_price, tolerance_pct, payload_json, client_id, access_token
                    FROM deferred_buys
                    WHERE status = 'pending'
                    """
                )
            )

    @classmethod
    def update_ltp(cls, row_id: str, ltp: float):
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute(
                "UPDATE deferred_buys SET last_ltp = ?, updated_at = ? WHERE id = ?",
                (ltp, time.strftime("%Y-%m-%dT%H:%M:%S"), row_id),
            )

    @classmethod
    def mark_placed(cls, row_id: str, order_id: str):
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute(
                "UPDATE deferred_buys SET status = 'placed', order_id = ?, updated_at = ? WHERE id = ?",
                (order_id, time.strftime("%Y-%m-%dT%H:%M:%S"), row_id),
            )

    @classmethod
    def mark_failed(cls, row_id: str, error: str):
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute(
                "UPDATE deferred_buys SET status = 'failed', error = ?, updated_at = ? WHERE id = ?",
                (error[:500], time.strftime("%Y-%m-%dT%H:%M:%S"), row_id),
            )

    @classmethod
    def clear(cls):
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute("DELETE FROM deferred_buys")

    @classmethod
    def list_all(cls) -> List[Dict]:
        with sqlite3.connect(DB_PATH) as conn:
            rows = conn.execute(
                """
                SELECT id, status, symbol, exchange, exchange_segment, security_id, trigger_price, tolerance_pct, last_ltp, error, order_id, created_at, updated_at
                FROM deferred_buys
                ORDER BY created_at DESC
                """
            ).fetchall()
        return [
            {
                "id": r[0],
                "status": r[1],
                "symbol": r[2],
                "exchange": r[3],
                "exchange_segment": r[4],
                "security_id": r[5],
                "trigger_price": r[6],
                "tolerance_pct": r[7],
                "last_ltp": r[8],
                "error": r[9],
                "order_id": r[10],
                "created_at": r[11],
                "updated_at": r[12],
            }
            for r in rows
        ]

    @classmethod
    def start_worker_once(cls):
        with cls._lock:
            if cls._worker_started:
                return
            cls._worker_started = True
            thread = threading.Thread(target=cls._worker_loop, daemon=True)
            thread.start()

    @classmethod
    def _worker_loop(cls):
        while True:
            try:
                pending = cls.list_pending()
                if not pending:
                    time.sleep(POLL_INTERVAL_SECONDS)
                    continue

                # Build payload grouped by exchangeSegment
                payload: Dict[str, List[str]] = {}
                meta: Dict[str, Dict] = {}
                for row in pending:
                    row_id, symbol, exch_seg, sec_id, trig, tol, payload_json, client_id, access_token = row
                    payload.setdefault(exch_seg, []).append(str(sec_id))
                    meta[row_id] = {
                        "symbol": symbol,
                        "exchange_segment": exch_seg,
                        "security_id": str(sec_id),
                        "trigger_price": float(trig),
                        "tolerance_pct": float(tol),
                        "payload_json": payload_json,
                        "client_id": client_id,
                        "access_token": access_token,
                    }

                first_meta = next(iter(meta.values())) if meta else None
                if first_meta is None:
                    time.sleep(POLL_INTERVAL_SECONDS)
                    continue

                ltp_map = fetch_ltp(
                    payload=payload,
                    client_id=first_meta["client_id"],
                    access_token=first_meta["access_token"],
                )

                for row_id, info in meta.items():
                    sec_id = info["security_id"]
                    exch_seg = info["exchange_segment"]
                    ltp = None
                    if exch_seg in ltp_map and sec_id in ltp_map[exch_seg]:
                        ltp = ltp_map[exch_seg][sec_id]
                    if ltp is None:
                        continue

                    cls.update_ltp(row_id, ltp)

                    trig = info["trigger_price"]
                    tol = info["tolerance_pct"]
                    lower = trig * (1 - tol / 100.0)
                    upper = trig * (1 + tol / 100.0)
                    if ltp < lower or ltp > upper:
                        continue

                    try:
                        payload_dict = json.loads(info["payload_json"])
                        orch = DhanSuperOrderOrchestrator(client_id=info["client_id"], access_token=info["access_token"])
                        result = orch.place_super_order(payload_dict)
                        order_id = result.get("orderId", "")
                        cls.mark_placed(row_id, order_id)
                    except Exception as e:  # pragma: no cover
                        cls.mark_failed(row_id, str(e))
            except Exception:
                # Avoid dying the loop; wait a bit and retry
                time.sleep(POLL_INTERVAL_SECONDS)
            time.sleep(POLL_INTERVAL_SECONDS)


def fetch_ltp(*, payload: Dict[str, List[str]], client_id: str, access_token: str) -> Dict[str, Dict[str, float]]:
    """Fetch LTP using Dhan marketfeed LTP API.

    payload format: {"NSE_EQ": ["11536"], "NSE_FNO": ["49081"]}
    Returns: {"NSE_EQ": {"11536": 4520.0}, ...}
    """
    if not payload:
        return {}

    url = "https://api.dhan.co/v2/marketfeed/ltp"
    headers = {
        "Content-Type": "application/json",
        "access-token": access_token,
        "client-id": client_id,
    }
    resp = get_session().post(url, headers=headers, json=payload, timeout=5)
    if resp.status_code != 200:
        raise RuntimeError(f"LTP fetch failed HTTP {resp.status_code}: {resp.text}")
    data = resp.json()
    out: Dict[str, Dict[str, float]] = {}
    for seg, seg_data in data.get("data", {}).items():
        inner = {}
        for sec_id, body in seg_data.items():
            lp = body.get("last_price")
            if lp is not None:
                inner[str(sec_id)] = float(lp)
        if inner:
            out[seg] = inner
    return out


def resolve_instrument(symbol: str, exchange: str, strike_price=None, expiry_date=None, option_type=None):
    DhanStore.load()
    inst = None
    if strike_price is not None or expiry_date is not None or option_type is not None:
        inst = DhanStore.lookup_by_details(symbol, strike_price=strike_price, expiry_date=expiry_date, option_type=option_type)
    if inst is None:
        inst = DhanStore.lookup_symbol(symbol)
    if inst is None:
        raise ValueError(f"Symbol '{symbol}' not found in Dhan instruments")
    api_seg = API_SEGMENT_MAP.get(exchange, exchange)
    return inst, api_seg


def start_deferred_worker():
    DeferredSuperQueue.init_db()
    DeferredSuperQueue.start_worker_once()
*** End Patch