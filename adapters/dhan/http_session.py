from __future__ import annotations

import threading
from typing import Optional

import requests


_thread_local = threading.local()


def get_session() -> requests.Session:
    """Return a thread-local requests.Session for connection reuse.

    Using a Session significantly reduces per-request overhead (TCP/TLS handshake)
    during bulk order placement.
    """
    sess: Optional[requests.Session] = getattr(_thread_local, "session", None)
    if sess is None:
        sess = requests.Session()
        _thread_local.session = sess
    return sess
