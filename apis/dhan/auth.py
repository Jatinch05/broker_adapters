from typing import Tuple
from dhanhq import dhanhq

DHAN_BASE_URL = "https://api.dhan.co"



class DhanAuthError(Exception):
    """Raised when Dhan authentication fails."""
    pass


def authenticate(client_id: str, access_token: str):
    """
    Authenticate with Dhan and return an authenticated dhanhq client.

    This function:
    - does NOT store credentials
    - does NOT retry
    - does NOT refresh tokens
    """

    if not client_id or not access_token:
        raise DhanAuthError("client_id and access_token are required")

    try:
        dhan = dhanhq(client_id, access_token)

        dhan.get_positions()

        return dhan

    except Exception as e:
        raise DhanAuthError(
            f"Dhan authentication failed. Check client_id / access_token. Error: {e}"
        )
