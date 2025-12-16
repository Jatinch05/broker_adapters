from adapters.base import BaseAdapter
from adapters.dhan.errors import DhanOrderError


class DhanAdapter(BaseAdapter):
    """
    SDK-backed Dhan adapter.
    """

    def __init__(self, dhan_client):
        """
        dhan_client must already be authenticated.
        """
        if dhan_client is None:
            raise ValueError("DhanAdapter requires an authenticated dhan_client")

        self.dhan = dhan_client

    # ------------------------------------------------
    # Normal orders (SDK-backed)
    # ------------------------------------------------

    def place_order(self, intent):
        raise NotImplementedError("Normal order placement not wired yet")

    def modify_order(self, order_id, params):
        return self.dhan.modify_order(order_id, params)

    def cancel_order(self, order_id):
        return self.dhan.cancel_order(order_id)

    # ------------------------------------------------
    # Read APIs
    # ------------------------------------------------

    def get_order(self, order_id):
        return self.dhan.get_order(order_id)

    def get_orders(self):
        return self.dhan.get_orders()

    def get_trades(self):
        return self.dhan.get_trades()

    def get_positions(self):
        return self.dhan.get_positions()

    def get_holdings(self):
        return self.dhan.get_holdings()

    def get_margins(self):
        return self.dhan.get_funds()

    def get_ltp(self, symbol):
        return self.dhan.get_ltp(symbol)

    def get_quote(self, symbol):
        return self.dhan.get_quote(symbol)

    # ------------------------------------------------
    # WebSocket (future)
    # ------------------------------------------------

    def start_ws(self):
        raise NotImplementedError

    def stop_ws(self):
        raise NotImplementedError
