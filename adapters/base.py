from abc import ABC, abstractmethod


class BaseAdapter(ABC):
    """
    Abstract base adapter for all brokers.
    """

    @abstractmethod
    def place_order(self, intent):
        pass

    @abstractmethod
    def modify_order(self, order_id, params):
        pass

    @abstractmethod
    def cancel_order(self, order_id):
        pass

    @abstractmethod
    def get_order(self, order_id):
        pass

    @abstractmethod
    def get_orders(self):
        pass

    @abstractmethod
    def get_trades(self):
        pass

    @abstractmethod
    def get_positions(self):
        pass

    @abstractmethod
    def get_holdings(self):
        pass

    @abstractmethod
    def get_margins(self):
        pass

    @abstractmethod
    def get_ltp(self, symbol):
        pass

    @abstractmethod
    def get_quote(self, symbol):
        pass

    @abstractmethod
    def start_ws(self):
        pass

    @abstractmethod
    def stop_ws(self):
        pass
