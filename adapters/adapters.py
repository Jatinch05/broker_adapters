from abc import abstractmethod, ABC

class BaseAdapter(ABC):

    @abstractmethod
    def place_order(intent):
        pass

    @abstractmethod
    def modify_order(order_id, params):
        pass

    @abstractmethod
    def cancel_order(order_id):
        pass

    @abstractmethod
    def get_order(order_id):
        pass

    @abstractmethod
    def get_orders():
        pass

    @abstractmethod
    def get_trades():
        pass

    @abstractmethod
    def get_positions():
        pass

    @abstractmethod
    def get_holdings():
        pass

    @abstractmethod
    def get_margins():
        pass

    @abstractmethod
    def get_ltp(symbol):
        pass

    @abstractmethod
    def get_quote(symbol):
        pass

    @abstractmethod
    def start_ws():
        pass

    @abstractmethod
    def stop_ws():
        pass

    @abstractmethod
    def authenticate():
        pass


class DhanAdapter(BaseAdapter):

    def __init__(self):
        self.dhan = None

    def place_order(intent):
        pass

    
    def modify_order(order_id, params):
        pass

    
    def cancel_order(order_id):
        pass

    
    def get_order(order_id):
        pass

    
    def get_orders():
        pass

    
    def get_trades():
        pass

    
    def get_positions():
        pass

    
    def get_holdings():
        pass

    
    def get_margins():
        pass

    
    def get_ltp(symbol):
        pass

    
    def get_quote(symbol):
        pass

    
    def start_ws():
        pass

    
    def stop_ws():
        pass

    
    def authenticate(self, client_id:int, access_token:str):
        from apis.dhan.auth import authenticate as auth
        dhan = auth(client_id, access_token)
        try:
            pos = dhan.positions()
            self.dhan = dhan
        except Exception as e:
            raise SystemExit(f"Error {e}. Enter the correct client_id, access_token")


