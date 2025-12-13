from dhanhq import dhanhq


def authenticate(client_id:int, access_token: str) -> dhanhq.dhanhq:
    '''
    Function for Dhan Authentication
    
    :param client_id: Dhan Client Id
    :type client_id: int
    :param access_token: Access token for authentication
    :type access_token: str
    :return: authenticated dhanhq object
    :rtype: Any
    '''
    dhan = dhanhq(client_id, access_token)
    return dhan

