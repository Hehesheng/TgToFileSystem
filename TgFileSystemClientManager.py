from typing import Any
from TgFileSystemClient import TgFileSystemClient


class TgFileSystemClientManager(object):
    MAX_MANAGE_CLIENTS: int = 10
    clients: dict[int, TgFileSystemClient]
    
    def __init__(self) -> None:
        pass
    
    def push_client(self, client: TgFileSystemClient) -> int:
        """
        push client to manager.

        Arguments
            client

        Returns
            client id

        """
        self.clients[id(client)] = client
        return id(client)
        
    def get_client(self, client_id: int) -> TgFileSystemClient:
        client = self.clients.get(client_id)
        return client
    
    

if __name__ == "__main__":
    import configParse
    t: TgFileSystemClient = TgFileSystemClient(configParse.get_TgToFileSystemParameter())
    print(f"{t.session_name=}")
