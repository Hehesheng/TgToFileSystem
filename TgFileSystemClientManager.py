from typing import Any
import time
import hashlib
import os

from TgFileSystemClient import TgFileSystemClient
from UserManager import UserManager
import configParse


class TgFileSystemClientManager(object):
    MAX_MANAGE_CLIENTS: int = 10
    param: configParse.TgToFileSystemParameter
    clients: dict[str, TgFileSystemClient] = {}

    def __init__(self, param: configParse.TgToFileSystemParameter) -> None:
        self.param = param
        self.db = UserManager()

    def __del__(self) -> None:
        pass

    def check_client_session_exist(self, client_id: str) -> bool:
        return os.path.isfile(client_id + '.session')

    def generate_client_id(self) -> str:
        return hashlib.md5(
            (str(time.perf_counter()) + self.param.base.salt).encode('utf-8')).hexdigest()

    def create_client(self, client_id: str = None) -> TgFileSystemClient:
        if client_id is None:
            client_id = self.generate_client_id()
        client = TgFileSystemClient(client_id, self.param)
        return client

    def register_client(self, client: TgFileSystemClient) -> bool:
        self.clients[client.session_name] = client
        return True

    def deregister_client(self, client_id: str) -> bool:
        self.clients.pop(client_id)
        return True

    def get_client(self, client_id: str) -> TgFileSystemClient:
        client = self.clients.get(client_id)
        return client
    
    async def get_client_force(self, client_id: str) -> TgFileSystemClient:
        client = self.get_client(client_id)
        if client is None:
            if not self.check_client_session_exist(client_id):
                raise RuntimeError("Client session does not found.")
            client = self.create_client(client_id=client_id)
        if not client.is_valid():
            await client.start()
            self.register_client(client)
        return client


if __name__ == "__main__":
    import configParse
    # t: TgFileSystemClient = TgFileSystemClient(configParse.get_TgToFileSystemParameter())
    print(f"{t.session_name=}")
