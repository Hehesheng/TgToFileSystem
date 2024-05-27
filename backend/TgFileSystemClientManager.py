from typing import Any
import asyncio
import time
import hashlib
import os
import logging

from backend.TgFileSystemClient import TgFileSystemClient
from backend.UserManager import UserManager
import configParse

logger = logging.getLogger(__file__.split("/")[-1])

class TgFileSystemClientManager(object):
    MAX_MANAGE_CLIENTS: int = 10
    param: configParse.TgToFileSystemParameter
    clients: dict[str, TgFileSystemClient] = {}

    def __init__(self, param: configParse.TgToFileSystemParameter) -> None:
        self.param = param
        self.db = UserManager()
        self.loop = asyncio.get_running_loop()
        if self.loop.is_running():
            self.loop.create_task(self._start_clients())
        else:
            self.loop.run_until_complete(self._start_clients())

    def __del__(self) -> None:
        self.clients.clear()
    
    async def _start_clients(self) -> None:
        # init cache clients
        for client_config in self.param.clients:
            client = self.create_client(client_id=client_config.token)
            if not client.is_valid():
                await client.start()
            self._register_client(client)

    def check_client_session_exist(self, client_id: str) -> bool:
        session_db_file = f"{os.path.dirname(__file__)}/db/{client_id}.session"
        return os.path.isfile(session_db_file)

    def generate_client_id(self) -> str:
        return hashlib.md5(
            (str(time.perf_counter()) + self.param.base.salt).encode('utf-8')).hexdigest()

    def create_client(self, client_id: str = None) -> TgFileSystemClient:
        if client_id is None:
            client_id = self.generate_client_id()
        client = TgFileSystemClient(client_id, self.param, self.db)
        return client

    def _register_client(self, client: TgFileSystemClient) -> bool:
        self.clients[client.session_name] = client
        return True

    def _unregister_client(self, client_id: str) -> bool:
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
            self._register_client(client)
        return client

