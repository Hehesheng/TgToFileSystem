import asyncio
import time
import hashlib
import rsa
import os
import traceback
import logging

from backend.TgFileSystemClient import TgFileSystemClient
from backend.UserManager import UserManager
from backend.MediaCacheManager import MediaChunkHolderManager
import configParse

logger = logging.getLogger(__file__.split("/")[-1])


class TgFileSystemClientManager(object):
    MAX_MANAGE_CLIENTS: int = 10
    is_init: bool = False
    param: configParse.TgToFileSystemParameter
    clients: dict[str, TgFileSystemClient] = {}
    # rsa key
    cache_sign: str
    public_key: rsa.PublicKey
    private_key: rsa.PrivateKey

    @classmethod
    def get_instance(cls):
        if not hasattr(TgFileSystemClientManager, "_instance"):
            TgFileSystemClientManager._instance = TgFileSystemClientManager(configParse.get_TgToFileSystemParameter())
        return TgFileSystemClientManager._instance

    def __init__(self, param: configParse.TgToFileSystemParameter) -> None:
        self.param = param
        self.db = UserManager()
        self.loop = asyncio.get_running_loop()
        self.media_chunk_manager = MediaChunkHolderManager()
        self.public_key, self.private_key = rsa.newkeys(1024)
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
            self._register_client(client)
        for _, client in self.clients.items():
            try:
                if not client.is_valid():
                    await client.start()
            except Exception as err:
                logger.warning(f"start client: {err=}, {traceback.format_exc()}")
        self.is_init = True

    async def get_status(self) -> dict[str, any]:
        clients_status = [
            {
                "status": client.is_valid(),
            }
            for _, client in self.clients.items()
        ]
        return {"init": self.is_init, "clients": clients_status}

    async def login_clients(self) -> str:
        for _, client in self.clients.items():
            login_url = await client.login()
            if login_url != "":
                return login_url
        return ""

    def check_client_session_exist(self, client_id: str) -> bool:
        session_db_file = f"{os.path.dirname(__file__)}/db/{client_id}.session"
        return os.path.isfile(session_db_file)

    def generate_client_id(self) -> str:
        return hashlib.md5((str(time.perf_counter()) + self.param.base.salt).encode("utf-8")).hexdigest()

    def create_client(self, client_id: str = None) -> TgFileSystemClient:
        if client_id is None:
            client_id = self.generate_client_id()
        client = TgFileSystemClient(client_id, self.param, self.db, self.media_chunk_manager)
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

    def get_first_client(self) -> TgFileSystemClient:
        for client in self.clients.values():
            return client
        return None

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
