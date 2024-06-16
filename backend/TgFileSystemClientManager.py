import asyncio
import time
import base64
import hashlib
import rsa
import os
from enum import IntEnum, unique, auto
import time
import traceback
import logging

from backend.TgFileSystemClient import TgFileSystemClient
from backend.UserManager import UserManager
from backend.MediaCacheManager import MediaChunkHolderManager
import configParse

logger = logging.getLogger(__file__.split("/")[-1])


@unique
class EnumSignLevel(IntEnum):
    ADMIN = auto()
    NORMAL = auto()
    VIST = auto()
    NONE = auto()


class TgFileSystemClientManager(object):
    TIME_MS_24HOURS: int = 24 * 60 * 60 * 1000
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
        self._init_rsa_keys()
        if self.loop.is_running():
            self.loop.create_task(self._start_clients())
        else:
            self.loop.run_until_complete(self._start_clients())

    def __del__(self) -> None:
        self.clients.clear()

    async def _start_clients(self) -> None:
        # init cache clients
        for client_config in self.param.clients:
            client = self.create_client(client_config.name)
            self._register_client(client)
        for _, client in self.clients.items():
            try:
                if not client.is_valid():
                    await client.start()
            except Exception as err:
                logger.warning(f"start client: {err=}, {traceback.format_exc()}")
        self.is_init = True

    def _init_rsa_keys(self):
        key_dir = f"{os.path.dirname(__file__)}/db"
        pub_key_path = f"{key_dir}/pub.pem"
        pri_key_path = f"{key_dir}/pri.pem"
        if not os.path.isfile(pub_key_path) or not os.path.isfile(pri_key_path):
            self.public_key, self.private_key = rsa.newkeys(512)
            with open(pub_key_path, "wb") as f:
                f.write(self.public_key.save_pkcs1())
            with open(pri_key_path, "wb") as f:
                f.write(self.private_key.save_pkcs1())
        else:
            with open(pub_key_path, "rb") as f:
                self.public_key = rsa.PublicKey.load_pkcs1(f.read())
            with open(pri_key_path, "rb") as f:
                self.private_key = rsa.PrivateKey.load_pkcs1(f.read())

    def generate_sign(
        self, client_id: str, sign_type: EnumSignLevel = EnumSignLevel.NORMAL, salt: str = None, valid_time: int = -1
    ) -> str:
        timestamp = int(time.time())
        if valid_time == -1:
            timestamp += self.TIME_MS_24HOURS
        elif valid_time == 0:
            timestamp = 0
        else:
            timestamp += valid_time * 1000
        need_encrypt_str = f"ts={timestamp};l={sign_type.value};"
        if salt:
            need_encrypt_str += f"s={hashlib.md5(salt).hexdigest()[:8]};"
        # rsa 512 bits only
        valid_len = 512 // 8 - 11
        valid_len -= len(need_encrypt_str)
        # id=xxxxx;
        valid_len -= len("id=;")
        if valid_len < 0:
            logger.error(f"{need_encrypt_str=},{traceback.format_exc()}")
            raise RuntimeError(f"generate sign too big")
        real_client_id = client_id[:valid_len]
        if len(real_client_id) != len(client_id):
            logger.warning(f"client id too long: {client_id} -> {real_client_id}")
        need_encrypt_str += f"id={real_client_id};"
        need_encrypt_bin = need_encrypt_str.encode()
        sign_bin = rsa.encrypt(need_encrypt_bin, self.public_key)
        sign = base64.b64encode(sign_bin).decode()
        logger.info(f"generate {sign_type.name} sign: {sign}")
        return sign

    def parse_sign(self, sign: str) -> dict[str, any] | None:
        try:
            res_dict = {}
            sign_bin = base64.b64decode(sign)
            decrypt_bin = rsa.decrypt(sign_bin, self.private_key)
            decrypt_str = decrypt_bin.decode()
            for key_value_str in decrypt_str.split(";"):
                if key_value_str == "":
                    continue
                key, value = key_value_str.split("=")
                res_dict[key] = value
        except Exception as err:
            logger.warning(f"verify sign {err=}, {traceback.format_exc()}")
            return None
        return res_dict

    @staticmethod
    def get_sign_client_id(key_map: dict[str, any]) -> str:
        return key_map.get("id")

    def verify_sign(
        self,
        sign: str,
        client_id: str = None,
        v_ts: bool = True,
        target_level: EnumSignLevel = EnumSignLevel.NONE,
        salt: str = None,
    ) -> bool:
        key_map = self.parse_sign(sign)
        if not key_map:
            return False
        if client_id and (not key_map.get("id") or not client_id.startswith(key_map.get("id"))):
            return False
        if not key_map.get("l") or target_level.value < int(key_map.get("l")):
            return False
        if v_ts and int(key_map.get("ts", 0)) > 0 and (int(time.time()) - int(key_map.get("ts", 0)) > 0):
            return False
        if salt and hashlib.md5(key_map.get("s", "")).hexdigest() != salt:
            return False
        return True

    async def get_status(self) -> dict[str, any]:
        clients_status = [
            {"status": client.is_valid(), "name": client.session_name, "sign": self.generate_sign(client.session_name)}
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

    def create_client(self, client_id: str) -> TgFileSystemClient:
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
