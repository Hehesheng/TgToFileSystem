import asyncio
import time
import base64
import hashlib
import hmac
import os
import threading
from enum import IntEnum, unique, auto
import traceback
import logging

from backend.TgFileSystemClient import TgFileSystemClient
from backend.UserManager import UserManager
from backend.MediaCacheManager import MediaChunkHolderManager
import configParse

logger = logging.getLogger(__file__.split("/")[-1])

# Thread lock for singleton initialization
_instance_lock = threading.Lock()


@unique
class EnumSignLevel(IntEnum):
    ADMIN = auto()
    NORMAL = auto()
    VIST = auto()
    NONE = auto()


class TgFileSystemClientManager(object):
    TIME_SECONDS_24H: int = 24 * 60 * 60  # 24 hours in seconds
    SIGNATURE_LENGTH: int = 32  # 32 hex chars = 128 bits security
    MAX_MANAGE_CLIENTS: int = 10
    is_init: bool = False
    param: configParse.TgToFileSystemParameter
    clients: dict[str, TgFileSystemClient] = {}
    secret_key: bytes

    @classmethod
    def get_instance(cls):
        """Thread-safe singleton getter."""
        if not hasattr(TgFileSystemClientManager, "_instance"):
            with _instance_lock:
                if not hasattr(TgFileSystemClientManager, "_instance"):
                    TgFileSystemClientManager._instance = TgFileSystemClientManager(configParse.get_TgToFileSystemParameter())
        return TgFileSystemClientManager._instance

    def __init__(self, param: configParse.TgToFileSystemParameter) -> None:
        self.param = param
        self.db = UserManager()
        self.loop = asyncio.get_running_loop()
        self.media_chunk_manager = MediaChunkHolderManager()
        self._init_secret_key()
        if self.loop.is_running():
            self.loop.create_task(self._start_clients())
        else:
            self.loop.run_until_complete(self._start_clients())

    def __del__(self) -> None:
        self.clients.clear()

    async def _start_clients(self) -> None:
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

    def _init_secret_key(self):
        """Initialize HMAC secret key from file or generate new one."""
        key_dir = f"{os.path.dirname(__file__)}/db"
        key_path = f"{key_dir}/hmac_key.bin"
        try:
            with open(key_path, "rb") as f:
                self.secret_key = f.read()
            logger.info("Loaded existing HMAC secret key")
        except FileNotFoundError:
            self.secret_key = os.urandom(32)  # 256-bit key
            with open(key_path, "wb") as f:
                f.write(self.secret_key)
            logger.info("Generated new HMAC secret key")

    def _b64encode_safe(self, data: str) -> str:
        """URL-safe base64 encoding without padding."""
        encoded = base64.urlsafe_b64encode(data.encode()).decode()
        return encoded.rstrip("=")

    def _b64decode_safe(self, data: str) -> str:
        """URL-safe base64 decoding with padding restoration."""
        padding_needed = (4 - len(data) % 4) % 4
        data += "=" * padding_needed
        return base64.urlsafe_b64decode(data).decode()

    def generate_sign(
        self,
        client_id: str,
        sign_type: EnumSignLevel = EnumSignLevel.NORMAL,
        valid_seconds: int = -1,
    ) -> str:
        """
        Generate HMAC-SHA256 based signature.

        Token format: base64(payload|h=signature)
        payload: ts={expire_ts}|id={client_id_b64}|l={level}
        signature: HMAC-SHA256(payload, secret_key)[:32]

        Args:
            client_id: Client identifier
            sign_type: Sign level (ADMIN/NORMAL/VIST)
            valid_seconds: Token validity in seconds (-1=24h, must be >0)

        Returns:
            URL-safe base64 encoded token (~90 chars)

        Raises:
            ValueError: If valid_seconds <= 0 (except -1 for default)
        """
        # Calculate expiration timestamp (always expires)
        current_ts = int(time.time())
        if valid_seconds == -1:
            expire_ts = current_ts + self.TIME_SECONDS_24H
        elif valid_seconds <= 0:
            raise ValueError(f"valid_seconds must be positive or -1, got {valid_seconds}")
        else:
            expire_ts = current_ts + valid_seconds

        # Encode client_id to avoid special characters in payload
        encoded_id = self._b64encode_safe(client_id) if client_id else ""

        # Build payload
        payload = f"ts={expire_ts}|id={encoded_id}|l={sign_type.value}"

        # Generate HMAC signature (128 bits for adequate security)
        hmac_sig = hmac.new(self.secret_key, payload.encode(), hashlib.sha256).hexdigest()[:self.SIGNATURE_LENGTH]

        # Combine and encode
        full_token = f"{payload}|h={hmac_sig}"
        sign = self._b64encode_safe(full_token)

        logger.info(f"generate {sign_type.name} sign for {client_id}: expires={expire_ts}")
        return sign

    def parse_sign(self, sign: str) -> dict | None:
        """
        Parse and verify HMAC signature.

        Args:
            sign: URL-safe base64 encoded token

        Returns:
            Dict with ts, id, l fields if valid, None otherwise
        """
        try:
            full_token = self._b64decode_safe(sign)

            # Split payload and signature
            if "|h=" not in full_token:
                logger.warning("Invalid sign format: missing signature")
                return None

            payload, provided_sig = full_token.rsplit("|h=", 1)

            # Verify HMAC signature
            expected_sig = hmac.new(self.secret_key, payload.encode(), hashlib.sha256).hexdigest()[:self.SIGNATURE_LENGTH]
            if not hmac.compare_digest(provided_sig, expected_sig):
                logger.warning("HMAC signature mismatch")
                return None

            # Parse payload
            res = {}
            for part in payload.split("|"):
                if "=" in part:
                    key, value = part.split("=", 1)
                    if key == "id" and value:
                        value = self._b64decode_safe(value)  # Decode client_id
                    res[key] = value

            # Validate required fields
            if "ts" not in res or "id" not in res or "l" not in res:
                logger.warning("Missing required fields in sign")
                return None

            return res

        except Exception as err:
            logger.warning(f"parse sign error: {err=}")
            return None

    @staticmethod
    def get_sign_client_id(key_map: dict) -> str | None:
        return key_map.get("id")

    def verify_sign(
        self,
        sign: str,
        client_id: str = None,
        target_level: EnumSignLevel = EnumSignLevel.NONE,
    ) -> bool:
        """
        Verify signature and check constraints.

        Args:
            sign: Token to verify
            client_id: Expected client_id prefix match
            target_level: Minimum required sign level

        Returns:
            True if valid and not expired, False otherwise
        """
        key_map = self.parse_sign(sign)
        if not key_map:
            return False

        # Check client_id prefix match
        if client_id:
            sign_client_id = key_map.get("id", "")
            if not sign_client_id or not client_id.startswith(sign_client_id):
                logger.warning(f"client_id '{client_id}' does not start with expected prefix '{sign_client_id}'")
                return False

        # Check sign level (smaller number = higher privilege)
        # ADMIN=1 > NORMAL=2 > VIST=3 > NONE=4
        # Sign must have level <= target (higher or equal privilege)
        sign_level = int(key_map.get("l", 0))
        if sign_level > target_level.value:
            logger.warning(f"level mismatch: sign level {sign_level} insufficient for required level {target_level.value}")
            return False

        # Always check expiration (token must have lifetime)
        expire_ts = int(key_map.get("ts", 0))
        if expire_ts <= 0 or time.time() > expire_ts:
            logger.warning(f"sign expired or invalid ts: {expire_ts}")
            return False

        return True

    async def get_status(self) -> dict:
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
        session_file = f"{os.path.dirname(__file__)}/db/{client_id}.session"
        return os.path.isfile(session_file)

    def create_client(self, client_id: str) -> TgFileSystemClient:
        return TgFileSystemClient(client_id, self.param, self.db, self.media_chunk_manager)

    def _register_client(self, client: TgFileSystemClient) -> bool:
        self.clients[client.session_name] = client
        return True

    def _unregister_client(self, client_id: str) -> bool:
        self.clients.pop(client_id, None)
        return True

    def get_client(self, client_id: str) -> TgFileSystemClient | None:
        return self.clients.get(client_id)

    def get_first_client(self) -> TgFileSystemClient | None:
        for client in self.clients.values():
            return client
        return None

    async def get_client_force(self, client_id: str) -> TgFileSystemClient:
        client = self.get_client(client_id)
        if client is None:
            if not self.check_client_session_exist(client_id):
                raise RuntimeError("Client session does not found.")
            client = self.create_client(client_id)
        if not client.is_valid():
            await client.start()
            self._register_client(client)
        return client