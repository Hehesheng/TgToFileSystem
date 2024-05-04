import asyncio
from typing import Union

from telethon import TelegramClient, types

import configParse


class TgFileSystemClient(object):
    api_id: int
    api_hash: str
    session_name: str
    proxy_param: dict[str, any]
    client: TelegramClient
    me: Union[types.User, types.InputPeerUser]

    def __init__(self, param: configParse.TgToFileSystemParameter) -> None:
        self.api_id = param.tgApi.api_id
        self.api_hash = param.tgApi.api_hash
        self.session_name = param.base.name
        self.proxy_param = {
            'proxy_type': param.proxy.proxy_type,
            'addr': param.proxy.addr,
            'port': param.proxy.port,
        } if param.proxy.enable else {}
        self.client = TelegramClient(
            self.session_name, self.api_id, self.api_hash, proxy=self.proxy_param)


    def __repr__(self) -> str:
        if not self.client.is_connected:
            return f"client disconnected, session_name:{self.session_name}"
        return f"client connected, session_name:{self.session_name}, username:{self.me.username}, phone:{self.me.phone}, detail:{self.me.stringify()}"

    async def init_client(self):
        self.me = await self.client.get_me()

    def __enter__(self):
        self.client.__enter__()
        self.client.loop.run_until_complete(self.init_client())

    def __exit__(self):
        self.client.__exit__()
        self.me = None

    async def __aenter__(self):
        await self.client.__enter__()
        await self.init_client()

    async def __aexit__(self):
        await self.client.__aexit__()
