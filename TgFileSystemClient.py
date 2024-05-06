import asyncio
import json
from typing import Union, Optional

from telethon import TelegramClient, types, hints

import configParse
import apiutils


class TgFileSystemClient(object):
    api_id: int
    api_hash: str
    session_name: str
    proxy_param: dict[str, any]
    client: TelegramClient
    dialogs_cache: Optional[hints.TotalList] = None
    me: Union[types.User, types.InputPeerUser]

    def __init__(self, session_name: str, param: configParse.TgToFileSystemParameter) -> None:
        self.api_id = param.tgApi.api_id
        self.api_hash = param.tgApi.api_hash
        self.session_name = session_name
        self.proxy_param = {
            'proxy_type': param.proxy.proxy_type,
            'addr': param.proxy.addr,
            'port': param.proxy.port,
        } if param.proxy.enable else {}
        self.client = TelegramClient(
            self.session_name, self.api_id, self.api_hash, proxy=self.proxy_param)

    def __del__(self) -> None:
        self.client.disconnect()

    def __repr__(self) -> str:
        if not self.client.is_connected:
            return f"client disconnected, session_name:{self.session_name}"
        return f"client connected, session_name:{self.session_name}, username:{self.me.username}, phone:{self.me.phone}, detail:{self.me.stringify()}"

    def _call_before_check(func):
        def call_check_wrapper(self, *args, **kwargs):
            if not self.is_valid():
                raise RuntimeError("Client does not run.")
            result = func(self, *args, **kwargs)
            return result
        return call_check_wrapper

    def _acall_before_check(func):
        async def call_check_wrapper(self, *args, **kwargs):
            if not self.is_valid():
                raise RuntimeError("Client does not run.")
            result = await func(self, *args, **kwargs)
            return result
        return call_check_wrapper

    @_call_before_check
    def to_dict(self) -> dict:
        return self.me.to_dict()

    @_call_before_check
    def to_json(self) -> str:
        return self.me.to_json()

    def is_valid(self) -> bool:
        return self.client.is_connected() and self.me is not None

    async def start(self) -> None:
        if not self.client.is_connected():
            await self.client.connect()
        self.me = await self.client.get_me()
        if self.me is None:
            raise RuntimeError(
                f"The {self.session_name} Client Does Not Login")

    async def stop(self) -> None:
        await self.client.disconnect()

    @_acall_before_check
    async def get_message(self, chat_id: int, msg_id: int) -> types.Message:
        msg = await self.client.get_messages(chat_id, ids=msg_id)
        return msg

    @_acall_before_check
    async def get_dialogs(self, limit: int = 10, offset: int = 0, refresh: bool = False) -> hints.TotalList:
        def _to_json(item) -> str:
            return json.dumps({"id": item.id, "is_channel": item.is_channel,
                               "is_group": item.is_group, "is_user": item.is_user, "name": item.name, })
        if self.dialogs_cache is not None and refresh is False:
            return self.dialogs_cache[offset:offset+limit]
        self.dialogs_cache = await self.client.get_dialogs()
        for item in self.dialogs_cache:
            item.to_json = _to_json
        return self.dialogs_cache[offset:offset+limit]

    async def _get_offset_msg_id(self, chat_id: int, offset: int) -> int:
        if offset != 0:
            begin = await self.client.get_messages(chat_id, limit=1)
            if len(begin) == 0:
                return hints.TotalList()
            first_id = begin[0].id
            offset = first_id + offset
        return offset

    @_acall_before_check
    async def get_messages(self, chat_id: int, limit: int = 10, offset: int = 0) -> hints.TotalList:
        offset = await self._get_offset_msg_id(chat_id, offset)
        res_list = await self.client.get_messages(chat_id, limit=limit, offset_id=offset)
        return res_list

    @_acall_before_check
    async def get_messages_by_search(self, chat_id: int, search_word: str, limit: int = 10, offset: int = 0, inner_search: bool = False) -> hints.TotalList:
        offset = await self._get_offset_msg_id(chat_id, offset)
        if inner_search:
            res_list = await self.client.get_messages(chat_id, limit=limit, offset_id=offset, search=search_word)
            return res_list
        # search by myself
        res_list = hints.TotalList()
        async for msg in self.client.iter_messages(chat_id, offset_id=offset):
            if msg.text.find(search_word) == -1 and apiutils.get_message_media_name(msg).find(search_word) == -1:
                continue
            res_list.append(msg)
            if len(res_list) >= limit:
                break
        return res_list

    def __enter__(self):
        raise NotImplemented

    def __exit__(self):
        raise NotImplemented

    async def __aenter__(self):
        await self.start()

    async def __aexit__(self):
        await self.stop()
