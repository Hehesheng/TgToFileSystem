import asyncio
import json
import bisect
import time
import re
import rsa
import os
import functools
import collections
import traceback
import logging
from collections import OrderedDict
from typing import Union, Optional

from telethon import TelegramClient, types, hints, events
from fastapi import Request

import configParse
from backend import apiutils
from backend.UserManager import UserManager

logger = logging.getLogger(__file__.split("/")[-1])

class TgFileSystemClient(object):
    @functools.total_ordering
    class MediaChunkHolder(object):
        waiters: collections.deque[asyncio.Future]
        requester: list[Request] = []
        chunk_id: int = 0
        is_done: bool = False

        def __init__(self, chat_id: int, msg_id: int, start: int, target_len: int, mem: Optional[bytes] = None) -> None:
            self.chat_id = chat_id
            self.msg_id = msg_id
            self.start = start
            self.target_len = target_len
            self.mem = mem or bytes()
            self.length = len(self.mem)
            self.waiters = collections.deque()

        def __repr__(self) -> str:
            return f"MediaChunk,start:{self.start},len:{self.length}"

        def __eq__(self, other: 'TgFileSystemClient.MediaChunkHolder'):
            if isinstance(other, int):
                return self.start == other
            return self.start == other.start

        def __le__(self, other: 'TgFileSystemClient.MediaChunkHolder'):
            if isinstance(other, int):
                return self.start <= other
            return self.start <= other.start

        def __gt__(self, other: 'TgFileSystemClient.MediaChunkHolder'):
            if isinstance(other, int):
                return self.start > other
            return self.start > other.start

        def __add__(self, other):
            if isinstance(other, bytes):
                self.append_chunk_mem(other)
            elif isinstance(other, TgFileSystemClient.MediaChunkHolder):
                self.append_chunk_mem(other.mem)
            else:
                raise RuntimeError("does not suported this type to add")

        def is_completed(self) -> bool:
            return self.length >= self.target_len

        def set_done(self) -> None:
            # self.is_done = True
            # self.notify_waiters()
            self.requester.clear()
            
        def notify_waiters(self) -> None:
            while self.waiters:
                waiter = self.waiters.popleft()
                if not waiter.done():
                    waiter.set_result(None)

        def _set_chunk_mem(self, mem: Optional[bytes]) -> None:
            self.mem = mem
            self.length = len(self.mem)
            if self.length > self.target_len:
                raise RuntimeWarning(
                    f"MeidaChunk Overflow:start:{self.start},len:{self.length},tlen:{self.target_len}")

        def append_chunk_mem(self, mem: bytes) -> None:
            self.mem = self.mem + mem
            self.length = len(self.mem)
            if self.length > self.target_len:
                raise RuntimeWarning(
                    f"MeidaChunk Overflow:start:{self.start},len:{self.length},tlen:{self.target_len}")
            self.notify_waiters()

        def add_chunk_requester(self, req: Request) -> None:
            self.requester.append(req)

        async def is_disconneted(self) -> bool:
            while self.requester:
                res = await self.requester[0].is_disconnected()
                if res:
                    self.requester.pop(0)
                    continue
                return res
            return True

        async def wait_chunk_update(self) -> None:
            if self.is_done:
                return
            waiter = asyncio.Future()
            self.waiters.append(waiter)
            try:
                await waiter
            except:
                waiter.cancel()
                try:
                    self.waiters.remove(waiter)
                except ValueError:
                    pass

    class MediaChunkHolderManager(object):
        MAX_CACHE_SIZE = 1024 * 1024 * 1024  # 1Gb
        current_cache_size: int = 0
        # chat_id -> msg_id -> offset -> mem
        chunk_cache: dict[int, dict[int,
                                    list['TgFileSystemClient.MediaChunkHolder']]] = {}
        # ChunkHolderId -> ChunkHolder
        unique_chunk_id: int = 0
        chunk_lru: OrderedDict[int, 'TgFileSystemClient.MediaChunkHolder']

        def __init__(self) -> None:
            self.chunk_lru = OrderedDict()

        def _get_media_msg_cache(self, msg: types.Message) -> Optional[list['TgFileSystemClient.MediaChunkHolder']]:
            chat_cache = self.chunk_cache.get(msg.chat_id)
            if chat_cache is None:
                return None
            return chat_cache.get(msg.id)

        def _get_media_chunk_cache(self, msg: types.Message, start: int) -> Optional['TgFileSystemClient.MediaChunkHolder']:
            msg_cache = self._get_media_msg_cache(msg)
            if msg_cache is None or len(msg_cache) == 0:
                return None
            pos = bisect.bisect_left(msg_cache, start)
            if pos == len(msg_cache):
                pos = pos - 1
                if msg_cache[pos].start <= start and msg_cache[pos].start + msg_cache[pos].target_len > start:
                    return msg_cache[pos]
                return None
            elif msg_cache[pos].start == start:
                return msg_cache[pos]
            elif pos > 0:
                pos = pos - 1
                if msg_cache[pos].start <= start and msg_cache[pos].start + msg_cache[pos].target_len > start:
                    return msg_cache[pos]
                return None
            return None

        def _remove_pop_chunk(self, pop_chunk: 'TgFileSystemClient.MediaChunkHolder') -> None:
            self.chunk_cache[pop_chunk.chat_id][pop_chunk.msg_id].remove(
                pop_chunk.start)
            self.current_cache_size -= pop_chunk.target_len
            if len(self.chunk_cache[pop_chunk.chat_id][pop_chunk.msg_id]) == 0:
                self.chunk_cache[pop_chunk.chat_id].pop(pop_chunk.msg_id)
                if len(self.chunk_cache[pop_chunk.chat_id]) == 0:
                    self.chunk_cache.pop(pop_chunk.chat_id)

        def get_media_chunk(self, msg: types.Message, start: int, lru: bool = True) -> Optional['TgFileSystemClient.MediaChunkHolder']:
            res = self._get_media_chunk_cache(msg, start)
            if res is None:
                return None
            if lru:
                self.chunk_lru.move_to_end(res.chunk_id)
            return res

        def set_media_chunk(self, chunk: 'TgFileSystemClient.MediaChunkHolder') -> None:
            cache_chat = self.chunk_cache.get(chunk.chat_id)
            if cache_chat is None:
                self.chunk_cache[chunk.chat_id] = {}
                cache_chat = self.chunk_cache[chunk.chat_id]
            cache_msg = cache_chat.get(chunk.msg_id)
            if cache_msg is None:
                cache_chat[chunk.msg_id] = []
                cache_msg = cache_chat[chunk.msg_id]
            chunk.chunk_id = self.unique_chunk_id
            self.unique_chunk_id += 1
            bisect.insort(cache_msg, chunk)
            self.chunk_lru[chunk.chunk_id] = chunk
            self.current_cache_size += chunk.target_len
            while self.current_cache_size > self.MAX_CACHE_SIZE:
                dummy = self.chunk_lru.popitem(last=False)
                self._remove_pop_chunk(dummy[1])

        def cancel_media_chunk(self, chunk: 'TgFileSystemClient.MediaChunkHolder') -> None:
            cache_chat = self.chunk_cache.get(chunk.chat_id)
            if cache_chat is None:
                return
            cache_msg = cache_chat.get(chunk.msg_id)
            if cache_msg is None:
                return
            dummy = self.chunk_lru.pop(chunk.chunk_id, None)
            if dummy is None:
                return
            self._remove_pop_chunk(dummy)

    MAX_WORKER_ROUTINE = 4
    SINGLE_NET_CHUNK_SIZE = 256 * 1024  # 256kb
    SINGLE_MEDIA_SIZE = 5 * 1024 * 1024  # 5mb
    api_id: int
    api_hash: str
    session_name: str
    proxy_param: dict[str, any]
    client: TelegramClient
    media_chunk_manager: MediaChunkHolderManager
    dialogs_cache: Optional[hints.TotalList] = None
    msg_cache: list[types.Message] = []
    worker_routines: list[asyncio.Task] = []
    # task should: (task_id, callabledFunc)
    task_queue: asyncio.Queue
    task_id: int = 0
    me: Union[types.User, types.InputPeerUser]
    # client config
    client_param: configParse.TgToFileSystemParameter.ClientConfigPatameter

    def __init__(self, session_name: str, param: configParse.TgToFileSystemParameter, db: UserManager) -> None:
        self.api_id = param.tgApi.api_id
        self.api_hash = param.tgApi.api_hash
        self.session_name = session_name
        self.proxy_param = {
            'proxy_type': param.proxy.proxy_type,
            'addr': param.proxy.addr,
            'port': param.proxy.port,
        } if param.proxy.enable else {}
        self.client_param = next((client_param for client_param in param.clients if client_param.token == session_name), configParse.TgToFileSystemParameter.ClientConfigPatameter())
        self.task_queue = asyncio.Queue()
        self.client = TelegramClient(
            f"{os.path.dirname(__file__)}/db/{self.session_name}.session", self.api_id, self.api_hash, proxy=self.proxy_param)
        self.media_chunk_manager = TgFileSystemClient.MediaChunkHolderManager()
        self.db = db

    def __del__(self) -> None:
        if self.client.loop.is_running():
            self.client.loop.create_task(self.stop())
        else:
            self.client.loop.run_until_complete(self.stop())

    def __repr__(self) -> str:
        if not self.client.is_connected:
            return f"client disconnected, session_name:{self.session_name}"
        return f"client connected, session_name:{self.session_name}, username:{self.me.username}, phone:{self.me.phone}, detail:{self.me.stringify()}"

    def _check_before_call(func):
        def call_check_wrapper(self, *args, **kwargs):
            if not self.is_valid():
                raise RuntimeError("Client does not run.")
            result = func(self, *args, **kwargs)
            return result
        return call_check_wrapper

    def _acheck_before_call(func):
        async def call_check_wrapper(self, *args, **kwargs):
            if not self.is_valid():
                raise RuntimeError("Client does not run.")
            result = await func(self, *args, **kwargs)
            return result
        return call_check_wrapper

    @_check_before_call
    def to_dict(self) -> dict:
        return self.me.to_dict()

    @_check_before_call
    def to_json(self) -> str:
        return self.me.to_json()

    def is_valid(self) -> bool:
        return self.client.is_connected() and self.me is not None

    @_check_before_call
    def _register_update_event(self, from_users: list[int] = []) -> None:
        @self.client.on(events.NewMessage(incoming=True, from_users=from_users))
        async def _incoming_new_message_handler(event) -> None:
            msg: types.Message = event.message
            self.db.insert_by_message(self.me, msg)

    async def start(self) -> None:
        if self.is_valid():
            return
        if not self.client.is_connected():
            await self.client.connect()
        self.me = await self.client.get_me()
        if self.me is None:
            raise RuntimeError(
                f"The {self.session_name} Client Does Not Login")
        for _ in range(self.MAX_WORKER_ROUTINE):
            worker_routine = self.client.loop.create_task(
                self._worker_routine_handler())
            self.worker_routines.append(worker_routine)
        if len(self.client_param.whitelist_chat) > 0:
            self._register_update_event(from_users=self.client_param.whitelist_chat)
            # await self.task_queue.put((self._get_unique_task_id(), self._cache_whitelist_chat()))
            await self.task_queue.put((self._get_unique_task_id(), self._cache_whitelist_chat2()))

    async def stop(self) -> None:
        await self.client.loop.create_task(self._cancel_tasks())
        while not self.task_queue.empty():
            self.task_queue.get_nowait()
            self.task_queue.task_done()
        await self.client.disconnect()

    async def _cancel_tasks(self) -> None:
        for t in self.worker_routines:
            try:
                t.cancel()
            except Exception as err:
                logger.error(f"{err=}")

    async def _cache_whitelist_chat2(self):
        for chat_id in self.client_param.whitelist_chat:
            async for msg in self.client.iter_messages(chat_id):
                if len(self.db.get_msg_by_unique_id(self.db.generate_unique_id_by_msg(self.me, msg))) != 0:
                    continue
                self.db.insert_by_message(self.me, msg)

    async def _cache_whitelist_chat(self):
        for chat_id in self.client_param.whitelist_chat:
            # update newest msg
            newest_msg = self.db.get_newest_msg_by_chat_id(chat_id)
            if len(newest_msg) > 0:
                newest_msg = newest_msg[0]
                async for msg in self.client.iter_messages(chat_id):
                    if msg.id <= self.db.get_column_msg_id(newest_msg):
                        break
                    self.db.insert_by_message(self.me, msg)
            # update oldest msg
            oldest_msg = self.db.get_oldest_msg_by_chat_id(chat_id)
            if len(oldest_msg) > 0:
                oldest_msg = oldest_msg[0]
                offset = self.db.get_column_msg_id(oldest_msg)
                async for msg in self.client.iter_messages(chat_id, offset_id=offset):
                    self.db.insert_by_message(self.me, msg)
            else:
                async for msg in self.client.iter_messages(chat_id):
                    self.db.insert_by_message(self.me, msg)
            

    @_acheck_before_call
    async def get_message(self, chat_id: int, msg_id: int) -> types.Message:
        msg = await self.client.get_messages(chat_id, ids=msg_id)
        return msg

    @_acheck_before_call
    async def get_dialogs(self, limit: int = 10, offset: int = 0, refresh: bool = False) -> hints.TotalList:
        if self.dialogs_cache is not None and refresh is False:
            return self.dialogs_cache[offset:offset+limit]
        self.dialogs_cache = await self.client.get_dialogs()
        return self.dialogs_cache[offset:offset+limit]

    async def _worker_routine_handler(self) -> None:
        while self.client.is_connected():
            try:
                task = await self.task_queue.get()
                await task[1]
            except Exception as err:
                logger.error(f"{err=}")
            finally:
                self.task_queue.task_done()
            
    def _get_unique_task_id(self) -> int:
        self.task_id += 1
        return self.task_id

    async def _get_offset_msg_id(self, chat_id: int, offset: int) -> int:
        if offset != 0:
            begin = await self.client.get_messages(chat_id, limit=1)
            if len(begin) == 0:
                return hints.TotalList()
            first_id = begin[0].id
            offset = first_id + offset
        return offset

    @_acheck_before_call
    async def get_messages(self, chat_id: int, limit: int = 10, offset: int = 0) -> hints.TotalList:
        offset = await self._get_offset_msg_id(chat_id, offset)
        res_list = await self.client.get_messages(chat_id, limit=limit, offset_id=offset)
        return res_list

    @_acheck_before_call
    async def get_messages_by_search(self, chat_id: int, search_word: str, limit: int = 10, offset: int = 0, inner_search: bool = False, ignore_case: bool = False) -> hints.TotalList:
        offset = await self._get_offset_msg_id(chat_id, offset)
        if inner_search:
            res_list = await self.client.get_messages(chat_id, limit=limit, offset_id=offset, search=search_word)
            return res_list
        # search by myself
        res_list = hints.TotalList()
        cnt = 0
        async for msg in self.client.iter_messages(chat_id, offset_id=offset):
            if cnt >= 1_000:
                break
            cnt += 1
            if msg.text.find(search_word) == -1 and apiutils.get_message_media_name(msg).find(search_word) == -1:
                continue
            res_list.append(msg)
            if len(res_list) >= limit:
                break
        return res_list
    
    async def get_messages_by_search_db(self, chat_id: int, search_word: str, limit: int = 10, offset: int = 0, inc: bool = False, ignore_case: bool = False) -> list[any]:
        if chat_id not in self.client_param.whitelist_chat:
            return []
        res = self.db.get_msg_by_chat_id_and_keyword(chat_id, search_word, limit=limit, offset=offset, inc=inc, ignore_case=ignore_case)
        res = [self.db.get_column_msg_js(v) for v in res]
        return res

    async def _download_media_chunk(self, msg: types.Message, media_holder: MediaChunkHolder) -> None:
        try:
            offset = media_holder.start + media_holder.length
            target_size = media_holder.target_len - media_holder.length
            remain_size = target_size
            async for chunk in self.client.iter_download(msg, offset=offset, chunk_size=self.SINGLE_NET_CHUNK_SIZE):
                if not isinstance(chunk, bytes):
                    chunk = chunk.tobytes()
                remain_size -= len(chunk)
                if remain_size <= 0:
                    media_holder.append_chunk_mem(
                        chunk[:len(chunk)+remain_size])
                    break
                media_holder.append_chunk_mem(chunk)
        except asyncio.CancelledError as err:
            logger.warning(f"cancel holder:{media_holder}")
            self.media_chunk_manager.cancel_media_chunk(media_holder)
        except Exception as err:
            logger.error(
                f"_download_media_chunk err:{err=},{offset=},{target_size=},{media_holder},\r\n{traceback.format_exc()}")
        finally:
            media_holder.set_done()
            logger.debug(
                f"downloaded chunk:{time.time()}.{offset=},{target_size=},{media_holder}")

    async def streaming_get_iter(self, msg: types.Message, start: int, end: int, req: Request):
        try:
            logger.debug(
                f"new steaming request:{msg.chat_id=},{msg.id=},[{start}:{end}]")
            cur_task_id = self._get_unique_task_id()
            pos = start
            while not await req.is_disconnected() and pos <= end:
                cache_chunk = self.media_chunk_manager.get_media_chunk(
                    msg, pos)
                if cache_chunk is None:
                    # post download task
                    # align pos download task
                    file_size = msg.media.document.size
                    # align_pos = pos // self.SINGLE_MEDIA_SIZE * self.SINGLE_MEDIA_SIZE
                    align_pos = pos
                    align_size = min(self.SINGLE_MEDIA_SIZE,
                                     file_size - align_pos)
                    holder = TgFileSystemClient.MediaChunkHolder(
                        msg.chat_id, msg.id, align_pos, align_size)
                    holder.add_chunk_requester(req)
                    self.media_chunk_manager.set_media_chunk(holder)
                    await self.task_queue.put((cur_task_id, self._download_media_chunk(msg, holder)))
                elif not cache_chunk.is_completed():
                    # yield return completed part
                    # await untill completed or pos > end
                    cache_chunk.add_chunk_requester(req)
                    while pos < cache_chunk.start + cache_chunk.target_len and pos <= end:
                        if await req.is_disconnected():
                            break
                        offset = pos - cache_chunk.start
                        if offset >= cache_chunk.length:
                            await cache_chunk.wait_chunk_update()
                            continue
                        need_len = min(cache_chunk.length -
                                       offset, end - pos + 1)
                        pos = pos + need_len
                        yield cache_chunk.mem[offset:offset+need_len]
                else:
                    offset = pos - cache_chunk.start
                    if offset >= cache_chunk.length:
                        raise RuntimeError(
                            f"lru cache missed!{pos=},{cache_chunk=}")
                    need_len = min(cache_chunk.length - offset, end - pos + 1)
                    pos = pos + need_len
                    yield cache_chunk.mem[offset:offset+need_len]
        except Exception as err:
            traceback.print_exc()
            logger.error(f"stream iter:{err=}")
        finally:
            async def _cancel_task_by_id(task_id: int):
                for _ in range(self.task_queue.qsize()):
                    task = self.task_queue.get_nowait()
                    self.task_queue.task_done()
                    if task[0] != task_id:
                        self.task_queue.put_nowait(task)
            await self.client.loop.create_task(_cancel_task_by_id(cur_task_id))
            logger.debug(f"yield quit,{msg.chat_id=},{msg.id=},[{start}:{end}]")

    def __enter__(self):
        raise NotImplementedError

    def __exit__(self):
        raise NotImplementedError

    async def __aenter__(self):
        await self.start()

    async def __aexit__(self):
        await self.stop()
