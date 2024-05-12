import asyncio
import json
import bisect
import time
import re
import rsa
import functools
import collections
import traceback
from collections import OrderedDict
from typing import Union, Optional

from telethon import TelegramClient, types, hints

import configParse
import apiutils


class TgFileSystemClient(object):
    @functools.total_ordering
    class MediaChunkHolder(object):
        waiters: collections.deque[asyncio.Future]
        chunk_id: int = 0

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
            while self.waiters:
                waiter = self.waiters.popleft()
                if not waiter.done():
                    waiter.set_result(None)

        async def wait_chunk_update(self):
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

        def get_media_chunk(self, msg: types.Message, start: int, lru: bool = True) -> Optional['TgFileSystemClient.MediaChunkHolder']:
            res = self._get_media_chunk_cache(msg, start)
            if res is None:
                return None
            if lru:
                self.chunk_lru.move_to_end(res.chunk_id)
            return res

        def set_media_chunk(self, chunk: 'TgFileSystemClient.MediaChunkHolder') -> None:
            if self.chunk_cache.get(chunk.chat_id) is None:
                self.chunk_cache[chunk.chat_id] = {}
            if self.chunk_cache[chunk.chat_id].get(chunk.msg_id) is None:
                self.chunk_cache[chunk.chat_id][chunk.msg_id] = []
            chunk.chunk_id = self.unique_chunk_id
            self.unique_chunk_id += 1
            bisect.insort(self.chunk_cache[chunk.chat_id][chunk.msg_id], chunk)
            self.chunk_lru[chunk.chunk_id] = chunk
            self.current_cache_size += chunk.target_len
            while self.current_cache_size > self.MAX_CACHE_SIZE:
                dummy = self.chunk_lru.popitem(last=False)
                pop_chunk = dummy[1]
                self.chunk_cache[pop_chunk.chat_id][pop_chunk.msg_id].remove(
                    pop_chunk.start)
                self.current_cache_size -= pop_chunk.target_len
                if len(self.chunk_cache[pop_chunk.chat_id][pop_chunk.msg_id]) == 0:
                    self.chunk_cache[pop_chunk.chat_id].pop(pop_chunk.msg_id)
                    if len(self.chunk_cache[pop_chunk.chat_id]) == 0:
                        self.chunk_cache.pop(pop_chunk.chat_id)

    MAX_DOWNLOAD_ROUTINE = 4
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
    download_routines: list[asyncio.Task] = []
    # task should: (task_id, callabledFunc)
    task_queue: asyncio.Queue
    task_id: int = 0
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
        self.task_queue = asyncio.Queue()
        self.client = TelegramClient(
            self.session_name, self.api_id, self.api_hash, proxy=self.proxy_param)
        self.media_chunk_manager = TgFileSystemClient.MediaChunkHolderManager()

    def __del__(self) -> None:
        if self.client.loop.is_running():
            self.client.loop.create_task(self.stop())
        else:
            self.client.loop.run_until_complete(self.stop())

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
        if self.is_valid():
            return
        if not self.client.is_connected():
            await self.client.connect()
        self.me = await self.client.get_me()
        if self.me is None:
            raise RuntimeError(
                f"The {self.session_name} Client Does Not Login")
        for _ in range(self.MAX_DOWNLOAD_ROUTINE):
            download_rt = self.client.loop.create_task(
                self._download_routine_handler())
            self.download_routines.append(download_rt)

    async def stop(self) -> None:
        await self.client.loop.create_task(self._cancel_tasks())
        while not self.task_queue.empty():
            self.task_queue.get_nowait()
            self.task_queue.task_done()
        await self.client.disconnect()

    async def _cancel_tasks(self) -> None:
        for t in self.download_routines:
            try:
                t.cancel()
            except Exception as err:
                print(f"{err=}")

    @_acall_before_check
    async def get_message(self, chat_id: int, msg_id: int) -> types.Message:
        msg = await self.client.get_messages(chat_id, ids=msg_id)
        return msg

    @_acall_before_check
    async def get_dialogs(self, limit: int = 10, offset: int = 0, refresh: bool = False) -> hints.TotalList:
        if self.dialogs_cache is not None and refresh is False:
            return self.dialogs_cache[offset:offset+limit]
        self.dialogs_cache = await self.client.get_dialogs()
        return self.dialogs_cache[offset:offset+limit]

    async def _download_routine_handler(self) -> None:
        while self.client.is_connected():
            task = await self.task_queue.get()
            await task[1]
            self.task_queue.task_done()
        print("task quit!!!!")

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
        cnt = 0
        async for msg in self.client.iter_messages(chat_id, offset_id=offset):
            if cnt >= 10_000:
                break
            cnt += 1
            if msg.text.find(search_word) == -1 and apiutils.get_message_media_name(msg).find(search_word) == -1:
                continue
            res_list.append(msg)
            if len(res_list) >= limit:
                break
        return res_list

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
        except Exception as err:
            print(
                f"_download_media_chunk err:{err=},{offset=},{target_size=},{media_holder}")
        finally:
            pass
            # print(
            #     f"downloaded chunk:{time.time()}.{offset=},{target_size=},{media_holder}")

    async def streaming_get_iter(self, msg: types.Message, start: int, end: int):
        try:
            # print(
            #     f"new steaming request:{msg.chat_id=},{msg.id=},[{start}:{end}]")
            self.task_id += 1
            cur_task_id = self.task_id
            pos = start
            while pos <= end:
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
                    self.media_chunk_manager.set_media_chunk(holder)
                    await self.task_queue.put((cur_task_id, self._download_media_chunk(msg, holder)))
                    # while self.task_queue.qsize() < self.MAX_DOWNLOAD_ROUTINE and align_pos <= end:
                    #     align_pos = align_pos + align_size
                    #     align_size = min(self.SINGLE_MEDIA_SIZE,
                    #                      file_size - align_pos)
                    #     cache_chunk = self.media_chunk_manager.get_media_chunk(
                    #         msg, align_pos, lru=False)
                    #     if cache_chunk is not None:
                    #         break
                    #     holder = TgFileSystemClient.MediaChunkHolder(
                    #         msg.chat_id, msg.id, align_pos, align_size)
                    #     self.media_chunk_manager.set_media_chunk(holder)
                    #     await self.task_queue.put((cur_task_id, self._download_media_chunk(msg, holder)))
                elif not cache_chunk.is_completed():
                    # yield return completed part
                    # await untill completed or pos > end
                    while pos < cache_chunk.start + cache_chunk.target_len and pos <= end:
                        offset = pos - cache_chunk.start
                        if offset >= cache_chunk.length:
                            await cache_chunk.wait_chunk_update()
                            continue
                        need_len = min(cache_chunk.length -
                                       offset, end - pos + 1)
                        # print(
                        #     f"return missed {need_len} bytes:[{pos}:{pos+need_len}].{cache_chunk=}")
                        pos = pos + need_len
                        yield cache_chunk.mem[offset:offset+need_len]
                else:
                    offset = pos - cache_chunk.start
                    if offset >= cache_chunk.length:
                        raise RuntimeError(
                            f"lru cache missed!{pos=},{cache_chunk=}")
                    need_len = min(cache_chunk.length - offset, end - pos + 1)
                    # print(
                    #     f"return hited {need_len} bytes:[{pos}:{pos+need_len}].{cache_chunk=}")
                    pos = pos + need_len
                    yield cache_chunk.mem[offset:offset+need_len]
        except Exception as err:
            traceback.print_exc()
            print(f"stream iter:{err=}")
        finally:
            async def _cancel_task_by_id(task_id: int):
                for _ in range(self.task_queue.qsize()):
                    task = self.task_queue.get_nowait()
                    self.task_queue.task_done()
                    if task[0] != task_id:
                        self.task_queue.put_nowait(task)
            await self.client.loop.create_task(_cancel_task_by_id(cur_task_id))
            # print("yield quit")

    def __enter__(self):
        raise NotImplementedError

    def __exit__(self):
        raise NotImplementedError

    async def __aenter__(self):
        await self.start()

    async def __aexit__(self):
        await self.stop()
