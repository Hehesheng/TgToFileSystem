import os
import functools
import logging
import bisect
import collections
import asyncio
import traceback
import hashlib
import collections
from typing import Union, Optional

import diskcache
from fastapi import Request
from telethon import types

logger = logging.getLogger(__file__.split("/")[-1])


@functools.total_ordering
class ChunkInfo(object):
    def __init__(self, md5id: str, chat_id: int, msg_id: int, start: int, length: int) -> None:
        self.id = md5id
        self.chat_id = chat_id
        self.msg_id = msg_id
        self.start = start
        self.length = length

    def __repr__(self) -> str:
        return f"chunkinfo:id:{self.id},cid:{self.chat_id},mid:{self.msg_id},offset:{self.start},len:{self.length}"

    def __eq__(self, other: Union["ChunkInfo", int]):
        if isinstance(other, int):
            return self.start == other
        return self.start == other.start

    def __le__(self, other: Union["ChunkInfo", int]):
        if isinstance(other, int):
            return self.start <= other
        return self.start <= other.start


@functools.total_ordering
class MediaChunkHolder(object):
    waiters: collections.deque[asyncio.Future]
    requesters: list[Request] = []
    unique_id: str = ""
    info: ChunkInfo

    @staticmethod
    def generate_id(chat_id: int, msg_id: int, start: int) -> str:
        return f"{chat_id}:{msg_id}:{start}"

    def __init__(self, chat_id: int, msg_id: int, start: int, target_len: int) -> None:
        self.unique_id = MediaChunkHolder.generate_id(chat_id, msg_id, start)
        self.info = ChunkInfo(hashlib.md5(self.unique_id.encode()).hexdigest(), chat_id, msg_id, start, target_len)
        self.mem = bytes()
        self.length = len(self.mem)
        self.waiters = collections.deque()

    def __repr__(self) -> str:
        return f"MediaChunk,unique_id:{self.unique_id},{self.info},mlen:{self.length}"

    def __eq__(self, other: Union["MediaChunkHolder", ChunkInfo, int]):
        if isinstance(other, int):
            return self.info.start == other
        if isinstance(other, ChunkInfo):
            return self.info.start == other.start
        return self.info.start == other.info.start

    def __le__(self, other: Union["MediaChunkHolder", ChunkInfo, int]):
        if isinstance(other, int):
            return self.info.start <= other
        if isinstance(other, ChunkInfo):
            return self.info.start <= other.start
        return self.info.start <= other.info.start

    def is_completed(self) -> bool:
        return self.length >= self.info.length

    @property
    def chunk_id(self) -> str:
        return self.info.id

    @property
    def start(self) -> int:
        return self.info.start

    @property
    def target_len(self) -> int:
        return self.info.length

    def notify_waiters(self) -> None:
        while self.waiters:
            waiter = self.waiters.popleft()
            if not waiter.done():
                waiter.set_result(None)

    def append_chunk_mem(self, mem: bytes) -> None:
        self.mem = self.mem + mem
        self.length = len(self.mem)
        if self.length > self.target_len:
            logger.warning(RuntimeWarning(
                f"MeidaChunk Overflow:start:{self.start},len:{self.length},tlen:{self.target_len}"))
        self.notify_waiters()

    def add_chunk_requester(self, req: Request) -> None:
        if self.is_completed():
            return
        self.requesters.append(req)

    async def is_disconneted(self) -> bool:
        while self.requesters:
            req = self.requesters[0]
            if not await req.is_disconnected():
                return False
            try:
                self.requesters.remove(req)
            except Exception as err:
                logger.warning(f"{err=}, trace:{traceback.format_exc()}")
                return False
        return True

    async def wait_chunk_update(self) -> None:
        if self.is_completed():
            return
        waiter = asyncio.Future()
        self.waiters.append(waiter)
        try:
            await waiter
        except:
            waiter.cancel()
            logger.warning("waiter cancel")
            try:
                self.waiters.remove(waiter)
            except ValueError:
                pass

    def try_clear_waiter_and_requester(self) -> bool:
        if not self.is_completed():
            return False
        # clear all waiter and requester
        self.notify_waiters()
        self.requesters.clear()
        return True 


class MediaChunkHolderManager(object):
    MAX_CACHE_SIZE = 2**31  # 2GB
    current_cache_size: int = 0
    # chunk unique id -> ChunkHolder
    disk_chunk_cache: diskcache.Cache
    # incompleted chunk
    incompleted_chunk: dict[str, MediaChunkHolder] = {}
    # chunk id -> ChunkInfo
    chunk_lru: collections.OrderedDict[str, ChunkInfo]
    # chat_id -> msg_id -> list[ChunkInfo]
    chunk_cache: dict[int, dict[int, list[ChunkInfo]]] = {}

    def __init__(self) -> None:
        self.chunk_lru = collections.OrderedDict()
        self.disk_chunk_cache = diskcache.Cache(
            f"{os.path.dirname(__file__)}/cache_media", size_limit=MediaChunkHolderManager.MAX_CACHE_SIZE * 2
        )
        self._restore_cache()

    def _restore_cache(self) -> None:
        for id in self.disk_chunk_cache.iterkeys():
            try:
                holder: MediaChunkHolder = self.disk_chunk_cache.get(id)
                if holder is not None:
                    self._set_media_chunk_index(holder.info)
            except Exception as err:
                logger.warning(f"restore, {err=},{traceback.format_exc()}")
        while self.current_cache_size > self.MAX_CACHE_SIZE:
            self._remove_pop_chunk()

    def get_chunk_holder_by_info(self, info: ChunkInfo) -> MediaChunkHolder:
        holder = self.incompleted_chunk.get(info.id)
        if holder is not None:
            return holder
        holder = self.disk_chunk_cache.get(info.id)
        return holder

    def _get_media_msg_cache(self, msg: types.Message) -> Optional[list[ChunkInfo]]:
        chat_cache = self.chunk_cache.get(msg.chat_id)
        if chat_cache is None:
            return None
        return chat_cache.get(msg.id)

    def _get_media_chunk_cache(self, msg: types.Message, start: int) -> Optional[MediaChunkHolder]:
        msg_cache = self._get_media_msg_cache(msg)
        if msg_cache is None or len(msg_cache) == 0:
            return None
        pos = bisect.bisect_left(msg_cache, start)
        if pos == len(msg_cache):
            pos = pos - 1
            if msg_cache[pos].start <= start and msg_cache[pos].start + msg_cache[pos].length > start:
                return self.get_chunk_holder_by_info(msg_cache[pos])
            return None
        elif msg_cache[pos].start == start:
            return self.get_chunk_holder_by_info(msg_cache[pos])
        elif pos > 0:
            pos = pos - 1
            if msg_cache[pos].start <= start and msg_cache[pos].start + msg_cache[pos].length > start:
                return self.get_chunk_holder_by_info(msg_cache[pos])
            return None
        return None

    def _remove_pop_chunk(self, pop_chunk: ChunkInfo = None) -> None:
        try:
            if pop_chunk is None:
                dummy = self.chunk_lru.popitem(last=False)
                pop_chunk = dummy[1]
            self.chunk_cache[pop_chunk.chat_id][pop_chunk.msg_id].remove(pop_chunk.start)
            self.current_cache_size -= pop_chunk.length
            if len(self.chunk_cache[pop_chunk.chat_id][pop_chunk.msg_id]) == 0:
                self.chunk_cache[pop_chunk.chat_id].pop(pop_chunk.msg_id)
                if len(self.chunk_cache[pop_chunk.chat_id]) == 0:
                    self.chunk_cache.pop(pop_chunk.chat_id)
            pop_holder = self.incompleted_chunk.get(pop_chunk.id)
            if pop_holder is not None:
                self.incompleted_chunk.pop(pop_chunk.id)
                return
            suc = self.disk_chunk_cache.delete(pop_chunk.id)
            if not suc:
                logger.warning(f"could not del, {pop_chunk}")
        except Exception as err:
            logger.warning(f"remove chunk,{err=},{traceback.format_exc()}")

    def create_media_chunk_holder(self, chat_id: int, msg_id: int, start: int, target_len: int) -> MediaChunkHolder:
        return MediaChunkHolder(chat_id, msg_id, start, target_len)

    def get_media_chunk(self, msg: types.Message, start: int, lru: bool = True) -> Optional[MediaChunkHolder]:
        res = self._get_media_chunk_cache(msg, start)
        logger.debug(f"get_media_chunk:{res}")
        if res is None:
            return None
        if lru:
            self.chunk_lru.move_to_end(res.chunk_id)
        return res

    def _set_media_chunk_index(self, info: ChunkInfo) -> None:
        self.chunk_lru[info.id] = info
        self.chunk_cache.setdefault(info.chat_id, {})
        self.chunk_cache[info.chat_id].setdefault(info.msg_id, [])
        bisect.insort(self.chunk_cache[info.chat_id][info.msg_id], info)
        self.current_cache_size += info.length

    def set_media_chunk(self, chunk: MediaChunkHolder) -> None:
        if chunk.is_completed():
            self.disk_chunk_cache.set(chunk.chunk_id, chunk)
        else:
            self.incompleted_chunk[chunk.chunk_id] = chunk
        self._set_media_chunk_index(chunk.info)
        while self.current_cache_size > self.MAX_CACHE_SIZE:
            self._remove_pop_chunk()

    def cancel_media_chunk(self, chunk: MediaChunkHolder) -> None:
        dummy = self.chunk_lru.pop(chunk.chunk_id, None)
        if dummy is None:
            return
        self._remove_pop_chunk(dummy)

    def move_media_chunk_to_disk(self, holder: MediaChunkHolder) -> bool:
        cache_holder = self.incompleted_chunk.pop(holder.chunk_id, None)
        if cache_holder is None:
            logger.warning(f"the holder not in mem, {holder}")
            return False
        if not holder.is_completed():
            logger.error(f"chunk not completed, but move to disk:{holder=}")
        logger.info(f"cache new chunk:{holder}")
        self.disk_chunk_cache.set(holder.chunk_id, holder)
        return True
