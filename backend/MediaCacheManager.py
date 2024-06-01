import functools
import logging
import bisect
import collections
import asyncio
import collections
from typing import Union, Optional

import diskcache
from fastapi import Request
from telethon import types

logger = logging.getLogger(__file__.split("/")[-1])

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

    def __eq__(self, other: 'MediaChunkHolder'):
        if isinstance(other, int):
            return self.start == other
        return self.start == other.start

    def __le__(self, other: 'MediaChunkHolder'):
        if isinstance(other, int):
            return self.start <= other
        return self.start <= other.start

    def __gt__(self, other: 'MediaChunkHolder'):
        if isinstance(other, int):
            return self.start > other
        return self.start > other.start

    def __add__(self, other: Union['MediaChunkHolder', bytes]):
        if isinstance(other, MediaChunkHolder):
            other = other.mem
        self.append_chunk_mem(other)

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
                                list[MediaChunkHolder]]] = {}
    # ChunkHolderId -> ChunkHolder
    unique_chunk_id: int = 0
    chunk_lru: collections.OrderedDict[int, MediaChunkHolder]

    def __init__(self) -> None:
        self.chunk_lru = collections.OrderedDict()

    def _get_media_msg_cache(self, msg: types.Message) -> Optional[list[MediaChunkHolder]]:
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

    def _remove_pop_chunk(self, pop_chunk: MediaChunkHolder) -> None:
        self.chunk_cache[pop_chunk.chat_id][pop_chunk.msg_id].remove(
            pop_chunk.start)
        self.current_cache_size -= pop_chunk.target_len
        if len(self.chunk_cache[pop_chunk.chat_id][pop_chunk.msg_id]) == 0:
            self.chunk_cache[pop_chunk.chat_id].pop(pop_chunk.msg_id)
            if len(self.chunk_cache[pop_chunk.chat_id]) == 0:
                self.chunk_cache.pop(pop_chunk.chat_id)

    def get_media_chunk(self, msg: types.Message, start: int, lru: bool = True) -> Optional[MediaChunkHolder]:
        res = self._get_media_chunk_cache(msg, start)
        if res is None:
            return None
        if lru:
            self.chunk_lru.move_to_end(res.chunk_id)
        return res

    def set_media_chunk(self, chunk: MediaChunkHolder) -> None:
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

    def cancel_media_chunk(self, chunk: MediaChunkHolder) -> None:
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


@functools.total_ordering
class MediaBlockHolder(object):
    waiters: collections.deque[asyncio.Future]
    chunk_id: int = 0

    def __init__(self, chat_id: int, msg_id: int, start: int, target_len: int) -> None:
        self.chat_id = chat_id
        self.msg_id = msg_id
        self.start = start
        self.target_len = target_len
        self.mem = bytes()
        self.length = len(self.mem)
        self.waiters = collections.deque()

    def __repr__(self) -> str:
        return f"MediaBlockHolder,id:{self.chat_id}-{self.msg_id},start:{self.start},len:{self.length}/{self.target_len}"

    def __eq__(self, other: Union['MediaBlockHolder', int]):
        if isinstance(other, int):
            return self.start == other
        return self.start == other.start

    def __le__(self, other: Union['MediaBlockHolder', int]):
        if isinstance(other, int):
            return self.start <= other
        return self.start <= other.start

    def __gt__(self, other: Union['MediaBlockHolder', int]):
        if isinstance(other, int):
            return self.start > other
        return self.start > other.start

    def __add__(self, other: Union['MediaBlockHolder', bytes]):
        if isinstance(other, MediaBlockHolder):
            other = other.mem
        self.append_mem(other.mem)

    def is_completed(self) -> bool:
        return self.length >= self.target_len
        
    def notify_waiters(self) -> None:
        while self.waiters:
            waiter = self.waiters.popleft()
            if not waiter.done():
                waiter.set_result(None)

    def append_mem(self, mem: bytes) -> None:
        self.mem = self.mem + mem
        self.length = len(self.mem)
        self.notify_waiters()
        if self.length > self.target_len:
            logger.warning(f"MeidaBlock Overflow:{self}")

    async def wait_update(self) -> None:
        if self.is_completed():
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

@functools.total_ordering
class BlockInfo(object):
    def __init__(self, hashid: int, offset: int, length: int, in_mem: bool) -> None:
        self.hashid = hashid
        self.offset = offset
        self.length = length
        self.in_mem = in_mem

    def __eq__(self, other: Union['BlockInfo', int]):
        if isinstance(other, int):
            return self.offset == other
        return self.offset == other.offset

    def __le__(self, other: Union['BlockInfo', int]):
        if isinstance(other, int):
            return self.offset <= other
        return self.offset <= other.offset

class MediaBlockHolderManager(object):

    DEFAULT_MAX_CACHE_SIZE = 1024 * 1024 * 1024  # 1Gb
    # chat_id -> msg_id -> list[BlockInfo]
    chunk_cache: dict[int, dict[int, list[BlockInfo]]] = {}

    def __init__(self, limit_size: int = DEFAULT_MAX_CACHE_SIZE, dir: str = 'cache') -> None:
        pass

