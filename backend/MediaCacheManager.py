import functools
import logging
import collections
import asyncio

import diskcache

logger = logging.getLogger(__file__.split("/")[-1])

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

    def __eq__(self, other: 'MediaBlockHolder'|int):
        if isinstance(other, int):
            return self.start == other
        return self.start == other.start

    def __le__(self, other: 'MediaBlockHolder'|int):
        if isinstance(other, int):
            return self.start <= other
        return self.start <= other.start

    def __gt__(self, other: 'MediaBlockHolder'|int):
        if isinstance(other, int):
            return self.start > other
        return self.start > other.start

    def __add__(self, other: 'MediaBlockHolder'|bytes):
        if isinstance(other, bytes):
            self.append_mem(other)
        elif isinstance(other, MediaBlockHolder):
            self.append_mem(other.mem)
        else:
            raise RuntimeError(f"{self} can't add {type(other)}")

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

    def __eq__(self, other: 'BlockInfo'|int):
        if isinstance(other, int):
            return self.offset == other
        return self.offset == other.offset

    def __le__(self, other: 'BlockInfo'|int):
        if isinstance(other, int):
            return self.offset <= other
        return self.offset <= other.offset

class MediaBlockHolderManager(object):

    DEFAULT_MAX_CACHE_SIZE = 1024 * 1024 * 1024  # 1Gb
    # chat_id -> msg_id -> list[BlockInfo]
    chunk_cache: dict[int, dict[int, list[BlockInfo]]] = {}

    def __init__(self, limit_size: int = DEFAULT_MAX_CACHE_SIZE, dir: str = 'cache') -> None:
        pass

