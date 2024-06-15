import asyncio
import json
import time
import re
import os
import functools
import traceback
import logging
from typing import Union, Optional, Literal, Callable

from telethon import TelegramClient, types, hints, events
from telethon.custom import QRLogin
from fastapi import Request

import configParse
from backend import apiutils
from backend.UserManager import UserManager
from backend.MediaCacheManager import MediaChunkHolder, MediaChunkHolderManager

logger = logging.getLogger(__file__.split("/")[-1])


class TgFileSystemClient(object):
    MAX_WORKER_ROUTINE = 8
    SINGLE_NET_CHUNK_SIZE = 256 * 1024  # 256kb
    SINGLE_MEDIA_SIZE = 5 * 1024 * 1024  # 5mb
    api_id: int
    api_hash: str
    session_name: str
    proxy_param: dict[str, any]
    client: TelegramClient
    media_chunk_manager: MediaChunkHolderManager
    dialogs_cache: Optional[hints.TotalList] = None
    worker_routines: list[asyncio.Task]
    qr_login: QRLogin | None = None
    login_task: asyncio.Task | None = None
    # task should: (task_id, callabledFunc)
    task_queue: asyncio.Queue
    task_id: int = 0
    me: Union[types.User, types.InputPeerUser] = None
    # client config
    client_param: configParse.TgToFileSystemParameter.ClientConfigPatameter

    def __init__(
        self,
        session_name: str,
        param: configParse.TgToFileSystemParameter,
        db: UserManager,
        chunk_manager: MediaChunkHolderManager,
    ) -> None:
        self.api_id = param.tgApi.api_id
        self.api_hash = param.tgApi.api_hash
        self.session_name = session_name
        self.proxy_param = (
            {
                "proxy_type": param.proxy.proxy_type,
                "addr": param.proxy.addr,
                "port": param.proxy.port,
            }
            if param.proxy.enable
            else {}
        )
        self.client_param = next(
            (client_param for client_param in param.clients if client_param.token == session_name),
            configParse.TgToFileSystemParameter.ClientConfigPatameter(),
        )
        self.task_queue = asyncio.Queue()
        self.client = TelegramClient(
            f"{os.path.dirname(__file__)}/db/{self.session_name}.session",
            self.api_id,
            self.api_hash,
            proxy=self.proxy_param,
        )
        self.media_chunk_manager = chunk_manager
        self.db = db
        self.worker_routines = []

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

    async def login(self, mode: Literal["phone", "qrcode"] = "qrcode") -> str:
        if self.is_valid():
            return ""
        if mode == "phone":
            raise NotImplementedError
        if self.qr_login is not None:
            return self.qr_login.url
        self.qr_login = await self.client.qr_login()

        async def wait_for_qr_login():
            try:
                await self.qr_login.wait()
                await self.start()
            except Exception as err:
                logger.warning(f"wait for login, {err=}, {traceback.format_exc()}")
            finally:
                self.login_task = None
                self.qr_login = None

        self.login_task = self.client.loop.create_task(wait_for_qr_login())
        return self.qr_login.url

    async def start(self) -> None:
        if self.is_valid():
            return
        if not self.client.is_connected():
            await self.client.connect()
        self.me = await self.client.get_me()
        if self.me is None:
            raise RuntimeError(f"The {self.session_name} Client Does Not Login")
        for _ in range(self.MAX_WORKER_ROUTINE):
            worker_routine = self.client.loop.create_task(self._worker_routine_handler())
            self.worker_routines.append(worker_routine)
        if len(self.client_param.whitelist_chat) > 0:
            self._register_update_event(from_users=self.client_param.whitelist_chat)
            await self.task_queue.put((self._get_unique_task_id(), self._cache_whitelist_chat()))

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
                logger.error(traceback.format_exc())

    async def _cache_whitelist_chat_full_policy(self, chat_id: int, callback: Callable = None):
        async for msg in self.client.iter_messages(chat_id):
            if len(self.db.get_msg_by_unique_id(UserManager.generate_unique_id_by_msg(self.me, msg))) != 0:
                continue
            self.db.insert_by_message(self.me, msg)
        if callback is not None:
            callback()
        logger.info(f"{chat_id} quit cache task.")

    async def _cache_whitelist_chat_lazy_policy(self, chat_id: int, callback: Callable = None):
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
        if callback is not None:
            callback()
        logger.info(f"{chat_id} quit cache task.")

    async def _cache_whitelist_chat(self):
        max_cache_tasks_num = TgFileSystemClient.MAX_WORKER_ROUTINE // 2
        tasks_sem = asyncio.Semaphore(value=max_cache_tasks_num)

        def _sem_release_callback():
            tasks_sem.release()

        for chat_id in self.client_param.whitelist_chat:
            await tasks_sem.acquire()
            await self.task_queue.put(
                (self._get_unique_task_id(), self._cache_whitelist_chat_lazy_policy(chat_id, callback=_sem_release_callback))
            )

    @_acheck_before_call
    async def get_message(self, chat_id: int | str, msg_id: int) -> types.Message:
        msg = await self.client.get_messages(chat_id, ids=msg_id)
        return msg

    @_acheck_before_call
    async def get_dialogs(self, limit: int = 10, offset: int = 0, refresh: bool = False) -> hints.TotalList:
        if self.dialogs_cache is not None and refresh is False:
            return self.dialogs_cache[offset : offset + limit]
        self.dialogs_cache = await self.client.get_dialogs()
        return self.dialogs_cache[offset : offset + limit]

    async def _worker_routine_handler(self) -> None:
        while self.client.is_connected():
            try:
                task = await self.task_queue.get()
                await task[1]
            except Exception as err:
                logger.error(f"{err=}")
                logger.error(traceback.format_exc())
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
    async def get_entity(self, chat_id_or_name) -> hints.Entity:
        return await self.client.get_entity(chat_id_or_name)

    @_acheck_before_call
    async def get_messages(self, chat_id: int, limit: int = 10, offset: int = 0) -> hints.TotalList:
        offset = await self._get_offset_msg_id(chat_id, offset)
        res_list = await self.client.get_messages(chat_id, limit=limit, offset_id=offset)
        return res_list

    @_acheck_before_call
    async def get_messages_by_search(
        self,
        chat_id: int,
        search_word: str,
        limit: int = 10,
        offset: int = 0,
        inner_search: bool = False,
        ignore_case: bool = False,
    ) -> hints.TotalList:
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

    async def get_messages_by_search_db(
        self,
        chat_ids: list[int],
        search_word: str,
        limit: int = 10,
        offset: int = 0,
        inc: bool = False,
        ignore_case: bool = False,
    ) -> list[any]:
        res = self.db.get_msg_by_chat_id_and_keyword(
            chat_ids,
            search_word,
            limit=limit,
            offset=offset,
            inc=inc,
            ignore_case=ignore_case,
        )
        res = [self.db.get_column_msg_js(v) for v in res]
        return res

    async def _download_media_chunk(self, msg: types.Message, media_holder: MediaChunkHolder) -> None:
        logger.info(f"start downloading new chunk:{media_holder=}")
        try:
            offset = media_holder.start + media_holder.length
            target_size = media_holder.target_len - media_holder.length
            remain_size = target_size
            async for chunk in self.client.iter_download(msg, offset=offset, chunk_size=self.SINGLE_NET_CHUNK_SIZE):
                if not isinstance(chunk, bytes):
                    chunk = chunk.tobytes()
                remain_size -= len(chunk)
                if remain_size <= 0:
                    media_holder.append_chunk_mem(chunk[: len(chunk) + remain_size])
                else:
                    media_holder.append_chunk_mem(chunk)
                if media_holder.is_completed():
                    break
                if await media_holder.is_disconneted():
                    raise asyncio.CancelledError("all requester canceled.")
        except asyncio.CancelledError as err:
            logger.info(f"cancel holder:{media_holder}")
            self.media_chunk_manager.cancel_media_chunk(media_holder)
        except Exception as err:
            logger.error(
                f"_download_media_chunk err:{err=},{offset=},{target_size=},{media_holder},\r\n{err=}\r\n{traceback.format_exc()}"
            )
        else:
            if not self.media_chunk_manager.move_media_chunk_to_disk(media_holder):
                logger.warning(f"move to disk failed, {media_holder=}")
            logger.debug(f"downloaded chunk:{offset=},{target_size=},{media_holder}")
        finally:
            pass

    async def streaming_get_iter(self, msg: types.Message, start: int, end: int, req: Request):
        try:
            logger.debug(f"new steaming request:{msg.chat_id=},{msg.id=},[{start}:{end}]")
            cur_task_id = self._get_unique_task_id()
            pos = start
            while not await req.is_disconnected() and pos <= end:
                cache_chunk = self.media_chunk_manager.get_media_chunk(msg, pos)
                if cache_chunk is None:
                    # post download task
                    # align pos download task
                    file_size = msg.media.document.size
                    # align_pos = pos // self.SINGLE_MEDIA_SIZE * self.SINGLE_MEDIA_SIZE
                    align_pos = pos
                    align_size = min(self.SINGLE_MEDIA_SIZE, file_size - align_pos)
                    holder = self.media_chunk_manager.create_media_chunk_holder(msg.chat_id, msg.id, align_pos, align_size)
                    logger.info(f"new holder create:{holder}")
                    holder.add_chunk_requester(req)
                    self.media_chunk_manager.set_media_chunk(holder)
                    self.task_queue.put_nowait((cur_task_id, self._download_media_chunk(msg, holder)))
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
                        need_len = min(cache_chunk.length - offset, end - pos + 1)
                        pos = pos + need_len
                        yield cache_chunk.mem[offset : offset + need_len]
                else:
                    offset = pos - cache_chunk.start
                    if offset >= cache_chunk.length:
                        raise RuntimeError(f"lru cache missed!{pos=},{cache_chunk=}")
                    need_len = min(cache_chunk.length - offset, end - pos + 1)
                    pos = pos + need_len
                    yield cache_chunk.mem[offset : offset + need_len]
        except Exception as err:
            logger.error(f"stream iter:{err=}")
            logger.error(traceback.format_exc())
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
