import traceback
import json
import logging
from urllib.parse import quote

from telethon import types, hints, utils
import fastapi
from fastapi import Request
from fastapi.responses import StreamingResponse, Response

import configParse
from backend import apiutils
from backend.TgFileSystemClientManager import TgFileSystemClientManager, EnumSignLevel


logger = logging.getLogger(__file__.split("/")[-1])


async def link_convert(link: str) -> str:
    clients_mgr = TgFileSystemClientManager.get_instance()
    link_slice = link.split("/")
    if len(link_slice) < 5:
        raise RuntimeError("link format invalid")
    chat_id_or_name, msg_id = link_slice[-2:]
    is_msg_id = msg_id.isascii() and msg_id.isdecimal()
    if not is_msg_id:
        raise RuntimeError("message id invalid")
    msg_id = int(msg_id)
    is_chat_name = chat_id_or_name.isascii() and not chat_id_or_name.isdecimal()
    is_chat_id = chat_id_or_name.isascii() and chat_id_or_name.isdecimal()
    if not is_chat_name and not is_chat_id:
        raise RuntimeError("chat id invalid")
    client = clients_mgr.get_first_client()
    if client is None:
        raise RuntimeError("client not ready, login first pls.")
    if is_chat_id:
        chat_id_or_name = int(chat_id_or_name)
    msg = await client.get_message(chat_id_or_name, msg_id)
    file_name = apiutils.get_message_media_name(msg)
    param = configParse.get_TgToFileSystemParameter()
    sign = clients_mgr.generate_sign(client.session_name, EnumSignLevel.VIST)
    url = (
        f"{param.base.exposed_url}/tg/api/v1/file/get/{utils.get_peer_id(msg.peer_id)}/{msg.id}/{quote(file_name)}?sign={sign}"
    )
    return url


async def get_chat_details(mgr: TgFileSystemClientManager) -> dict[int, any]:
    chat_details = {}
    for _, client in mgr.clients.items():
        chat_list = client.client_param.whitelist_chat
        for chat_id in chat_list:
            chat_entity = await client.get_entity(chat_id)
            chat_details[chat_id] = json.loads(chat_entity.to_json())
    return chat_details


async def get_clients_manager_status(detail: bool) -> dict[str, any]:
    clients_mgr = TgFileSystemClientManager.get_instance()
    ret = await clients_mgr.get_status()
    if not detail:
        return ret
    ret["clist"] = await get_chat_details(clients_mgr)
    return ret


async def get_media_file_stream(sign: str, cid: int, mid: int, request: Request) -> StreamingResponse:
    msg_id = mid
    chat_id = cid
    headers = {
        # "content-type": "video/mp4",
        "accept-ranges": "bytes",
        "content-encoding": "identity",
        # "content-length": stream_file_size,
        "access-control-expose-headers": ("content-type, accept-ranges, content-length, " "content-range, content-encoding"),
    }
    range_header = request.headers.get("range")

    clients_mgr = TgFileSystemClientManager.get_instance()
    sign_info = clients_mgr.parse_sign(sign)
    client_id = TgFileSystemClientManager.get_sign_client_id(sign_info)
    client = await clients_mgr.get_client_force(client_id)
    msg = await client.get_message(chat_id, msg_id)
    if not isinstance(msg.media, types.MessageMediaDocument) and not isinstance(msg.media, types.MessageMediaPhoto):
        raise RuntimeError(f"request don't support: {msg.media=}")
    file_size = msg.media.document.size
    start = 0
    end = file_size - 1
    status_code = fastapi.status.HTTP_200_OK
    mime_type = msg.media.document.mime_type
    headers["content-type"] = mime_type
    # headers["content-length"] = str(file_size)
    file_name = apiutils.get_message_media_name(msg)
    if file_name == "":
        maybe_file_type = mime_type.split("/")[-1]
        file_name = f"{chat_id}.{msg_id}.{maybe_file_type}"
    headers["Content-Disposition"] = f"inline; filename*=utf-8'{quote(file_name)}'"

    if range_header is not None:
        start, end = apiutils.get_range_header(range_header, file_size)
        size = end - start + 1
        # headers["content-length"] = str(size)
        headers["content-range"] = f"bytes {start}-{end}/{file_size}"
        status_code = fastapi.status.HTTP_206_PARTIAL_CONTENT
    else:
        headers["content-length"] = str(file_size)
        headers["content-range"] = f"bytes 0-{file_size-1}/{file_size}"
    return StreamingResponse(
        client.streaming_get_iter(msg, start, end, request),
        headers=headers,
        media_type=mime_type,
        status_code=status_code,
    )
