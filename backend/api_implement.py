import traceback
import json
import logging

from telethon import types, hints, utils

import configParse
from backend import apiutils
from backend.TgFileSystemClientManager import TgFileSystemClientManager


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
    url = (
        f"{param.base.exposed_url}/tg/api/v1/file/get/{utils.get_peer_id(msg.peer_id)}/{msg.id}/{file_name}?sign={client.sign}"
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